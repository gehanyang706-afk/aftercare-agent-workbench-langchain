"""C2 authenticated callbacks and support commands, backed by real PostgreSQL."""

import hashlib
import hmac
import json
from contextlib import ExitStack
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import func, select
from test_fulfillment_api import PASSWORD, error
from test_outbox_api import TOKEN, enqueue, shipping_state
from test_outbox_api import no_warehouse_http as no_warehouse_http

from aftercare.api import warehouse as warehouse_api
from aftercare.api.app import create_app
from aftercare.config import Settings
from aftercare.domain.warehouse import PROVIDER_ID, WarehouseEvent
from aftercare.repositories.models import (
    InboxConflict,
    ReconciliationTask,
    RecoveryCommand,
    WarehouseInbox,
)
from aftercare.services.business import columns
from aftercare.services.outbox import OutboxService
from aftercare.services.recovery import RecoveryService
from aftercare.services.staff_auth import StaffAuthService

pytestmark = pytest.mark.integration

SECRET = "C2-test-callback-secret-1234567890123456"
NOW = 1_800_000_000
CALLBACK_PATH = "/api/integrations/warehouse/events"


@pytest.fixture
def api(service, no_warehouse_http, monkeypatch):
    settings = Settings(
        _env_file=None,
        app_env="test",
        demo_login_enabled=True,
        warehouse_delivery_enabled=True,
        warehouse_simulation_enabled=False,
        warehouse_token=TOKEN,
        warehouse_callbacks_enabled=True,
        warehouse_callback_secret=SECRET,
        database_url=service.sessions.kw["bind"].url.render_as_string(hide_password=False),
    )
    app = create_app(settings)
    app.state.business = service
    auth = StaffAuthService(service.sessions)
    accounts = {
        role: auth.create_account(f"b2-{role}", PASSWORD, role) for role in ("support", "warehouse")
    }
    # Only the callback's wall-clock lookup is frozen; account/session clocks stay real.
    monkeypatch.setattr(warehouse_api, "time", SimpleNamespace(time=lambda: NOW))
    with ExitStack() as stack:
        yield SimpleNamespace(
            app=app,
            settings=settings,
            auth=auth,
            accounts=accounts,
            http_calls=no_warehouse_http,
            client=lambda: stack.enter_context(TestClient(app)),
        )


def event_for(request, *, event_id="event-1", sequence=1, status="ACCEPTED", **changes):
    values = {
        "event_id": event_id,
        "effect_key": request["effect_key"],
        "application_id": request["application_id"],
        "request_hash": request["request_hash"],
        "external_shipment_id": "C2-HTTP-EXTERNAL-001",
        "sequence": sequence,
        "status": status,
    }
    values.update(changes)
    return WarehouseEvent(**values).model_dump()


def unknown_event(**changes):
    return event_for(
        {
            "effect_key": "exchange:not-yet-known",
            "application_id": "not-yet-known",
            "request_hash": "0" * 64,
        },
        **changes,
    )


def raw_body(event):
    return json.dumps(event, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()


def signed_headers(raw, *, timestamp=NOW, secret=SECRET):
    stamp = str(timestamp)
    digest = hmac.new(secret.encode(), stamp.encode() + b"." + raw, hashlib.sha256).hexdigest()
    return {
        "Content-Type": "application/json",
        "X-Warehouse-Source": PROVIDER_ID,
        "X-Warehouse-Timestamp": stamp,
        "X-Warehouse-Signature": "v1=" + digest,
    }


def callback(client, event=None, *, raw=None, timestamp=NOW, headers=None, secret=SECRET):
    raw = raw_body(event) if raw is None else raw
    return client.post(
        CALLBACK_PATH,
        content=raw,
        headers={**signed_headers(raw, timestamp=timestamp, secret=secret), **(headers or {})},
    )


def recovery_state(service, application_id):
    state = shipping_state(service, application_id)
    with service.sessions() as session:
        for name, model in (
            ("inbox", WarehouseInbox),
            ("conflicts", InboxConflict),
            ("reconciliation", ReconciliationTask),
            ("recovery_commands", RecoveryCommand),
        ):
            state[name] = [
                columns(row) for row in session.scalars(select(model).order_by(model.id))
            ]
    return state


def counts(service):
    with service.sessions() as session:
        return {
            model.__tablename__: session.scalar(select(func.count()).select_from(model))
            for model in (WarehouseInbox, InboxConflict, RecoveryCommand, ReconciliationTask)
        }


def blocked_request(api, service, *, failed=False):
    application_id, original, owner, support, warehouse, _receipt = enqueue(api, service)
    outbox = OutboxService(service.sessions)
    claim = outbox.claim("c2-http-test-worker")
    assert claim is not None
    outbox.begin_delivery(claim)
    outbox.finish(
        claim,
        "FAILED" if failed else "UNKNOWN",
        error_code="WAREHOUSE_PERMANENT_REJECTION" if failed else "WAREHOUSE_TRANSPORT_UNKNOWN",
    )
    state = shipping_state(service, application_id)
    assert state["outboxes"][0]["status"] == ("DEAD" if failed else "BLOCKED")
    return (
        application_id,
        original,
        owner,
        support,
        warehouse,
        state["requests"][0],
        state["outboxes"][0],
    )


def management_post(
    client, path, generation, *, key="c2-manual", reason="Investigate delivery", **extra
):
    return client.post(
        path,
        json={"expected_generation": generation, "reason": reason, **extra},
        headers={"Idempotency-Key": key},
    )


def test_signed_callbacks_apply_success_once_without_human_login_or_network_io(api, service):
    application_id, original, owner, _support, _warehouse, request, _outbox = blocked_request(
        api, service
    )
    provider = api.client()
    accepted = event_for(request)
    response = callback(provider, accepted)
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "PROCESSED"
    before_success = shipping_state(service, application_id)
    assert before_success["application"]["version"] == 5
    assert before_success["reservation"]["status"] == "RESERVED"
    succeeded = event_for(request, event_id="event-2", sequence=2, status="SUCCEEDED")
    response = callback(provider, succeeded)
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "PROCESSED"
    completed = recovery_state(service, application_id)
    assert (completed["application"]["status"], completed["application"]["version"]) == (
        "REPLACEMENT_SHIPPED",
        6,
    )
    assert completed["reservation"]["status"] == "CONSUMED"
    assert (completed["inventory"]["available"], completed["inventory"]["reserved"]) == (0, 0)
    assert completed["operation"]["receipt"] == original
    repeated = callback(provider, succeeded)
    assert repeated.status_code == 200, repeated.text
    assert recovery_state(service, application_id) == completed
    detail = owner.get(f"/api/applications/{application_id}").json()
    assert detail["receipt"] == original
    assert detail["history"][-1]["actor_source"] == "trusted_adapter"
    assert api.http_calls == []


def test_out_of_order_success_is_durable_pending_until_the_sequence_gap_is_filled(api, service):
    application_id, _original, _owner, _support, _warehouse, request, _outbox = blocked_request(
        api, service
    )
    provider = api.client()
    response = callback(
        provider, event_for(request, event_id="sequence-2", sequence=2, status="SUCCEEDED")
    )
    assert response.status_code == 202, response.text
    assert response.json()["status"] == "PENDING"
    pending = recovery_state(service, application_id)
    assert len(pending["inbox"]) == 1
    assert pending["application"]["version"] == 5
    assert pending["reservation"]["status"] == "RESERVED"
    response = callback(provider, event_for(request, event_id="sequence-1"))
    assert response.status_code == 200, response.text
    final = recovery_state(service, application_id)
    assert final["application"]["version"] == 6
    assert final["reservation"]["status"] == "CONSUMED"
    assert len(final["inbox"]) == 2


def test_stale_accepted_event_cannot_regress_a_successful_delivery(api, service):
    application_id, _original, _owner, _support, _warehouse, request, _outbox = blocked_request(
        api, service
    )
    provider = api.client()
    assert callback(provider, event_for(request)).status_code == 200
    assert (
        callback(
            provider, event_for(request, event_id="succeeded", sequence=2, status="SUCCEEDED")
        ).status_code
        == 200
    )
    before = shipping_state(service, application_id)
    response = callback(provider, event_for(request, event_id="late-accepted", sequence=1))
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "STALE"
    assert shipping_state(service, application_id) == before


def test_unknown_business_key_is_only_persistently_received_not_falsely_completed(api, service):
    response = callback(api.client(), unknown_event())
    assert response.status_code == 202, response.text
    assert response.json()["status"] == "PENDING"
    assert counts(service)["warehouse_inbox"] == 1
    with service.sessions() as session:
        row = session.scalar(select(WarehouseInbox))
        assert row.status == "PENDING" and row.effect_key == "exchange:not-yet-known"


def test_same_event_id_with_other_signed_parameters_is_a_durable_conflict(api, service):
    application_id, _original, _owner, _support, _warehouse, request, _outbox = blocked_request(
        api, service
    )
    provider = api.client()
    event = event_for(request)
    assert callback(provider, event).status_code == 200
    changed = {**event, "external_shipment_id": "CONFLICTING-EXTERNAL-ID"}
    response = callback(provider, changed)
    assert response.status_code == 409, response.text
    assert response.json()["status"] == "CONFLICT"
    with service.sessions() as session:
        inbox = session.scalar(select(WarehouseInbox))
        assert inbox.payload == event
        conflict = session.scalar(select(InboxConflict))
        assert conflict is not None and conflict.payload == changed
        assert conflict.original_hash != conflict.new_hash
    assert counts(service)["warehouse_inbox"] == counts(service)["inbox_conflicts"] == 1
    again = callback(provider, changed)
    assert again.status_code == 409
    assert counts(service)["inbox_conflicts"] == 1
    state = shipping_state(service, application_id)
    assert state["application"]["version"] == 5
    assert state["reservation"]["status"] == "RESERVED"


@pytest.mark.parametrize(
    "headers",
    [
        {"X-Warehouse-Source": "untrusted-provider"},
        {"X-Warehouse-Source": ""},
        {"X-Warehouse-Timestamp": ""},
        {"X-Warehouse-Timestamp": "not-a-time"},
        {"X-Warehouse-Signature": ""},
        {"X-Warehouse-Signature": "v1=" + "0" * 64},
    ],
)
def test_invalid_callback_auth_never_writes_even_with_a_valid_staff_session(api, service, headers):
    application_id, _original, _owner, support, _warehouse, request, _outbox = blocked_request(
        api, service
    )
    before = recovery_state(service, application_id)
    error(
        callback(support, event_for(request), headers=headers),
        401,
        "WAREHOUSE_CALLBACK_AUTH_FAILED",
    )
    assert recovery_state(service, application_id) == before


@pytest.mark.parametrize("offset", [-301, 301])
def test_correct_hmac_outside_the_replay_window_is_rejected_before_persistence(
    api, service, offset
):
    before = counts(service)
    error(
        callback(api.client(), unknown_event(), timestamp=NOW + offset),
        401,
        "WAREHOUSE_CALLBACK_AUTH_FAILED",
    )
    assert counts(service) == before


@pytest.mark.parametrize("offset", [-300, 300])
def test_replay_window_boundary_is_accepted_with_the_fixed_http_clock(api, offset):
    response = callback(api.client(), unknown_event(), timestamp=NOW + offset)
    assert response.status_code == 202, response.text


def test_expired_duplicate_or_changed_raw_bytes_cannot_use_an_old_valid_signature(api, service):
    provider = api.client()
    event = unknown_event()
    assert callback(provider, event).status_code == 202
    before = counts(service)
    error(callback(provider, event, timestamp=NOW - 301), 401, "WAREHOUSE_CALLBACK_AUTH_FAILED")
    raw = raw_body(event)
    error(
        provider.post(CALLBACK_PATH, content=raw + b" ", headers=signed_headers(raw)),
        401,
        "WAREHOUSE_CALLBACK_AUTH_FAILED",
    )
    error(callback(provider, event, secret=TOKEN), 401, "WAREHOUSE_CALLBACK_AUTH_FAILED")
    assert counts(service) == before


def test_disabled_and_oversized_callbacks_do_not_create_inbox_records(api, service):
    client = api.client()
    event = unknown_event()
    raw = raw_body(event)
    oversized = raw + b" " * (16_385 - len(raw))
    error(callback(client, raw=oversized), 413, "CALLBACK_BODY_TOO_LARGE")
    assert counts(service)["warehouse_inbox"] == 0
    boundary = raw + b" " * (16_384 - len(raw))
    assert callback(client, raw=boundary).status_code == 202
    before = counts(service)
    api.settings.warehouse_callbacks_enabled = False
    error(callback(client, event), 404)
    assert counts(service) == before


@pytest.mark.parametrize(
    "changes",
    [
        {"sequence": 0},
        {"sequence": True},
        {"sequence": "1"},
        {"status": "FAILED"},
        {"actor": {"source": "trusted_adapter"}},
    ],
)
def test_valid_signature_does_not_bypass_strict_event_schema(api, service, changes):
    before = counts(service)
    event = {**unknown_event(), **changes}
    error(callback(api.client(), event), 422, "INVALID_INPUT")
    assert counts(service) == before


@pytest.mark.parametrize("role", ["customer", "warehouse"])
@pytest.mark.parametrize("action", ["reconcile", "redrive"])
def test_only_real_support_sessions_may_issue_recovery_commands(api, service, role, action):
    application_id, _original, owner, _support, warehouse, request, outbox = blocked_request(
        api, service
    )
    client = owner if role == "customer" else warehouse
    path = (
        f"/api/fulfillment-requests/{request['id']}/reconcile"
        if action == "reconcile"
        else f"/api/outbox/{outbox['id']}/redrive"
    )
    before = recovery_state(service, application_id)
    error(management_post(client, path, outbox["generation"]), 403)
    assert recovery_state(service, application_id) == before


def test_reconcile_only_schedules_a_query_with_replay_cas_and_authenticated_audit(api, service):
    application_id, _original, _owner, support, _warehouse, request, outbox = blocked_request(
        api, service
    )
    path = f"/api/fulfillment-requests/{request['id']}/reconcile"
    response = management_post(support, path, outbox["generation"])
    assert response.status_code == 202, response.text
    receipt = response.json()
    assert receipt["status"] == "PENDING" and receipt["task_id"]
    state = recovery_state(service, application_id)
    assert state["application"]["version"] == 5
    assert state["reservation"]["status"] == "RESERVED"
    assert len(state["recovery_commands"]) == 1
    audit = state["recovery_commands"][0]
    assert (audit["actor_id"], audit["actor_role"], audit["actor_source"]) == (
        api.accounts["support"]["id"],
        "support",
        "server_session",
    )
    assert management_post(support, path, outbox["generation"]).json() == receipt
    error(
        management_post(support, path, outbox["generation"], key="stale-generation"),
        409,
        "GENERATION_CONFLICT",
    )
    error(
        management_post(support, path, outbox["generation"], reason="Changed reason"),
        409,
        "IDEMPOTENCY_CONFLICT",
    )
    assert recovery_state(service, application_id) == state
    assert api.http_calls == []


def test_redrive_requires_definite_nonacceptance_and_only_queues_an_existing_business_request(
    api, service
):
    application_id, _original, _owner, support, _warehouse, request, outbox = blocked_request(
        api, service, failed=True
    )
    path = f"/api/outbox/{outbox['id']}/redrive"
    response = management_post(support, path, outbox["generation"], key="retry-definite-rejection")
    assert response.status_code == 202, response.text
    receipt = response.json()
    state = recovery_state(service, application_id)
    assert state["application"]["version"] == 5
    assert state["reservation"]["status"] == "RESERVED"
    assert len(state["requests"]) == len(state["outboxes"]) == 1
    assert state["requests"][0]["id"] == request["id"]
    assert state["outboxes"][0]["status"] == "PENDING"
    assert state["outboxes"][0]["generation"] > outbox["generation"]
    replay = management_post(support, path, outbox["generation"], key="retry-definite-rejection")
    assert replay.status_code == 202 and replay.json() == receipt
    assert recovery_state(service, application_id) == state
    assert api.http_calls == []


def test_unknown_delivery_cannot_be_redriven_as_a_new_post(api, service):
    application_id, _original, _owner, support, _warehouse, _request, outbox = blocked_request(
        api, service
    )
    before = recovery_state(service, application_id)
    error(
        management_post(support, f"/api/outbox/{outbox['id']}/redrive", outbox["generation"]),
        409,
        "REDRIVE_NOT_ALLOWED",
    )
    assert recovery_state(service, application_id) == before
    assert api.http_calls == []


@pytest.mark.parametrize(
    "extra",
    [
        {"status": "SUCCEEDED"},
        {"external_shipment_id": "FORGED"},
        {"source": "trusted_adapter"},
        {"actor": {"role": "support"}},
        {"fault": "before_commit"},
    ],
)
def test_recovery_dto_does_not_allow_humans_to_supply_a_success_result(api, service, extra):
    application_id, _original, _owner, support, _warehouse, request, outbox = blocked_request(
        api, service
    )
    before = recovery_state(service, application_id)
    error(
        management_post(
            support,
            f"/api/fulfillment-requests/{request['id']}/reconcile",
            outbox["generation"],
            **extra,
        ),
        422,
    )
    assert recovery_state(service, application_id) == before


def test_recovery_support_session_is_rechecked_before_the_command_transaction(
    api, service, monkeypatch
):
    application_id, _original, _owner, support, _warehouse, request, outbox = blocked_request(
        api, service
    )
    before = recovery_state(service, application_id)
    original_guard = warehouse_api._guard_identity
    calls = []

    def revoke_before_guard(session, request_http, who):
        calls.append(who.actor)
        api.auth.revoke_all(api.accounts["support"]["id"])
        return original_guard(session, request_http, who)

    monkeypatch.setattr(warehouse_api, "_guard_identity", revoke_before_guard)
    error(
        management_post(
            support, f"/api/fulfillment-requests/{request['id']}/reconcile", outbox["generation"]
        ),
        401,
        "SESSION_EXPIRED",
    )
    assert len(calls) == 1 and calls[0].source == "server_session"
    assert recovery_state(service, application_id) == before


def test_failure_after_recovery_savepoint_rolls_back_manual_command_and_generation(
    api, service, monkeypatch
):
    application_id, _original, _owner, support, _warehouse, request, outbox = blocked_request(
        api, service
    )
    before = recovery_state(service, application_id)
    original = RecoveryService.manual_reconcile

    def fail_after_savepoint(self, *args, **kwargs):
        original(self, *args, **kwargs)
        raise RuntimeError("INJECTED_RECOVERY_HTTP_ROLLBACK")

    monkeypatch.setattr(RecoveryService, "manual_reconcile", fail_after_savepoint)
    with pytest.raises(RuntimeError, match="INJECTED_RECOVERY_HTTP_ROLLBACK"):
        management_post(
            support, f"/api/fulfillment-requests/{request['id']}/reconcile", outbox["generation"]
        )
    assert recovery_state(service, application_id) == before


def test_management_generations_and_internal_delivery_secrets_are_not_exposed_to_customers(
    api, service
):
    _application_id, _original, owner, support, warehouse, request, outbox = blocked_request(
        api, service
    )
    path = f"/api/fulfillment-requests/{request['id']}"
    staff_view = support.get(path)
    assert staff_view.status_code == 200, staff_view.text
    assert staff_view.json()["management"]["outbox_generation"] == outbox["generation"]
    assert "redrive_count" in staff_view.json()["management"]
    forbidden = {
        "owner",
        "lease_owner",
        "lease_expires_at",
        "payload",
        "signature",
        "warehouse_token",
        "warehouse_callback_secret",
    }

    def check(value):
        if isinstance(value, dict):
            assert not forbidden.intersection(value)
            for nested in value.values():
                check(nested)
        elif isinstance(value, list):
            for nested in value:
                check(nested)

    for client in (owner, warehouse):
        response = client.get(path)
        assert response.status_code == 200, response.text
        assert "management" not in response.json()
        check(response.json())
        assert SECRET not in response.text and TOKEN not in response.text


def test_support_recovery_commands_keep_csrf_idempotency_and_strict_input_protection(api, service):
    application_id, _original, _owner, support, _warehouse, request, outbox = blocked_request(
        api, service
    )
    path = f"/api/fulfillment-requests/{request['id']}/reconcile"
    data = {"expected_generation": outbox["generation"], "reason": "Investigate delivery"}
    before = recovery_state(service, application_id)
    for headers in (
        {"Idempotency-Key": "csrf-probe", "X-CSRF-Token": ""},
        {"Idempotency-Key": "origin-probe", "Origin": "https://evil.example"},
    ):
        error(support.post(path, json=data, headers=headers), 403)
    error(support.post(path, json=data), 422)
    for generation in (-1, "1", True):
        error(management_post(support, path, generation), 422)
    error(management_post(support, path, outbox["generation"], reason="   "), 422)
    assert recovery_state(service, application_id) == before
