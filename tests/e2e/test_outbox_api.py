"""C1's authenticated enqueue/read boundary; no warehouse HTTP runs inside requests."""

import http.client
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from threading import Barrier
from types import SimpleNamespace

import httpx
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select
from test_fulfillment_api import (
    PASSWORD,
    customer,
    error,
    persisted,
    post,
    prepared,
    staff,
    success,
)

from aftercare.api.app import create_app
from aftercare.config import Settings
from aftercare.repositories.models import OutboxAttempt, ShipmentRequest, WarehouseOutbox
from aftercare.services.business import columns
from aftercare.services.fulfillment import FulfillmentService
from aftercare.services.outbox import OutboxService
from aftercare.services.staff_auth import StaffAuthService

pytestmark = pytest.mark.integration

TOKEN = "C1-local-warehouse-test-token-123456789"


@pytest.fixture
def no_warehouse_http(monkeypatch):
    calls = []

    def unexpected_http(*args, **kwargs):
        calls.append(True)
        raise AssertionError("WAREHOUSE_HTTP_MUST_NOT_RUN_INSIDE_API_REQUEST")

    # TestClient uses its ASGI transport and does not touch either network transport.
    monkeypatch.setattr(http.client.HTTPConnection, "request", unexpected_http)
    monkeypatch.setattr(httpx.HTTPTransport, "handle_request", unexpected_http)
    return calls


@pytest.fixture
def api(service, no_warehouse_http):
    settings = Settings(
        _env_file=None,
        app_env="test",
        demo_login_enabled=True,
        warehouse_delivery_enabled=True,
        warehouse_simulation_enabled=False,
        warehouse_token=TOKEN,
        database_url=service.sessions.kw["bind"].url.render_as_string(hide_password=False),
    )
    app = create_app(settings)
    app.state.business = service
    auth = StaffAuthService(service.sessions)
    accounts = {
        role: auth.create_account(f"b2-{role}", PASSWORD, role) for role in ("support", "warehouse")
    }
    with ExitStack() as stack:
        yield SimpleNamespace(
            app=app,
            settings=settings,
            auth=auth,
            accounts=accounts,
            http_calls=no_warehouse_http,
            client=lambda: stack.enter_context(TestClient(app)),
        )


def to_received(api, application_id):
    owner, support, warehouse = customer(api), staff(api), staff(api, "warehouse")
    success(post(support, application_id, "review", 1, decision="approve"), "AWAITING_RETURN")
    success(
        post(
            owner,
            application_id,
            "return-shipment",
            2,
            carrier="C1-local-simulator",
            tracking_number="C1-RETURN-001",
        ),
        "RETURN_IN_TRANSIT",
    )
    success(post(warehouse, application_id, "receive", 3), "RETURN_RECEIVED")
    return owner, support, warehouse


def shipping_state(service, application_id):
    state = persisted(service, application_id)
    with service.sessions() as session:
        requests = list(
            session.scalars(
                select(ShipmentRequest)
                .where(ShipmentRequest.application_id == application_id)
                .order_by(ShipmentRequest.id)
            )
        )
        outboxes = list(
            session.scalars(
                select(WarehouseOutbox)
                .where(WarehouseOutbox.request_id.in_([row.id for row in requests]))
                .order_by(WarehouseOutbox.id)
            )
        )
        attempts = list(
            session.scalars(
                select(OutboxAttempt)
                .where(OutboxAttempt.outbox_id.in_([row.id for row in outboxes]))
                .order_by(OutboxAttempt.id)
            )
        )
        state.update(
            requests=[columns(row) for row in requests],
            outboxes=[columns(row) for row in outboxes],
            attempts=[columns(row) for row in attempts],
        )
    return state


def enqueue(api, service):
    original = prepared(service)
    application_id = original["application_id"]
    owner, support, warehouse = to_received(api, application_id)
    result = success(
        post(warehouse, application_id, "replacement-shipments", 4, key="c1-enqueue"),
        "REPLACEMENT_PENDING",
    )
    return application_id, original, owner, support, warehouse, result


def simulation_mode(api):
    values = api.settings.model_dump()
    values.update(warehouse_delivery_enabled=False, warehouse_simulation_enabled=True)
    api.settings = Settings(_env_file=None, **values)
    api.app.state.settings = api.settings


def test_request_only_commits_pending_outbox_with_original_receipt_and_real_staff_trigger(
    api, service
):
    application_id, original, owner, support, warehouse, result = enqueue(api, service)
    assert result["version"] == 5
    assert set(result) == {"command_id", "application_id", "command", "status", "version"}
    state = shipping_state(service, application_id)
    assert len(state["requests"]) == len(state["outboxes"]) == 1
    request, outbox = state["requests"][0], state["outboxes"][0]
    assert request["status"] == outbox["status"] == "PENDING"
    assert outbox["request_id"] == request["id"]
    assert outbox["attempts"] == outbox["generation"] == 0
    assert state["attempts"] == []
    assert request.get("external_shipment_id") is None
    assert request["triggered_by"] == {
        "actor_id": api.accounts["warehouse"]["id"],
        "role": "warehouse",
        "source": "server_session",
    }
    assert state["operation"]["receipt"] == original
    assert state["reservation"]["status"] == "RESERVED"
    assert (state["inventory"]["available"], state["inventory"]["reserved"]) == (0, 1)
    for client in (owner, support, warehouse):
        detail = client.get(f"/api/applications/{application_id}")
        assert detail.status_code == 200, detail.text
        assert detail.json()["status"] == "REPLACEMENT_PENDING"
        assert detail.json()["shipment_request"]["id"] == request["id"]
        status = client.get(f"/api/fulfillment-requests/{request['id']}")
        assert status.status_code == 200, status.text
        assert status.json()["request"]["status"] == "PENDING"
        assert TOKEN not in status.text
    assert api.http_calls == []


def test_exact_request_replay_never_duplicates_outbox_or_changes_the_first_trigger(api, service):
    application_id, _original, _owner, _support, warehouse, first = enqueue(api, service)
    before = shipping_state(service, application_id)
    replay = post(warehouse, application_id, "replacement-shipments", 4, key="c1-enqueue")
    assert replay.status_code == 200, replay.text
    assert replay.json() == first
    assert shipping_state(service, application_id) == before
    assert api.http_calls == []


def test_parallel_http_request_replay_creates_one_outbox_pair(api, service):
    application_id = prepared(service)["application_id"]
    _owner, _support, first = to_received(api, application_id)
    second = staff(api, "warehouse")
    barrier = Barrier(2)

    def call(client):
        barrier.wait(timeout=5)
        return post(client, application_id, "replacement-shipments", 4, key="c1-parallel")

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(call, first), pool.submit(call, second)]
        responses = [future.result(timeout=15) for future in futures]
    assert all(response.status_code == 200 for response in responses)
    assert responses[0].json() == responses[1].json()
    state = shipping_state(service, application_id)
    assert len(state["requests"]) == len(state["outboxes"]) == 1
    assert state["application"]["version"] == 5
    assert len(state["commands"]) == 4 and len(state["audits"]) == 5
    assert api.http_calls == []


def test_failure_after_domain_savepoint_rolls_back_request_outbox_command_and_audit(
    api, service, monkeypatch
):
    application_id = prepared(service)["application_id"]
    _owner, _support, warehouse = to_received(api, application_id)
    before = shipping_state(service, application_id)
    original_execute = FulfillmentService.execute

    def fail_after_nested_commit(self, actor, app_id, command, **kwargs):
        receipt = original_execute(self, actor, app_id, command, **kwargs)
        if app_id == application_id and command.command == "request_shipment":
            assert receipt["status"] == "REPLACEMENT_PENDING"
            raise RuntimeError("INJECTED_C1_AFTER_SAVEPOINT")
        return receipt

    monkeypatch.setattr(FulfillmentService, "execute", fail_after_nested_commit)
    with pytest.raises(RuntimeError, match="INJECTED_C1_AFTER_SAVEPOINT"):
        post(warehouse, application_id, "replacement-shipments", 4, key="c1-rollback")
    assert shipping_state(service, application_id) == before
    monkeypatch.setattr(FulfillmentService, "execute", original_execute)
    success(
        post(warehouse, application_id, "replacement-shipments", 4, key="c1-rollback"),
        "REPLACEMENT_PENDING",
    )
    state = shipping_state(service, application_id)
    assert len(state["requests"]) == len(state["outboxes"]) == 1
    assert api.http_calls == []


def test_request_read_requires_identity_and_customer_ownership(api, service):
    application_id, _original, owner, _support, _warehouse, _result = enqueue(api, service)
    request_id = shipping_state(service, application_id)["requests"][0]["id"]
    error(api.client().get(f"/api/fulfillment-requests/{request_id}"), 401)
    error(
        customer(api, "bob").get(
            f"/api/fulfillment-requests/{request_id}", headers={"X-Customer-Id": "alice"}
        ),
        404,
        "NOT_FOUND",
    )
    error(owner.get("/api/fulfillment-requests/missing-request"), 404, "NOT_FOUND")


def test_request_projection_does_not_expose_lease_owner_or_transport_credentials(api, service):
    application_id, _original, owner, _support, _warehouse, _result = enqueue(api, service)
    request_id = shipping_state(service, application_id)["requests"][0]["id"]
    private_owner = "c1-private-worker-owner"
    assert OutboxService(service.sessions).claim(private_owner) is not None
    response = owner.get(f"/api/fulfillment-requests/{request_id}")
    assert response.status_code == 200, response.text
    body = response.json()
    assert set(body) == {"request", "outbox", "attempts", "reconciliation", "inbox", "conflicts"}
    assert set(body["request"]) <= {
        "id",
        "application_id",
        "effect_key",
        "request_hash",
        "status",
        "external_shipment_id",
        "triggered_by",
        "created_at",
        "updated_at",
    }
    assert set(body["outbox"]) <= {
        "id",
        "status",
        "attempts",
        "next_attempt_at",
        "last_error_code",
        "created_at",
        "updated_at",
    }
    assert len(body["attempts"]) == 1
    assert set(body["attempts"][0]) <= {
        "generation",
        "outcome",
        "error_code",
        "started_at",
        "finished_at",
    }
    assert private_owner not in response.text and TOKEN not in response.text
    assert api.http_calls == []


@pytest.mark.parametrize("role", ["customer", "support"])
def test_self_declared_warehouse_role_cannot_enqueue_delivery(api, service, role):
    application_id = prepared(service)["application_id"]
    owner, support, _warehouse = to_received(api, application_id)
    client = owner if role == "customer" else support
    before = shipping_state(service, application_id)
    error(
        post(
            client,
            application_id,
            "replacement-shipments",
            4,
            headers={"X-Role": "warehouse", "X-Actor-Id": api.accounts["warehouse"]["id"]},
        ),
        403,
    )
    assert shipping_state(service, application_id) == before


@pytest.mark.parametrize(
    "field,value",
    [
        ("role", "warehouse"),
        ("actor", {"role": "warehouse", "source": "trusted_adapter"}),
        ("external_shipment_id", "FORGED-SHIPMENT"),
        ("fault", "before_commit"),
    ],
)
def test_enqueue_dto_cannot_supply_identity_results_or_fault_controls(api, service, field, value):
    application_id = prepared(service)["application_id"]
    _owner, _support, warehouse = to_received(api, application_id)
    before = shipping_state(service, application_id)
    error(post(warehouse, application_id, "replacement-shipments", 4, **{field: value}), 422)
    assert shipping_state(service, application_id) == before


def test_existing_http_request_cannot_be_shipped_via_simulation_after_configuration_switch(
    api, service
):
    application_id, _original, _owner, _support, warehouse, _result = enqueue(api, service)
    before = shipping_state(service, application_id)
    simulation_mode(api)
    error(post(warehouse, application_id, "simulate-shipment", 5), 409, "ASYNC_DELIVERY_REQUIRED")
    assert shipping_state(service, application_id) == before
    assert api.http_calls == []


def test_disabled_http_delivery_preserves_the_existing_b2_simulation_path(api, service):
    simulation_mode(api)
    application_id = prepared(service)["application_id"]
    _owner, _support, warehouse = to_received(api, application_id)
    success(post(warehouse, application_id, "replacement-shipments", 4), "REPLACEMENT_PENDING")
    state = shipping_state(service, application_id)
    assert state["requests"] == state["outboxes"] == []
    success(post(warehouse, application_id, "simulate-shipment", 5), "REPLACEMENT_SHIPPED")
    assert shipping_state(service, application_id)["reservation"]["status"] == "CONSUMED"
    assert api.http_calls == []


@pytest.mark.parametrize("command", ["reconcile", "redrive"])
def test_warehouse_staff_cannot_use_support_recovery_commands(api, service, command):
    application_id, _original, _owner, _support, warehouse, _result = enqueue(api, service)
    before = shipping_state(service, application_id)
    request_id = before["requests"][0]["id"]
    outbox_id = before["outboxes"][0]["id"]
    path = (
        f"/api/fulfillment-requests/{request_id}/reconcile"
        if command == "reconcile"
        else f"/api/outbox/{outbox_id}/redrive"
    )
    error(
        warehouse.post(
            path,
            json={"expected_generation": before["outboxes"][0]["generation"], "reason": "Review"},
            headers={"Idempotency-Key": "c2-warehouse-forbidden"},
        ),
        403,
        "FORBIDDEN",
    )
    assert shipping_state(service, application_id) == before
