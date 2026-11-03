"""B2 HTTP/session contracts backed by PostgreSQL; no request may supply its Actor."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from datetime import timedelta
from threading import Barrier, Event
from time import monotonic, sleep
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select, text

from aftercare.agent.runtime import setup_checkpoints
from aftercare.api import fulfillment as fulfillment_api
from aftercare.api.app import create_app
from aftercare.config import Settings
from aftercare.domain.contracts import ActionInput, utc_now
from aftercare.repositories.models import (
    Application,
    DemoSession,
    ExchangeFulfillment,
    FulfillmentAudit,
    FulfillmentCommand,
    Inventory,
    Operation,
    Reservation,
    StaffAccount,
    StaffSession,
)
from aftercare.services import staff_auth as staff_auth_module
from aftercare.services.business import columns, digest
from aftercare.services.fulfillment import FulfillmentService
from aftercare.services.staff_auth import StaffAuthService
from aftercare.workers.worker import run_once

pytestmark = pytest.mark.integration

PASSWORD = "B2-Test-Password-123!"
STAFF_COOKIE = "aftercare_staff_session"
CUSTOMER_COOKIE = "aftercare_session"


@pytest.fixture
def api(service):
    settings = Settings(
        _env_file=None,
        app_env="test",
        demo_login_enabled=True,
        warehouse_simulation_enabled=True,
        database_url=service.sessions.kw["bind"].url.render_as_string(hide_password=False),
    )
    setup_checkpoints(settings)
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
            client=lambda: stack.enter_context(TestClient(app)),
        )


def customer(api, identity="alice"):
    client = api.client()
    response = client.post("/api/demo-sessions", json={"customer_id": identity})
    assert response.status_code == 200, response.text
    client.headers["X-CSRF-Token"] = response.json()["csrf_token"]
    return client


def staff(api, role="support"):
    client = api.client()
    response = client.post(
        "/api/staff/sessions", json={"username": f"b2-{role}", "password": PASSWORD}
    )
    assert response.status_code == 200, response.text
    client.headers["X-CSRF-Token"] = response.json()["csrf_token"]
    return client


def prepared(service, *, owner="alice", order="S1002", kind="exchange"):
    conversation = service.create_conversation(owner)
    preview = service.preview(
        owner,
        conversation,
        ActionInput(
            action=kind,
            order_id=order,
            line_id=f"{order}-1",
            quantity=1,
            target_variant="tee-white-L" if kind == "exchange" else None,
        ),
    )
    service.confirm(owner, preview["id"], True)
    return service.commit(owner, preview["id"])


def post(client, application_id, path, version, *, key=None, headers=None, **data):
    return client.post(
        f"/api/applications/{application_id}/{path}",
        json={"expected_version": version, **data},
        headers={"Idempotency-Key": key or f"{path}:{version}", **(headers or {})},
    )


def success(response, status):
    assert response.status_code == 200, response.text
    result = response.json()
    assert result["status"] == status
    return result


def error(response, status, code=None):
    assert response.status_code == status, response.text
    if code:
        assert response.json().get("error_code", response.json().get("detail")) == code


def persisted(service, application_id):
    with service.sessions() as session:
        app = session.get(Application, application_id)
        return {
            "application": columns(app),
            "fulfillment": columns(session.get(ExchangeFulfillment, application_id)),
            "inventory": columns(session.get(Inventory, "tee-white-L")),
            "reservation": columns(session.get(Reservation, application_id)),
            "operation": columns(session.get(Operation, app.operation_id)),
            "commands": [
                columns(row)
                for row in session.scalars(
                    select(FulfillmentCommand)
                    .where(FulfillmentCommand.application_id == application_id)
                    .order_by(FulfillmentCommand.id)
                )
            ],
            "audits": [
                columns(row)
                for row in session.scalars(
                    select(FulfillmentAudit)
                    .where(FulfillmentAudit.application_id == application_id)
                    .order_by(FulfillmentAudit.to_version)
                )
            ],
        }


def to_pending(api, service, application_id):
    owner, support, warehouse = customer(api), staff(api), staff(api, "warehouse")
    success(post(support, application_id, "review", 1, decision="approve"), "AWAITING_RETURN")
    success(
        post(
            owner,
            application_id,
            "return-shipment",
            2,
            carrier="Simulator",
            tracking_number="RET-1",
        ),
        "RETURN_IN_TRANSIT",
    )
    success(post(warehouse, application_id, "receive", 3, note="Received"), "RETURN_RECEIVED")
    success(post(warehouse, application_id, "replacement-shipments", 4), "REPLACEMENT_PENDING")
    return owner, support, warehouse


def test_three_sessions_complete_exchange_created_by_original_http_worker_confirmation(
    api, service
):
    owner, support, warehouse = customer(api), staff(api), staff(api, "warehouse")
    conversation = owner.post("/api/conversations").json()["id"]
    accepted = owner.post(
        f"/api/conversations/{conversation}/messages",
        json={"text": "S1002 中 1 件换大一码", "client_message_id": "b2-original-chain"},
    )
    assert accepted.status_code == 202, accepted.text
    assert run_once(api.settings)
    snapshot = owner.get(f"/api/conversations/{conversation}").json()
    preview = snapshot["previews"][0]["id"]
    assert (
        owner.post(f"/api/previews/{preview}/confirm", json={"decision": "accept"}).status_code
        == 202
    )
    assert run_once(api.settings)
    snapshot = owner.get(f"/api/conversations/{conversation}").json()
    original = snapshot["operations"][0]
    application_id = original["application_id"]
    assert original["result"] == "EXCHANGE_REQUESTED"

    initial = owner.get(f"/api/applications/{application_id}").json()
    assert initial["status"] == "EXCHANGE_REQUESTED"
    assert initial["receipt"] == original
    assert "withdraw" in initial["allowed_actions"]
    assert "complete" not in initial["allowed_actions"]
    review = success(
        post(support, application_id, "review", 1, decision="approve", note="Approved via session"),
        "AWAITING_RETURN",
    )
    assert review["version"] == 2
    success(
        post(
            owner,
            application_id,
            "return-shipment",
            2,
            carrier="Simulator",
            tracking_number="RET-1",
        ),
        "RETURN_IN_TRANSIT",
    )
    success(post(warehouse, application_id, "receive", 3, note="Received"), "RETURN_RECEIVED")
    pending = success(
        post(warehouse, application_id, "replacement-shipments", 4), "REPLACEMENT_PENDING"
    )
    detail = warehouse.get(f"/api/applications/{application_id}").json()
    assert detail["status"] == pending["status"]
    assert detail["fulfillment"].get("external_shipment_id") is None
    assert "simulate_shipment" in detail["allowed_actions"]
    assert (
        "complete" not in owner.get(f"/api/applications/{application_id}").json()["allowed_actions"]
    )
    shipped = success(
        post(warehouse, application_id, "simulate-shipment", 5), "REPLACEMENT_SHIPPED"
    )
    success(post(owner, application_id, "complete", shipped["version"]), "COMPLETED")
    final = owner.get(f"/api/applications/{application_id}").json()
    assert (final["status"], final["version"]) == ("COMPLETED", 7)
    assert final["fulfillment"]["external_shipment_id"].startswith("SIM-")
    assert final["receipt"] == original
    assert final["allowed_actions"] == []
    history = owner.get(f"/api/applications/{application_id}/events").json()["items"]
    assert len(history) == 7
    assert [row["actor_source"] for row in history[1:]] == [
        "server_session",
        "server_session",
        "server_session",
        "server_session",
        "trusted_adapter",
        "server_session",
    ]
    assert [row["actor_id"] for row in history[1:5]] == [
        api.accounts["support"]["id"],
        "alice",
        api.accounts["warehouse"]["id"],
        api.accounts["warehouse"]["id"],
    ]
    state = persisted(service, application_id)
    assert state["reservation"]["status"] == "CONSUMED"
    assert (state["inventory"]["available"], state["inventory"]["reserved"]) == (0, 0)


def test_login_sets_private_cookie_and_reports_only_server_owned_identity(api):
    client = api.client()
    response = client.post(
        "/api/staff/sessions", json={"username": "b2-support", "password": PASSWORD}
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["actor_id"] == api.accounts["support"]["id"]
    assert body["role"] == "support"
    assert not {"token", "password", "password_hash"} & body.keys()
    cookie = response.headers["set-cookie"].lower()
    assert "httponly" in cookie and "samesite=strict" in cookie
    session = client.get("/api/fulfillment/session").json()
    assert session["actor_id"] == body["actor_id"]
    assert session["role"] == "support"
    assert session["csrf_token"] == body["csrf_token"]
    error(client.get("/api/session"), 401)


@pytest.mark.parametrize(
    "username,password", [("missing-user", PASSWORD), ("b2-support", "wrong-password")]
)
def test_invalid_staff_credentials_do_not_create_a_session(api, username, password):
    client = api.client()
    error(
        client.post("/api/staff/sessions", json={"username": username, "password": password}), 401
    )
    assert client.cookies.get(STAFF_COOKIE) is None
    error(client.get("/api/fulfillment/session"), 401)


def test_unauthenticated_requests_cannot_read_or_execute_commands(api, service):
    application_id = prepared(service)["application_id"]
    client = api.client()
    for path in (
        "/api/applications",
        f"/api/applications/{application_id}",
        f"/api/applications/{application_id}/events",
    ):
        error(client.get(path), 401)
    error(post(client, application_id, "withdraw", 1), 401)


@pytest.mark.parametrize(
    "field,value",
    [
        ("actor", {"actor_id": "forged", "role": "support", "source": "server_session"}),
        ("role", "support"),
        ("customer_id", "alice"),
        ("fault", "before_commit"),
    ],
)
def test_write_dto_rejects_client_supplied_identity_and_test_controls(api, service, field, value):
    application_id = prepared(service)["application_id"]
    support = staff(api)
    before = persisted(service, application_id)
    response = post(support, application_id, "review", 1, decision="approve", **{field: value})
    error(response, 422)
    assert persisted(service, application_id) == before


def test_identity_headers_cannot_elevate_customer_or_replace_authenticated_staff(api, service):
    application_id = prepared(service)["application_id"]
    owner, support = customer(api), staff(api)
    forged = {
        "X-Role": "support",
        "X-Actor-Id": "forged",
        "X-Customer-Id": "alice",
        "X-Actor-Source": "trusted_adapter",
    }
    before = persisted(service, application_id)
    error(post(owner, application_id, "review", 1, decision="approve", headers=forged), 403)
    assert persisted(service, application_id) == before
    success(
        post(support, application_id, "review", 1, decision="approve", headers=forged),
        "AWAITING_RETURN",
    )
    audit = persisted(service, application_id)["audits"][-1]
    assert (audit["actor_id"], audit["actor_role"], audit["actor_source"]) == (
        api.accounts["support"]["id"],
        "support",
        "server_session",
    )


@pytest.mark.parametrize(
    "role,path,body",
    [
        ("customer", "review", {"decision": "approve"}),
        ("support", "receive", {}),
        ("warehouse", "review", {"decision": "approve"}),
        ("support", "replacement-shipments", {}),
        ("warehouse", "complete", {}),
    ],
)
def test_api_roles_cannot_bypass_domain_permissions(api, service, role, path, body):
    application_id = prepared(service)["application_id"]
    client = customer(api) if role == "customer" else staff(api, role)
    before = persisted(service, application_id)
    error(post(client, application_id, path, 1, **body), 403)
    assert persisted(service, application_id) == before


def test_customer_ownership_applies_to_list_detail_audit_and_write(api, service):
    with service.sessions.begin() as session:
        session.get(Inventory, "tee-white-L").available = 2
    alice_id = prepared(service)["application_id"]
    bob_id = prepared(service, owner="bob", order="S2001")["application_id"]
    bob = customer(api, "bob")
    visible = bob.get("/api/applications").json()["items"]
    assert [row["application_id"] for row in visible] == [bob_id]
    before = persisted(service, alice_id)
    for path in (f"/api/applications/{alice_id}", f"/api/applications/{alice_id}/events"):
        error(bob.get(path, headers={"X-Customer-Id": "alice"}), 404, "NOT_FOUND")
    error(post(bob, alice_id, "withdraw", 1, headers={"X-Customer-Id": "alice"}), 404, "NOT_FOUND")
    assert persisted(service, alice_id) == before


def test_staff_list_cursor_is_stable_and_excludes_return_applications(api, service):
    with service.sessions.begin() as session:
        session.get(Inventory, "tee-white-L").available = 2
    exchange = {
        prepared(service)["application_id"],
        prepared(service, owner="bob", order="S2001")["application_id"],
    }
    prepared(service, kind="return")
    support = staff(api)
    first = support.get("/api/applications?limit=1").json()
    assert len(first["items"]) == 1 and first["next_cursor"]
    second = support.get(
        "/api/applications", params={"limit": 1, "after": first["next_cursor"]}
    ).json()
    assert len(second["items"]) == 1
    assert {first["items"][0]["application_id"], second["items"][0]["application_id"]} == exchange
    assert not second["next_cursor"]


@pytest.mark.parametrize(
    "headers",
    [
        {"X-CSRF-Token": ""},
        {"X-CSRF-Token": "wrong-token"},
        {"Origin": "https://evil.example"},
    ],
)
def test_staff_write_rejects_missing_wrong_csrf_or_cross_origin(api, service, headers):
    application_id = prepared(service)["application_id"]
    support = staff(api)
    before = persisted(service, application_id)
    error(post(support, application_id, "review", 1, decision="approve", headers=headers), 403)
    assert persisted(service, application_id) == before


def test_customer_write_and_staff_login_enforce_origin_and_csrf(api, service):
    application_id = prepared(service)["application_id"]
    owner = customer(api)
    error(post(owner, application_id, "withdraw", 1, headers={"X-CSRF-Token": ""}), 403)
    error(
        post(owner, application_id, "withdraw", 1, headers={"Origin": "https://evil.example"}), 403
    )
    client = api.client()
    error(
        client.post(
            "/api/staff/sessions",
            json={"username": "b2-support", "password": PASSWORD},
            headers={"Origin": "https://evil.example"},
        ),
        403,
    )
    assert client.cookies.get(STAFF_COOKIE) is None


def test_staff_logout_revokes_the_saved_cookie_on_the_server(api, service):
    application_id = prepared(service)["application_id"]
    support = staff(api)
    saved = support.cookies.get(STAFF_COOKIE)
    response = support.delete("/api/staff/session")
    assert response.status_code == 200, response.text
    support.cookies.clear()
    support.cookies.set(STAFF_COOKIE, saved)
    error(support.get("/api/staff/session"), 401)
    error(post(support, application_id, "review", 1, decision="approve"), 401)


def test_expired_staff_and_customer_sessions_cannot_be_reused(api, service):
    support, owner = staff(api), customer(api)
    staff_session = api.auth.authenticate(support.cookies.get(STAFF_COOKIE))["session_hash"]
    with service.sessions.begin() as session:
        session.get(StaffSession, staff_session).expires_at = utc_now() - timedelta(seconds=1)
        session.get(DemoSession, digest(owner.cookies.get(CUSTOMER_COOKIE))).expires_at = (
            utc_now() - timedelta(seconds=1)
        )
    error(support.get("/api/fulfillment/session"), 401)
    error(owner.get("/api/fulfillment/session"), 401)


@pytest.mark.parametrize(
    "changes", [{"active": False}, {"role": "warehouse"}, {"password": "Changed-B2-Password-123!"}]
)
def test_account_changes_invalidate_preexisting_sessions(api, service, changes):
    application_id = prepared(service)["application_id"]
    support = staff(api)
    api.auth.update_account(api.accounts["support"]["id"], **changes)
    before = persisted(service, application_id)
    error(support.get("/api/fulfillment/session"), 401)
    error(post(support, application_id, "review", 1, decision="approve"), 401)
    assert persisted(service, application_id) == before


def test_role_is_reloaded_from_the_account_instead_of_the_login_response(api, service):
    application_id = prepared(service)["application_id"]
    support = staff(api)
    with service.sessions.begin() as session:
        session.get(StaffAccount, api.accounts["support"]["id"]).role = "warehouse"
    current = support.get("/api/fulfillment/session")
    assert current.status_code == 200, current.text
    assert current.json()["role"] == "warehouse"
    error(post(support, application_id, "review", 1, decision="approve"), 403)


def test_customer_logout_and_identity_switch_do_not_leave_reusable_mixed_sessions(api):
    client = customer(api)
    response = client.post(
        "/api/staff/sessions", json={"username": "b2-support", "password": PASSWORD}
    )
    assert response.status_code == 200, response.text
    assert client.cookies.get(CUSTOMER_COOKIE) is None
    assert client.cookies.get(STAFF_COOKIE)
    response = client.post("/api/demo-sessions", json={"customer_id": "alice"})
    assert response.status_code == 200, response.text
    assert client.cookies.get(STAFF_COOKIE) is None
    client.headers["X-CSRF-Token"] = response.json()["csrf_token"]
    saved_customer = client.cookies.get(CUSTOMER_COOKIE)
    assert client.delete("/api/session").status_code == 200
    client.cookies.clear()
    client.cookies.set(CUSTOMER_COOKIE, saved_customer)
    # Logging out must invalidate the server record, including a copied cookie.
    error(client.get("/api/fulfillment/session"), 401)


def test_simultaneous_staff_and_customer_cookies_are_rejected_as_ambiguous(api, service):
    application_id = prepared(service)["application_id"]
    support, owner = staff(api), customer(api)
    support.cookies.set(CUSTOMER_COOKIE, owner.cookies.get(CUSTOMER_COOKIE))
    error(support.get("/api/fulfillment/session"), 401, "AMBIGUOUS_IDENTITY")
    error(support.get(f"/api/applications/{application_id}"), 401, "AMBIGUOUS_IDENTITY")
    error(post(support, application_id, "review", 1, decision="approve"), 401, "AMBIGUOUS_IDENTITY")


def test_exact_http_replay_keeps_original_version_and_conflicting_input_is_rejected(api, service):
    application_id = prepared(service)["application_id"]
    support = staff(api)
    first = success(
        post(
            support,
            application_id,
            "review",
            1,
            key="review-once",
            decision="approve",
            note="Original",
        ),
        "AWAITING_RETURN",
    )
    before = persisted(service, application_id)
    session_hash = api.auth.authenticate(support.cookies.get(STAFF_COOKIE))["session_hash"]
    with service.sessions() as session:
        auth_before = {
            "account": columns(session.get(StaffAccount, api.accounts["support"]["id"])),
            "session": columns(session.get(StaffSession, session_hash)),
        }
    replay = post(
        support, application_id, "review", 1, key="review-once", decision="approve", note="Original"
    )
    assert replay.status_code == 200 and replay.json() == first
    error(
        post(
            support,
            application_id,
            "review",
            1,
            key="review-once",
            decision="approve",
            note="Changed",
        ),
        409,
        "IDEMPOTENCY_CONFLICT",
    )
    error(
        post(support, application_id, "review", 1, key="other-command", decision="reject"),
        409,
        "VERSION_CONFLICT",
    )
    assert persisted(service, application_id) == before
    with service.sessions() as session:
        assert {
            "account": columns(session.get(StaffAccount, api.accounts["support"]["id"])),
            "session": columns(session.get(StaffSession, session_hash)),
        } == auth_before


def test_simulated_dispatch_retains_the_first_real_staff_trigger_across_other_staff_replay(
    api, service
):
    application_id = prepared(service)["application_id"]
    _owner, _support, warehouse = to_pending(api, service, application_id)
    api.auth.create_account("b2-warehouse-other", PASSWORD, "warehouse")
    other = api.client()
    login = other.post(
        "/api/staff/sessions",
        json={"username": "b2-warehouse-other", "password": PASSWORD},
    )
    assert login.status_code == 200, login.text
    other.headers["X-CSRF-Token"] = login.json()["csrf_token"]
    first = success(
        post(
            warehouse,
            application_id,
            "simulate-shipment",
            5,
            key="same-simulation",
            headers={"X-Actor-Id": "forged-trigger"},
        ),
        "REPLACEMENT_SHIPPED",
    )
    before = persisted(service, application_id)
    audit = before["audits"][-1]
    assert audit["actor_source"] == "trusted_adapter"
    assert audit["details"]["triggered_by"] == {
        "actor_id": api.accounts["warehouse"]["id"],
        "role": "warehouse",
        "source": "server_session",
    }
    assert login.json()["actor_id"] != api.accounts["warehouse"]["id"]
    replay = post(other, application_id, "simulate-shipment", 5, key="same-simulation")
    assert replay.status_code == 200, replay.text
    assert replay.json() == first
    assert persisted(service, application_id) == before
    history = other.get(f"/api/applications/{application_id}/events").json()["items"]
    assert history[-1]["details"]["triggered_by"] == audit["details"]["triggered_by"]


def test_invalid_login_inputs_and_credentials_never_echo_passwords(api):
    client = api.client()
    for invalid_password in (PASSWORD * 10, {"raw_password": PASSWORD}):
        response = client.post(
            "/api/staff/sessions",
            json={"username": "b2-support", "password": invalid_password},
        )
        error(response, 422)
        assert PASSWORD not in response.text
        assert "raw_password" not in response.text
    response = client.post(
        "/api/staff/sessions",
        json={"username": "b2-support", "password": PASSWORD + "-incorrect"},
    )
    error(response, 401)
    assert PASSWORD not in response.text


def test_http_review_and_customer_withdraw_race_allow_only_one_version_change(api, service):
    application_id = prepared(service)["application_id"]
    support, owner = staff(api), customer(api)
    barrier = Barrier(2)

    def call(client, path, data):
        barrier.wait(timeout=5)
        return post(client, application_id, path, 1, **data)

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [
            pool.submit(call, support, "review", {"decision": "approve"}),
            pool.submit(call, owner, "withdraw", {}),
        ]
        responses = [future.result(timeout=15) for future in futures]
    assert sorted(response.status_code for response in responses) == [200, 409]
    error(
        next(response for response in responses if response.status_code == 409),
        409,
        "VERSION_CONFLICT",
    )
    state = persisted(service, application_id)
    assert state["application"]["version"] == 2
    assert len(state["commands"]) == 1 and len(state["audits"]) == 2


def test_demo_and_simulation_require_explicit_flags_and_simulation_cannot_take_external_identity(
    api, service
):
    application_id = prepared(service)["application_id"]
    _owner, _support, warehouse = to_pending(api, service, application_id)
    before = persisted(service, application_id)
    error(
        post(warehouse, application_id, "simulate-shipment", 5, external_shipment_id="FORGED"), 422
    )
    error(
        post(warehouse, application_id, "confirm-shipment", 5, external_shipment_id="FORGED"), 404
    )
    api.settings.warehouse_simulation_enabled = False
    error(post(warehouse, application_id, "simulate-shipment", 5), 404)
    detail = warehouse.get(f"/api/applications/{application_id}").json()
    assert "simulate_shipment" not in detail["allowed_actions"]
    assert detail["status"] == "REPLACEMENT_PENDING"
    api.settings.demo_login_enabled = False
    fresh = api.client()
    error(fresh.post("/api/demo-sessions", json={"customer_id": "alice"}), 404)
    assert fresh.get("/api/auth/options").json() == {
        "demo_customer_login": False,
        "warehouse_simulation": False,
    }
    assert persisted(service, application_id) == before


def test_idempotency_header_and_strict_version_are_required(api, service):
    application_id = prepared(service)["application_id"]
    support = staff(api)
    before = persisted(service, application_id)
    error(
        support.post(
            f"/api/applications/{application_id}/review",
            json={"decision": "approve", "expected_version": 1},
        ),
        422,
    )
    for version in (0, "1", True):
        error(post(support, application_id, "review", version, decision="approve"), 422)
    assert persisted(service, application_id) == before


def test_http_command_holds_auth_guard_until_commit_before_concurrent_revocation(
    api, service, monkeypatch
):
    application_id = prepared(service)["application_id"]
    support = staff(api)
    staff_id = api.accounts["support"]["id"]
    entered, release, revoker_started = Event(), Event(), Event()
    guarded_pids, revoker_pids = [], []
    original_execute = FulfillmentService.execute
    original_lock = staff_auth_module._lock

    def pause_after_auth_guard(self, actor, app_id, value, **kwargs):
        if actor.actor_id == staff_id and app_id == application_id and value.command == "approve":
            guarded_pids.append(self.sessions.kw["bind"].scalar(text("SELECT pg_backend_pid()")))
            entered.set()
            if not release.wait(timeout=8):
                raise RuntimeError("HTTP_GUARD_PROBE_RELEASE_TIMEOUT")
        return original_execute(self, actor, app_id, value, **kwargs)

    def observe_revocation_pid(session, model, identity, *, shared=False):
        if model is StaffAccount and identity == staff_id and not shared:
            revoker_pids.append(session.scalar(text("SELECT pg_backend_pid()")))
            revoker_started.set()
        return original_lock(session, model, identity, shared=shared)

    monkeypatch.setattr(FulfillmentService, "execute", pause_after_auth_guard)
    monkeypatch.setattr(staff_auth_module, "_lock", observe_revocation_pid)
    observed = None
    with ThreadPoolExecutor(max_workers=2) as pool:
        approval = pool.submit(post, support, application_id, "review", 1, decision="approve")
        revocation = None
        try:
            assert entered.wait(timeout=5), "HTTP command did not reach its guarded transaction"
            revocation = pool.submit(api.auth.revoke_all, staff_id)
            assert revoker_started.wait(timeout=5), "Revocation did not acquire a DB connection"
            deadline = monotonic() + 2
            with service.sessions() as observer:
                while monotonic() < deadline:
                    row = (
                        observer.execute(
                            text(
                                "SELECT wait_event_type, pg_blocking_pids(pid) AS blocking_pids "
                                "FROM pg_stat_activity WHERE pid=:pid"
                            ),
                            {"pid": revoker_pids[0]},
                        )
                        .mappings()
                        .one_or_none()
                    )
                    observer.rollback()
                    if (
                        row is not None
                        and row["wait_event_type"] == "Lock"
                        and guarded_pids[0] in row["blocking_pids"]
                    ):
                        observed = dict(row)
                        break
                    if revocation.done():
                        break
                    # Poll a database lock relationship; elapsed time is not the pass condition.
                    sleep(0.01)
            assert observed is not None, (
                "Revocation was not blocked by the guarded HTTP transaction"
            )
        finally:
            release.set()
        response = approval.result(timeout=15)
        assert revocation is not None
        revoked = revocation.result(timeout=15)
    success(response, "AWAITING_RETURN")
    assert revoked["auth_version"] == api.accounts["support"]["auth_version"] + 1
    before = persisted(service, application_id)
    assert before["application"]["version"] == 2
    assert len(before["commands"]) == 1 and len(before["audits"]) == 2
    error(post(support, application_id, "review", 2, key="after-revoke", decision="reject"), 401)
    assert persisted(service, application_id) == before


def test_revocation_between_dependency_identity_and_write_guard_rejects_stale_identity(
    api, service, monkeypatch
):
    application_id = prepared(service)["application_id"]
    support = staff(api)
    staff_id = api.accounts["support"]["id"]
    before = persisted(service, application_id)
    original_guard = fulfillment_api._guard_identity
    observed = []

    def revoke_before_write_guard(session, request, who):
        assert who.actor.actor_id == staff_id and who.actor.source == "server_session"
        observed.append(who.actor)
        api.auth.revoke_all(staff_id)
        return original_guard(session, request, who)

    monkeypatch.setattr(fulfillment_api, "_guard_identity", revoke_before_write_guard)
    error(post(support, application_id, "review", 1, decision="approve"), 401, "SESSION_EXPIRED")
    assert len(observed) == 1
    assert persisted(service, application_id) == before
    error(support.get("/api/fulfillment/session"), 401)
