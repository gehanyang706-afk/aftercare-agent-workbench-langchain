"""Independent SQLite provider contracts; no product database or external network."""

import hashlib
import sqlite3
import stat
from concurrent.futures import ThreadPoolExecutor

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from aftercare.domain.warehouse import (
    PROVIDER_ID,
    ShipmentInput,
    ShipmentResult,
    shipment_request_hash,
    verify_shipment_hash,
)
from aftercare.mock_warehouse.app import create_app
from aftercare.mock_warehouse.store import (
    APPLICATION_ID,
    SCHEMA_VERSION,
    WarehouseConflict,
    WarehouseStateError,
    WarehouseStore,
)
from aftercare.services.business import digest

TOKEN = "warehouse-local-test-token-" + "x" * 32
AUTH = {"Authorization": "Bearer " + TOKEN}
POST = "/mock-warehouse/shipments"
GET = "/mock-warehouse/shipments/by-key/"


def payload(*, effect_key="exchange:app-1", application_id="app-1", quantity=1):
    values = {
        "effect_key": effect_key,
        "application_id": application_id,
        "variant_id": "白色-L",
        "quantity": quantity,
    }
    return {**values, "request_hash": shipment_request_hash(**values)}


def configure(client, mode, effect_key="exchange:app-1"):
    response = client.post(
        "/__test/faults", json={"effect_key": effect_key, "mode": mode}, headers=AUTH
    )
    assert response.status_code == 200


def test_digest_exactly_matches_product_canonical_unicode_digest():
    body = payload(effect_key="换货:app-1")
    assert body["request_hash"] == digest({k: v for k, v in body.items() if k != "request_hash"})
    assert verify_shipment_hash(ShipmentInput.model_validate(body))


@pytest.mark.parametrize(
    "token", [None, "short", "x" * 257, "x" * 31 + " ", "x" * 31 + "\n", "密" * 32]
)
def test_invalid_provider_token_is_rejected_before_creating_state(tmp_path, token):
    path = tmp_path / "state.sqlite"
    with pytest.raises(ValueError, match="WAREHOUSE_TOKEN_REQUIRED"):
        create_app(path, token)
    assert not path.exists()


@pytest.mark.parametrize("test_mode", ["false", 1, None])
def test_fault_mode_requires_an_explicit_boolean_before_creating_state(tmp_path, test_mode):
    path = tmp_path / "warehouse.sqlite"
    with pytest.raises(ValueError, match="WAREHOUSE_TEST_MODE_INVALID"):
        create_app(path, TOKEN, test_mode=test_mode)
    assert not path.exists()


def test_authenticated_create_query_and_ready_have_no_secret_or_path_echo(tmp_path):
    path = tmp_path / "warehouse.sqlite"
    app = create_app(path, TOKEN)
    with TestClient(app) as client:
        ready = client.get("/health/ready")
        assert ready.status_code == 200 and ready.json() == {"status": "ready"}
        response = client.post(POST, json=payload(), headers=AUTH)
        assert response.status_code == 200
        result = ShipmentResult.model_validate(response.json())
        assert result.status == "SUCCEEDED" and result.external_shipment_id.startswith("MOCK-")
        assert result.effect_key == payload()["effect_key"]
        assert result.application_id == payload()["application_id"]
        assert result.request_hash == payload()["request_hash"]
        assert client.get(GET + payload()["effect_key"], headers=AUTH).json() == response.json()
        assert str(path) not in ready.text + response.text
        assert TOKEN not in ready.text + response.text
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    with sqlite3.connect(path) as connection:
        assert connection.execute("PRAGMA application_id").fetchone()[0] == APPLICATION_ID
        assert connection.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
        assert connection.execute("PRAGMA journal_mode").fetchone()[0] == "wal"


@pytest.mark.parametrize(
    "authorization", [None, "Bearer wrong-private-token", TOKEN, "Basic " + TOKEN]
)
def test_post_get_and_test_faults_require_exact_bearer(tmp_path, authorization):
    app = create_app(tmp_path / "warehouse.sqlite", TOKEN, test_mode=True)
    headers = {} if authorization is None else {"Authorization": authorization}
    with TestClient(app) as client:
        responses = [
            client.post(POST, json=payload(), headers=headers),
            client.get(GET + "exchange:app-1", headers=headers),
            client.post(
                "/__test/faults",
                json={"effect_key": "exchange:app-1", "mode": "permanent_reject"},
                headers=headers,
            ),
        ]
    for response in responses:
        assert response.status_code == 401
        assert response.json()["code"] == "UNAUTHORIZED"
        assert TOKEN not in response.text
        assert "wrong-private-token" not in response.text
    assert app.state.store.get("exchange:app-1") is None


@pytest.mark.parametrize(
    "change",
    [
        {"extra": "private-field-value"},
        {"quantity": True},
        {"quantity": "1"},
        {"quantity": 0},
        {"quantity": 1001},
        {"effect_key": "x" * 201},
        {"application_id": "x" * 101},
        {"variant_id": "x" * 101},
        {"request_hash": "not-a-hash"},
    ],
)
def test_strict_dto_rejects_invalid_data_without_echo_or_acceptance(tmp_path, change):
    app = create_app(tmp_path / "warehouse.sqlite", TOKEN)
    with TestClient(app) as client:
        response = client.post(POST, json={**payload(), **change}, headers=AUTH)
    assert response.status_code == 422
    assert response.json() == {
        "provider": PROVIDER_ID,
        "code": "INVALID_INPUT",
        "accepted": False,
        "retryable": False,
    }
    assert "private-field-value" not in response.text
    assert app.state.store.get("exchange:app-1") is None


def test_digest_mismatch_cannot_create_or_modify_a_binding(tmp_path):
    app = create_app(tmp_path / "warehouse.sqlite", TOKEN)
    with TestClient(app) as client:
        response = client.post(POST, json={**payload(), "request_hash": "0" * 64}, headers=AUTH)
        assert response.status_code == 400
        assert response.json()["code"] == "INVALID_REQUEST_HASH"
        assert app.state.store.get("exchange:app-1") is None
        first = client.post(POST, json=payload(), headers=AUTH)
        assert first.status_code == 200
        assert (
            client.post(
                POST, json={**payload(), "request_hash": "0" * 64}, headers=AUTH
            ).status_code
            == 400
        )
        assert client.get(GET + "exchange:app-1", headers=AUTH).json() == first.json()


def test_success_response_type_requires_status_and_rejects_extra_fields():
    result = {
        "effect_key": "key",
        "application_id": "app",
        "request_hash": "1" * 64,
        "external_shipment_id": "MOCK-test",
    }
    with pytest.raises(ValidationError):
        ShipmentResult.model_validate(result)
    with pytest.raises(ValidationError):
        ShipmentResult.model_validate({**result, "status": "SUCCEEDED", "accepted": True})
    with pytest.raises(ValidationError):
        ShipmentResult.model_validate(
            {**result, "status": "SUCCEEDED", "external_shipment_id": "   "}
        )


def test_same_key_same_payload_returns_same_result_after_restart(tmp_path):
    path = tmp_path / "warehouse.sqlite"
    with TestClient(create_app(path, TOKEN)) as client:
        first = client.post(POST, json=payload(), headers=AUTH)
        assert first.status_code == 200
        assert client.post(POST, json=payload(), headers=AUTH).json() == first.json()
    with TestClient(create_app(path, TOKEN)) as restarted:
        assert restarted.get(GET + "exchange:app-1", headers=AUTH).json() == first.json()
        assert restarted.post(POST, json=payload(), headers=AUTH).json() == first.json()


@pytest.mark.parametrize(
    "other,code",
    [
        (payload(quantity=2), "IDEMPOTENCY_CONFLICT"),
        (payload(application_id="app-2"), "IDEMPOTENCY_CONFLICT"),
        (payload(effect_key="different-key"), "APPLICATION_CONFLICT"),
    ],
)
def test_conflicts_do_not_mutate_success_binding(tmp_path, other, code):
    app = create_app(tmp_path / "warehouse.sqlite", TOKEN)
    with TestClient(app) as client:
        first = client.post(POST, json=payload(), headers=AUTH)
        rejected = client.post(POST, json=other, headers=AUTH)
        assert rejected.status_code == 409
        assert rejected.json()["code"] == code and rejected.json()["accepted"] is False
        assert client.get(GET + "exchange:app-1", headers=AUTH).json() == first.json()
        if other["effect_key"] != "exchange:app-1":
            assert app.state.store.get(other["effect_key"]) is None


def test_concurrent_duplicate_acceptance_creates_exactly_one_persistent_shipment(tmp_path):
    path = tmp_path / "warehouse.sqlite"
    store = WarehouseStore(path)
    body = ShipmentInput.model_validate(payload())
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: store.accept(body), range(24)))
    assert sum(created for _, created in results) == 1
    assert len({result.external_shipment_id for result, _ in results}) == 1
    with sqlite3.connect(path) as connection:
        assert connection.execute("SELECT count(*) FROM shipments").fetchone()[0] == 1


def test_concurrent_different_keys_cannot_ship_same_application_twice(tmp_path):
    path = tmp_path / "warehouse.sqlite"
    store = WarehouseStore(path)

    def accept(index):
        try:
            store.accept(ShipmentInput.model_validate(payload(effect_key=f"key-{index}")))
        except WarehouseConflict as error:
            assert error.code == "APPLICATION_CONFLICT"
            return False
        return True

    with ThreadPoolExecutor(max_workers=8) as pool:
        assert sum(pool.map(accept, range(16))) == 1
    with sqlite3.connect(path) as connection:
        assert connection.execute("SELECT count(*) FROM shipments").fetchone()[0] == 1


def test_transaction_failure_after_insert_rolls_back_entire_acceptance(tmp_path, monkeypatch):
    store = WarehouseStore(tmp_path / "warehouse.sqlite")
    original = store._insert_shipment

    def fail_after_insert(*args):
        original(*args)
        raise RuntimeError("INJECTED_PROVIDER_TRANSACTION_FAILURE")

    monkeypatch.setattr(store, "_insert_shipment", fail_after_insert)
    with pytest.raises(RuntimeError, match="INJECTED_PROVIDER_TRANSACTION_FAILURE"):
        store.accept(ShipmentInput.model_validate(payload()))
    assert store.get("exchange:app-1") is None
    monkeypatch.setattr(store, "_insert_shipment", original)
    _, created = store.accept(ShipmentInput.model_validate(payload()))
    assert created


@pytest.mark.parametrize(
    "kind", ["ordinary", "sqlite", "marked_but_foreign", "unsupported_version"]
)
def test_existing_nonprovider_files_are_not_rewritten(tmp_path, kind):
    path = tmp_path / "foreign.sqlite"
    if kind == "ordinary":
        path.write_bytes(b"This file is not warehouse state.\n")
    else:
        with sqlite3.connect(path) as connection:
            connection.execute("CREATE TABLE unrelated (value TEXT)")
            connection.execute("INSERT INTO unrelated VALUES ('preserve-me')")
            if kind in {"marked_but_foreign", "unsupported_version"}:
                connection.execute(f"PRAGMA application_id={APPLICATION_ID}")
                connection.execute(
                    f"PRAGMA user_version={SCHEMA_VERSION if kind == 'marked_but_foreign' else 99}"
                )
    before = hashlib.sha256(path.read_bytes()).hexdigest()
    mode = path.stat().st_mode
    with pytest.raises(WarehouseStateError, match="WAREHOUSE_STATE_INVALID"):
        WarehouseStore(path)
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before
    assert path.stat().st_mode == mode
    assert not path.with_name(path.name + "-wal").exists()
    assert not path.with_name(path.name + "-shm").exists()


def test_symlink_state_path_is_rejected_without_touching_target(tmp_path):
    target = tmp_path / "preserved.txt"
    target.write_bytes(b"preserve")
    link = tmp_path / "warehouse.sqlite"
    link.symlink_to(target)
    with pytest.raises(WarehouseStateError):
        WarehouseStore(link)
    assert target.read_bytes() == b"preserve"


def test_fault_endpoint_is_absent_by_default_and_success_cannot_be_reconfigured(tmp_path):
    path = tmp_path / "warehouse.sqlite"
    with TestClient(create_app(path, TOKEN)) as client:
        assert (
            client.post(
                "/__test/faults",
                json={"effect_key": "key", "mode": "permanent_reject"},
                headers=AUTH,
            ).status_code
            == 404
        )
    with TestClient(create_app(path, TOKEN, test_mode=True)) as client:
        first = client.post(POST, json=payload(), headers=AUTH)
        response = client.post(
            "/__test/faults",
            json={"effect_key": "exchange:app-1", "mode": "permanent_reject"},
            headers=AUTH,
        )
        assert response.status_code == 409
        assert client.post(POST, json=payload(), headers=AUTH).json() == first.json()


def test_retryable_rejection_proves_not_accepted_then_allows_one_acceptance(tmp_path):
    with TestClient(create_app(tmp_path / "warehouse.sqlite", TOKEN, test_mode=True)) as client:
        configure(client, "retryable_reject_once")
        rejected = client.post(POST, json=payload(), headers=AUTH)
        assert rejected.status_code == 503
        assert rejected.json() == {
            "provider": PROVIDER_ID,
            "code": "TEMPORARY_UNAVAILABLE",
            "accepted": False,
            "retryable": True,
        }
        absent = client.get(GET + "exchange:app-1", headers=AUTH)
        assert absent.status_code == 404
        assert absent.json()["accepted"] is False
        first = client.post(POST, json=payload(), headers=AUTH)
        assert first.status_code == 200
        assert client.post(POST, json=payload(), headers=AUTH).json() == first.json()


def test_permanent_rejection_keeps_all_requests_unaccepted(tmp_path):
    with TestClient(create_app(tmp_path / "warehouse.sqlite", TOKEN, test_mode=True)) as client:
        configure(client, "permanent_reject")
        for _ in range(2):
            rejected = client.post(POST, json=payload(), headers=AUTH)
            assert rejected.status_code == 422
            assert rejected.json() == {
                "provider": PROVIDER_ID,
                "code": "PERMANENT_REJECTED",
                "accepted": False,
                "retryable": False,
            }
        assert client.get(GET + "exchange:app-1", headers=AUTH).status_code == 404


def test_error_after_accept_has_no_success_payload_but_query_and_replay_recover(tmp_path):
    path = tmp_path / "warehouse.sqlite"
    with TestClient(create_app(path, TOKEN, test_mode=True)) as client:
        configure(client, "error_after_accept_once")
        unknown = client.post(POST, json=payload(), headers=AUTH)
        assert unknown.status_code == 500
        assert "external_shipment_id" not in unknown.json()
        assert "accepted" not in unknown.json()
        query = client.get(GET + "exchange:app-1", headers=AUTH)
        assert query.status_code == 200
        assert client.post(POST, json=payload(), headers=AUTH).json() == query.json()
    with TestClient(create_app(path, TOKEN)) as restarted:
        assert restarted.get(GET + "exchange:app-1", headers=AUTH).json() == query.json()
