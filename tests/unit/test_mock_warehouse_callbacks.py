"""Provider v2 migration and durable callbacks; no product PostgreSQL or network."""

import hashlib
import hmac
import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from aftercare.domain.warehouse import (
    PROVIDER_ID,
    ShipmentInput,
    WarehouseEvent,
    shipment_request_hash,
    sign_event,
    validate_callback_secret,
    verify_event_signature,
)
from aftercare.mock_warehouse.app import create_app
from aftercare.mock_warehouse.callbacks import CallbackSender
from aftercare.mock_warehouse.store import (
    APPLICATION_ID,
    MAX_CALLBACK_ATTEMPTS,
    SCHEMA_VERSION,
    WarehouseStateError,
    WarehouseStore,
    canonical_json,
)

TOKEN = "provider-bearer-" + "p" * 32
SECRET = "provider-callback-" + "s" * 32
AUTH = {"Authorization": "Bearer " + TOKEN}
BASE_URL = "http://127.0.0.1:8132"
POST = "/mock-warehouse/shipments"
GET = "/mock-warehouse/shipments/by-key/"
KEY = "exchange:app-1"


def request(*, key=KEY, application_id="app-1"):
    payload = {
        "effect_key": key,
        "application_id": application_id,
        "variant_id": "白色-L",
        "quantity": 1,
    }
    return ShipmentInput(**payload, request_hash=shipment_request_hash(**payload))


def v1_fixture(path, *, corrupt=False):
    body = request()
    with sqlite3.connect(path) as connection:
        connection.executescript(
            "CREATE TABLE provider_meta (singleton INTEGER PRIMARY KEY CHECK(singleton=1), "
            "marker TEXT NOT NULL); "
            "CREATE TABLE shipments (effect_key TEXT PRIMARY KEY NOT NULL, "
            "application_id TEXT NOT NULL UNIQUE, request_hash TEXT NOT NULL, "
            "payload_json TEXT NOT NULL, external_shipment_id TEXT NOT NULL UNIQUE, "
            "created_at TEXT NOT NULL);"
        )
        connection.execute("INSERT INTO provider_meta VALUES (1,?)", (PROVIDER_ID,))
        connection.execute(
            "INSERT INTO shipments VALUES (?,?,?,?,?,?)",
            (
                body.effect_key,
                body.application_id,
                "0" * 64 if corrupt else body.request_hash,
                canonical_json(body.request_payload()),
                "MOCK-original-fact",
                "2026-10-27T00:00:00Z",
            ),
        )
        connection.execute(f"PRAGMA application_id={APPLICATION_ID}")
        connection.execute("PRAGMA user_version=1")
        return connection.execute("SELECT * FROM shipments").fetchall()


class Client:
    def __init__(self, outcome="ACKNOWLEDGED"):
        self.outcome = outcome
        self.sent = []

    def send_event(self, event):
        self.sent.append(event.copy())
        return SimpleNamespace(outcome=self.outcome)


def test_event_hmac_matches_independent_raw_body_reference_and_never_reserializes():
    raw = '{"example": "中文"}'.encode()
    timestamp = "1793188800"
    expected = (
        "v1="
        + hmac.new(SECRET.encode(), timestamp.encode() + b"." + raw, hashlib.sha256).hexdigest()
    )
    assert sign_event(SECRET, raw, timestamp) == expected
    assert sign_event(SECRET, raw, int(timestamp)) == expected
    assert verify_event_signature(SECRET, raw, timestamp, expected)
    assert not verify_event_signature(SECRET, raw + b" ", timestamp, expected)
    assert not verify_event_signature(SECRET, raw, str(int(timestamp) + 1), expected)
    assert not verify_event_signature(TOKEN, raw, timestamp, expected)
    assert not verify_event_signature(SECRET, raw, timestamp, expected.upper())
    assert not verify_event_signature(SECRET, raw, True, expected)
    assert not verify_event_signature(SECRET, bytearray(raw), timestamp, expected)


@pytest.mark.parametrize("value", [None, "short", "x" * 257, "x" * 31 + " ", "密" * 32])
def test_callback_secret_validation_has_fixed_safe_error(value):
    with pytest.raises(ValueError, match="^WAREHOUSE_CALLBACK_SECRET_REQUIRED$"):
        validate_callback_secret(value)


@pytest.mark.parametrize(
    "change",
    [
        {"sequence": True},
        {"sequence": 0},
        {"sequence": 2147483648},
        {"extra": "private"},
        {"event_id": " "},
        {"status": "FAILED"},
    ],
)
def test_event_wire_type_is_strict_and_bounded(tmp_path, change):
    store = WarehouseStore(tmp_path / "warehouse.sqlite")
    store.accept(request())
    with pytest.raises(ValidationError):
        WarehouseEvent.model_validate({**store.events(KEY)[0], **change})


def test_v1_migration_preserves_every_shipment_binding_and_backfills_stable_events(tmp_path):
    path = tmp_path / "v1.sqlite"
    before = v1_fixture(path)
    store = WarehouseStore(path)
    assert store.get(KEY).external_shipment_id == "MOCK-original-fact"
    events = store.events(KEY)
    assert [(event["sequence"], event["status"]) for event in events] == [
        (1, "ACCEPTED"),
        (2, "SUCCEEDED"),
    ]
    assert len({event["event_id"] for event in events}) == 2
    assert all(event["external_shipment_id"] == "MOCK-original-fact" for event in events)
    assert all(event["request_hash"] == request().request_hash for event in events)
    with sqlite3.connect(path) as connection:
        assert connection.execute("SELECT * FROM shipments").fetchall() == before
        assert connection.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION == 2
    replay, created = store.accept(request())
    assert not created and replay.external_shipment_id == "MOCK-original-fact"
    assert WarehouseStore(path).events(KEY) == events


def test_failed_v1_backfill_keeps_original_version_and_facts(tmp_path):
    path = tmp_path / "corrupt-v1.sqlite"
    before = v1_fixture(path, corrupt=True)
    with pytest.raises(WarehouseStateError, match="^WAREHOUSE_STATE_INVALID$"):
        WarehouseStore(path)
    with sqlite3.connect(path) as connection:
        assert connection.execute("SELECT * FROM shipments").fetchall() == before
        assert connection.execute("PRAGMA user_version").fetchone()[0] == 1
        assert not connection.execute(
            "SELECT 1 FROM sqlite_master WHERE name='callback_events'"
        ).fetchall()


def test_event_insert_failure_rolls_back_shipment_and_both_events(tmp_path, monkeypatch):
    store = WarehouseStore(tmp_path / "warehouse.sqlite")
    original = store._insert_events

    def fail_after_events(*args):
        original(*args)
        raise RuntimeError("INJECTED_EVENT_FAILURE")

    monkeypatch.setattr(store, "_insert_events", fail_after_events)
    with pytest.raises(RuntimeError, match="INJECTED_EVENT_FAILURE"):
        store.accept(request())
    assert store.get(KEY) is None
    assert store.events(KEY) == []
    monkeypatch.setattr(store, "_insert_events", original)
    assert store.accept(request())[1]
    assert len(store.events(KEY)) == 2


def test_concurrent_acceptance_and_restart_never_regenerate_event_identity(tmp_path):
    path = tmp_path / "warehouse.sqlite"
    store = WarehouseStore(path)
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: store.accept(request()), range(16)))
    assert sum(created for _, created in results) == 1
    events = store.events(KEY)
    assert WarehouseStore(path).events(KEY) == events
    with sqlite3.connect(path) as connection:
        assert connection.execute("SELECT count(*) FROM callback_events").fetchone()[0] == 2
        with pytest.raises(sqlite3.IntegrityError, match="IMMUTABLE_CALLBACK_EVENT"):
            connection.execute("UPDATE callback_events SET payload_json='{}'")


def test_acknowledgement_is_durable_and_transport_runs_outside_sqlite_transaction(tmp_path):
    path = tmp_path / "warehouse.sqlite"
    store = WarehouseStore(path)
    store.accept(request())

    class OutsideTransaction(Client):
        def send_event(self, event):
            if not self.sent:
                # A separate writer must acquire its own lock while HTTP is active.
                store.accept(request(key="other", application_id="app-2"))
            return super().send_event(event)

    client = OutsideTransaction()
    sender = CallbackSender(path, BASE_URL, SECRET, client=client)
    assert sender.work_once(KEY)["status"] == "DELIVERED"
    assert sender.work_once(KEY)["status"] == "DELIVERED"
    assert sender.work_once(KEY) == {"worked": False, "status": "IDLE"}
    restarted = CallbackSender(path, BASE_URL, SECRET, client=Client())
    assert restarted.work_once(KEY) == {"worked": False, "status": "IDLE"}
    assert [event["sequence"] for event in client.sent] == [1, 2]
    assert store.events(KEY) == client.sent


def test_unknown_is_retained_with_backoff_and_same_event_retried_after_restart(tmp_path):
    path = tmp_path / "warehouse.sqlite"
    store = WarehouseStore(path)
    store.accept(request())
    client = Client("UNKNOWN")
    sender = CallbackSender(path, BASE_URL, SECRET, client=client)
    first = sender.work_once(KEY)
    assert first["status"] == "PENDING" and first["attempts"] == 1
    assert sender.work_once(KEY)["sequence"] == 2
    assert sender.work_once(KEY)["status"] == "IDLE"
    with sqlite3.connect(path) as connection:
        rows = connection.execute(
            "SELECT delivery_status,lease_owner,lease_expires_at,next_attempt_at "
            "FROM callback_events"
        ).fetchall()
        assert all(row[:3] == ("PENDING", None, None) and row[3] > 0 for row in rows)
        connection.execute("UPDATE callback_events SET next_attempt_at=0")
    accepted = Client()
    retry = CallbackSender(path, BASE_URL, SECRET, client=accepted).work_once(KEY)
    assert retry["event_id"] == first["event_id"] and retry["attempts"] == 2
    assert retry["status"] == "DELIVERED" and accepted.sent[0] == client.sent[0]


def test_non_ack_business_success_and_exceptions_cannot_mark_callback_delivered(tmp_path):
    path = tmp_path / "warehouse.sqlite"
    store = WarehouseStore(path)
    store.accept(request())
    sender = CallbackSender(path, BASE_URL, SECRET, client=Client("SUCCEEDED"))
    assert sender.work_once(KEY)["status"] == "PENDING"

    class Failing:
        def send_event(self, event):
            raise RuntimeError(SECRET + TOKEN + "private-request")

    result = CallbackSender(path, BASE_URL, SECRET, client=Failing()).work_once(KEY)
    assert result["status"] == "PENDING" and result["outcome"] == "UNKNOWN"
    assert SECRET not in json.dumps(result) and TOKEN not in json.dumps(result)
    with sqlite3.connect(path) as connection:
        assert connection.execute(
            "SELECT DISTINCT last_error_code FROM callback_events"
        ).fetchall() == [("CALLBACK_NOT_ACKNOWLEDGED",)]
    assert b"private-request" not in path.read_bytes()
    assert SECRET.encode() not in path.read_bytes()


def test_expired_sender_cannot_finish_new_owners_claim_and_attempts_are_bounded(tmp_path):
    store = WarehouseStore(tmp_path / "warehouse.sqlite")
    store.accept(request())
    now = 10_000_000_000.0
    old = store.claim_event("old", effect_key=KEY, now=now, lease_seconds=1)
    other = store.claim_event("other", effect_key=KEY, now=now, lease_seconds=20)
    assert store.finish_event(other, acknowledged=True, now=now + 0.1) == "DELIVERED"
    claim = store.claim_event("replacement", effect_key=KEY, now=now + 2, lease_seconds=20)
    assert claim["event"] == old["event"]
    assert claim["generation"] == old["generation"] + 1
    assert store.finish_event(old, acknowledged=True, now=now + 2.1) is None
    assert store.finish_event(claim, acknowledged=False, now=now + 2.2) == "PENDING"
    for attempt in range(3, MAX_CALLBACK_ATTEMPTS + 1):
        now += 32
        claim = store.claim_event("retry", effect_key=KEY, now=now, lease_seconds=20)
        assert claim["attempts"] == attempt
        status = store.finish_event(claim, acknowledged=False, now=now + 0.1)
        assert status == ("DEAD" if attempt == MAX_CALLBACK_ATTEMPTS else "PENDING")
    assert store.claim_event("never", effect_key=KEY, now=now + 60) is None
    assert len(store.events(KEY)) == 2


def test_concurrent_senders_claim_each_due_event_once_and_release_lease(tmp_path):
    path = tmp_path / "warehouse.sqlite"
    WarehouseStore(path).accept(request())
    senders = [CallbackSender(path, BASE_URL, SECRET, client=Client()) for _ in range(8)]
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda sender: sender.work_once(KEY), senders))
    sent = [event for sender in senders for event in sender.client.sent]
    assert len(sent) == len({event["event_id"] for event in sent}) == 2
    assert sum(result["status"] == "DELIVERED" for result in results) == 2
    with sqlite3.connect(path) as connection:
        assert connection.execute(
            "SELECT DISTINCT delivery_status,lease_owner,lease_expires_at FROM callback_events"
        ).fetchall() == [("DELIVERED", None, None)]


def test_post_attempts_callbacks_after_commit_and_before_response_without_changing_body(
    tmp_path, monkeypatch
):
    path = tmp_path / "warehouse.sqlite"
    client = Client()
    monkeypatch.setattr(
        "aftercare.mock_warehouse.app.CallbackSender",
        lambda state, url, secret, **kwargs: CallbackSender(state, url, secret, client=client),
    )
    app = create_app(path, TOKEN, callback_base_url=BASE_URL, callback_secret=SECRET)
    with TestClient(app) as http:
        result = http.post(POST, json=request().model_dump(), headers=AUTH)
        assert result.status_code == 200
        assert len(client.sent) == 2
        assert set(result.json()) == {
            "effect_key",
            "application_id",
            "request_hash",
            "external_shipment_id",
            "status",
        }
        query = http.get(GET + KEY, headers=AUTH)
        assert query.json() == result.json()
        assert query.headers["X-Warehouse-Sequence"] == "2"
        assert http.post(POST, json=request().model_dump(), headers=AUTH).json() == result.json()
        assert len(client.sent) == 2
        assert SECRET not in result.text + query.text and TOKEN not in result.text + query.text


def test_callback_failure_cannot_undo_committed_shipment_or_events(tmp_path, monkeypatch):
    path = tmp_path / "warehouse.sqlite"
    monkeypatch.setattr(
        "aftercare.mock_warehouse.app.CallbackSender",
        lambda state, url, secret, **kwargs: CallbackSender(
            state, url, secret, client=Client("UNKNOWN")
        ),
    )
    app = create_app(path, TOKEN, callback_base_url=BASE_URL, callback_secret=SECRET)
    with TestClient(app) as client:
        result = client.post(POST, json=request().model_dump(), headers=AUTH)
        assert result.status_code == 200
        assert client.get(GET + KEY, headers=AUTH).json() == result.json()
    assert len(app.state.store.events(KEY)) == 2
    with sqlite3.connect(path) as connection:
        assert connection.execute(
            "SELECT delivery_status,attempts FROM callback_events ORDER BY sequence"
        ).fetchall() == [("PENDING", 1), ("PENDING", 1)]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"callback_base_url": BASE_URL},
        {"callback_secret": SECRET},
        {"callback_base_url": "http://example.com:8132", "callback_secret": SECRET},
        {"callback_base_url": BASE_URL, "callback_secret": TOKEN},
        {"callback_base_url": BASE_URL, "callback_secret": "short"},
    ],
)
def test_bad_callback_configuration_is_rejected_before_state_creation(tmp_path, kwargs):
    path = tmp_path / "warehouse.sqlite"
    with pytest.raises(ValueError) as error:
        create_app(path, TOKEN, **kwargs)
    assert SECRET not in str(error.value) and TOKEN not in str(error.value)
    assert not path.exists()


def test_query_fault_and_clear_do_not_change_success_or_invent_absence_proof(tmp_path):
    app = create_app(tmp_path / "warehouse.sqlite", TOKEN, test_mode=True)
    with TestClient(app) as client:
        success = client.post(POST, json=request().model_dump(), headers=AUTH)
        before = app.state.store.events(KEY)
        for mode in ("query_unavailable", "clear"):
            assert (
                client.post(
                    "/__test/faults", json={"effect_key": KEY, "mode": mode}, headers=AUTH
                ).status_code
                == 200
            )
            query = client.get(GET + KEY, headers=AUTH)
            if mode == "query_unavailable":
                assert query.status_code == 503 and "accepted" not in query.json()
                assert query.json()["code"] == "QUERY_UNAVAILABLE"
                assert (
                    client.post(POST, json=request().model_dump(), headers=AUTH).json()
                    == success.json()
                )
            else:
                assert query.status_code == 200 and query.json() == success.json()
                assert query.headers["X-Warehouse-Sequence"] == "2"
        assert app.state.store.events(KEY) == before


def test_delay_fault_runs_after_persist_once_and_has_no_effect_on_replay(tmp_path, monkeypatch):
    app = create_app(tmp_path / "warehouse.sqlite", TOKEN, test_mode=True)
    sleeps = []

    def observed_sleep(seconds):
        assert app.state.store.get(KEY) is not None
        assert len(app.state.store.events(KEY)) == 2
        sleeps.append(seconds)

    monkeypatch.setattr("aftercare.mock_warehouse.app.time.sleep", observed_sleep)
    with TestClient(app) as client:
        configured = client.post(
            "/__test/faults",
            json={"effect_key": KEY, "mode": "delay_after_accept_once", "delay_seconds": 0.2},
            headers=AUTH,
        )
        assert configured.status_code == 200
        result = client.post(POST, json=request().model_dump(), headers=AUTH)
        assert result.status_code == 200 and sleeps == [0.2]
        assert client.post(POST, json=request().model_dump(), headers=AUTH).json() == result.json()
        assert sleeps == [0.2]


@pytest.mark.parametrize("delay", [-0.1, 5.1, "1", True])
def test_delay_fault_is_strict_and_bounded(tmp_path, delay):
    with TestClient(create_app(tmp_path / "warehouse.sqlite", TOKEN, test_mode=True)) as client:
        response = client.post(
            "/__test/faults",
            json={"effect_key": KEY, "mode": "delay_after_accept_once", "delay_seconds": delay},
            headers=AUTH,
        )
        assert response.status_code == 422
        assert response.json()["code"] == "INVALID_INPUT"
