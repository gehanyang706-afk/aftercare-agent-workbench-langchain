"""Callback receipt binding and SQLite lock-wait deadlines; no product database."""

import json
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from aftercare.domain.warehouse import ShipmentInput, shipment_request_hash
from aftercare.mock_warehouse.store import WarehouseStore
from aftercare.services import warehouse_http
from aftercare.services.warehouse_http import WarehouseCallbackHTTPClient

SECRET = "local-callback-unit-secret-" + "x" * 32
KEY = "exchange:reliability-1"
PAYLOAD = {
    "effect_key": KEY,
    "application_id": "reliability-1",
    "variant_id": "white-L",
    "quantity": 1,
}
HASH = shipment_request_hash(**PAYLOAD)
EVENT = {
    "event_id": "event-reliability-1",
    "effect_key": KEY,
    "application_id": PAYLOAD["application_id"],
    "request_hash": HASH,
    "external_shipment_id": "MOCK-reliability-1",
    "sequence": 2,
    "status": "SUCCEEDED",
}


def callback_reply(monkeypatch, status, body):
    class Connection:
        def __init__(self, *args, **kwargs):
            pass

        def request(self, method, path, *, headers, body):
            assert method == "POST"
            assert path == "/api/integrations/warehouse/events"
            assert json.loads(body)["event_id"] == EVENT["event_id"]

        def getresponse(self):
            return SimpleNamespace(status=status, read=lambda limit: json.dumps(body).encode())

        def close(self):
            pass

    monkeypatch.setattr(warehouse_http.http.client, "HTTPConnection", Connection)
    return WarehouseCallbackHTTPClient("http://127.0.0.1:8134", SECRET).send_event(EVENT)


@pytest.mark.parametrize("status", [200, 202])
@pytest.mark.parametrize("event_id", [None, "another-event"])
def test_ack_cannot_confirm_a_missing_or_different_event(monkeypatch, status, event_id):
    response = {"inbox_id": "inbox-1", "status": "PROCESSED" if status == 200 else "PENDING"}
    if event_id is not None:
        response["event_id"] = event_id
    result = callback_reply(monkeypatch, status, response)
    assert result.outcome == "UNKNOWN"
    assert result.error_code == "WAREHOUSE_CALLBACK_NOT_ACKNOWLEDGED"
    assert result.external_shipment_id is None


@pytest.mark.parametrize("status,inbox_status", [(200, "PROCESSED"), (202, "PENDING")])
def test_matching_ack_confirms_only_the_sent_event(monkeypatch, status, inbox_status):
    result = callback_reply(
        monkeypatch,
        status,
        {
            "event_id": EVENT["event_id"],
            "inbox_id": "inbox-1",
            "status": inbox_status,
        },
    )
    assert result.outcome == "ACKNOWLEDGED"
    assert result.external_shipment_id is None and result.provider_sequence is None


@pytest.mark.parametrize(
    "status,body",
    [
        (200, {"status": "PROCESSED"}),
        (202, {"status": "PENDING", "inbox_id": ""}),
        (200, {"status": "SUCCEEDED", "inbox_id": "inbox-1"}),
        (409, {"status": "PENDING", "inbox_id": "inbox-1"}),
    ],
)
def test_event_binding_does_not_replace_the_original_ack_rules(monkeypatch, status, body):
    result = callback_reply(monkeypatch, status, {**body, "event_id": EVENT["event_id"]})
    assert result.outcome == "UNKNOWN"


def new_store(tmp_path):
    store = WarehouseStore(tmp_path / "warehouse.sqlite")
    store.accept(ShipmentInput(**PAYLOAD, request_hash=HASH))
    return store


def observe_begin(monkeypatch, store):
    """Observe the real blocked SQLite statement without replacing transactions."""
    entered = threading.Event()
    original = store._connection

    @contextmanager
    def connection(**kwargs):
        with original(**kwargs) as value:
            value.set_trace_callback(
                lambda sql: entered.set() if sql == "BEGIN IMMEDIATE" else None
            )
            yield value

    monkeypatch.setattr(store, "_connection", connection)
    return entered


def test_finish_cannot_accept_a_lease_that_expired_while_waiting_for_sqlite_lock(
    tmp_path, monkeypatch
):
    store = new_store(tmp_path)
    claim = store.claim_event("old-sender", effect_key=KEY, lease_seconds=0.8)
    with sqlite3.connect(store.path) as inspect:
        expires = inspect.execute(
            "SELECT lease_expires_at FROM callback_events WHERE event_id=?",
            (claim["event"]["event_id"],),
        ).fetchone()[0]
    entered = observe_begin(monkeypatch, store)
    with sqlite3.connect(store.path, isolation_level=None) as blocker:
        blocker.execute("BEGIN IMMEDIATE")
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(store.finish_event, claim, acknowledged=True)
            try:
                assert entered.wait(timeout=2), "finish did not attempt its real write lock"
                assert time.time() < expires, "test must reach the lock before lease expiry"
                time.sleep(max(0, expires - time.time()) + 0.08)
            finally:
                blocker.execute("COMMIT")
            assert future.result(timeout=3) is None
    with sqlite3.connect(store.path) as inspect:
        row = inspect.execute(
            "SELECT delivery_status,delivered_at FROM callback_events WHERE event_id=?",
            (claim["event"]["event_id"],),
        ).fetchone()
    assert row == ("CLAIMED", None)


def test_claim_lease_starts_after_the_real_sqlite_write_lock_is_acquired(tmp_path, monkeypatch):
    store = new_store(tmp_path)
    entered = observe_begin(monkeypatch, store)
    lease = 0.3
    with sqlite3.connect(store.path, isolation_level=None) as blocker:
        blocker.execute("BEGIN IMMEDIATE")
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(
                store.claim_event, "new-sender", effect_key=KEY, lease_seconds=lease
            )
            try:
                assert entered.wait(timeout=2), "claim did not attempt its real write lock"
                time.sleep(lease + 0.15)
            finally:
                # Capture before COMMIT: the writer may wake immediately, and
                # the releasing thread itself can be descheduled afterwards.
                released_at = time.time()
                blocker.execute("COMMIT")
            claim = future.result(timeout=3)
    with sqlite3.connect(store.path) as inspect:
        updated, expires = inspect.execute(
            "SELECT updated_at,lease_expires_at FROM callback_events WHERE event_id=?",
            (claim["event"]["event_id"],),
        ).fetchone()
    assert updated >= released_at - 0.03
    assert expires >= released_at + lease - 0.03
    assert expires - updated == pytest.approx(lease)


def test_explicit_now_remains_a_fixed_injected_logical_time(tmp_path):
    store = new_store(tmp_path)
    logical_now = time.time() + 100
    claim = store.claim_event("fixed-clock", effect_key=KEY, lease_seconds=1, now=logical_now)
    with sqlite3.connect(store.path) as inspect:
        updated, expires = inspect.execute(
            "SELECT updated_at,lease_expires_at FROM callback_events WHERE event_id=?",
            (claim["event"]["event_id"],),
        ).fetchone()
    assert updated == logical_now and expires == logical_now + 1
    assert store.finish_event(claim, acknowledged=True, now=logical_now + 0.5) == "DELIVERED"
