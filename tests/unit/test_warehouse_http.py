"""Untrusted provider responses cannot manufacture a warehouse success or safe retry."""

import json
from types import SimpleNamespace

import pytest

from aftercare.domain.contracts import BusinessError
from aftercare.domain.warehouse import PROVIDER_ID, shipment_request_hash
from aftercare.services import warehouse_http
from aftercare.services.warehouse_http import WarehouseHTTPClient
from aftercare.workers.outbox_worker import OutboxWorker

TOKEN = "local-test-token-" + "a" * 32
PAYLOAD = {
    "effect_key": "exchange:app-client",
    "application_id": "app-client",
    "variant_id": "tee-black-M",
    "quantity": 1,
}
HASH = shipment_request_hash(**PAYLOAD)
SUCCESS = {
    "effect_key": PAYLOAD["effect_key"],
    "application_id": PAYLOAD["application_id"],
    "request_hash": HASH,
    "external_shipment_id": "MOCK-client-001",
    "status": "SUCCEEDED",
}


@pytest.mark.parametrize(
    ("status", "body", "outcome"),
    [
        (200, SUCCESS, "SUCCEEDED"),
        (200, {k: v for k, v in SUCCESS.items() if k != "status"}, "UNKNOWN"),
        (200, {**SUCCESS, "request_hash": "f" * 64}, "UNKNOWN"),
        (200, {**SUCCESS, "effect_key": "exchange:other"}, "UNKNOWN"),
        (200, {**SUCCESS, "application_id": "other"}, "UNKNOWN"),
        (200, {**SUCCESS, "status": "PENDING"}, "UNKNOWN"),
        (200, {**SUCCESS, "unexpected": "field"}, "UNKNOWN"),
        (500, SUCCESS, "UNKNOWN"),
        (302, SUCCESS, "UNKNOWN"),
        (404, {"code": "NOT_FOUND", "accepted": False}, "UNKNOWN"),
        (
            503,
            {
                "provider": PROVIDER_ID,
                "code": "TEMPORARY_UNAVAILABLE",
                "accepted": False,
                "retryable": True,
            },
            "RETRYABLE",
        ),
        (
            503,
            {"code": "TEMPORARY_UNAVAILABLE", "accepted": False, "retryable": True},
            "UNKNOWN",
        ),
        (
            503,
            {
                "provider": PROVIDER_ID,
                "code": "TEMPORARY_UNAVAILABLE",
                "accepted": 0,
                "retryable": True,
            },
            "UNKNOWN",
        ),
        (
            422,
            {
                "provider": PROVIDER_ID,
                "code": "PERMANENT_REJECTED",
                "accepted": False,
                "retryable": False,
            },
            "FAILED",
        ),
    ],
)
def test_provider_result_requires_exact_binding_and_definitive_rejection(status, body, outcome):
    result = WarehouseHTTPClient._decode(status, json.dumps(body).encode(), PAYLOAD, HASH)
    assert result.outcome == outcome
    assert result.external_shipment_id == (
        SUCCESS["external_shipment_id"] if outcome == "SUCCEEDED" else None
    )


def test_invalid_json_is_unknown():
    result = WarehouseHTTPClient._decode(200, b"<html>proxy error</html>", PAYLOAD, HASH)
    assert result.outcome == "UNKNOWN"


def test_transport_is_single_attempt_bounded_and_secret_safe(monkeypatch):
    sent = []

    class Connection:
        def __init__(self, host, port, timeout):
            assert (host, port, timeout) == ("127.0.0.1", 8140, 2.0)

        def request(self, method, path, *, body, headers):
            sent.append((method, path, json.loads(body)))
            assert headers["Authorization"] == "Bearer " + TOKEN
            raise OSError("peer accepted but response lost: " + TOKEN)

        def close(self):
            pass

    monkeypatch.setattr(warehouse_http.http.client, "HTTPConnection", Connection)
    result = WarehouseHTTPClient("http://127.0.0.1:8140", TOKEN).deliver(PAYLOAD, HASH)
    assert result.outcome == "UNKNOWN" and TOKEN not in repr(result)
    assert sent == [("POST", "/mock-warehouse/shipments", {**PAYLOAD, "request_hash": HASH})]


def test_local_payload_corruption_does_not_open_connection(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("Corrupt local payload must never reach provider")

    monkeypatch.setattr(warehouse_http.http.client, "HTTPConnection", forbidden)
    result = WarehouseHTTPClient("http://127.0.0.1:8140", TOKEN).deliver(PAYLOAD, "bad")
    assert result.outcome == "FAILED"


def test_large_provider_reply_is_bounded_unknown(monkeypatch):
    reads = []

    class Connection:
        def __init__(self, *args, **kwargs):
            pass

        def request(self, *args, **kwargs):
            pass

        def getresponse(self):
            def read(limit):
                reads.append(limit)
                return b"x" * limit

            return SimpleNamespace(status=200, read=read)

        def close(self):
            pass

    monkeypatch.setattr(warehouse_http.http.client, "HTTPConnection", Connection)
    result = WarehouseHTTPClient("http://127.0.0.1:8140", TOKEN).deliver(PAYLOAD, HASH)
    assert result.outcome == "UNKNOWN"
    assert reads == [warehouse_http.MAX_RESPONSE_BYTES + 1]


def test_expired_worker_is_refused_before_http():
    claim = SimpleNamespace(outbox_id="outbox-stale")

    class Service:
        def expire_leases(self):
            return 0

        def claim(self, owner):
            return claim

        def begin_delivery(self, value):
            raise BusinessError("OUTBOX_LEASE_LOST")

        def finish(self, *args, **kwargs):
            pytest.fail("Stale worker cannot finish")

    class Client:
        def deliver(self, *args):
            pytest.fail("Known stale worker cannot start HTTP")

    result = OutboxWorker(Service(), Client()).run_once()
    assert result == {
        "status": "LEASE_LOST",
        "outbox_id": claim.outbox_id,
        "error_code": "OUTBOX_LEASE_LOST",
    }
