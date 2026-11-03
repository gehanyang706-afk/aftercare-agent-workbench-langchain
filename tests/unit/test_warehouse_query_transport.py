"""Read-only observations and Inbox acknowledgements cannot become shipment success."""

import json
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from aftercare.config import Settings
from aftercare.domain.warehouse import PROVIDER_ID, shipment_request_hash
from aftercare.services import warehouse_http
from aftercare.services.warehouse_http import WarehouseCallbackHTTPClient, WarehouseHTTPClient

TOKEN = "C2-query-test-token-" + "k" * 32
SECRET = "C2-callback-test-secret-" + "q" * 32
PAYLOAD = {
    "effect_key": "exchange:query-1",
    "application_id": "query-1",
    "variant_id": "white-M",
    "quantity": 1,
}
HASH = shipment_request_hash(**PAYLOAD)
RESULT = {
    "effect_key": PAYLOAD["effect_key"],
    "application_id": PAYLOAD["application_id"],
    "request_hash": HASH,
    "external_shipment_id": "MOCK-query-1",
    "status": "SUCCEEDED",
}
EVENT = {
    "event_id": "event-query-1",
    "effect_key": PAYLOAD["effect_key"],
    "application_id": PAYLOAD["application_id"],
    "request_hash": HASH,
    "external_shipment_id": "MOCK-query-1",
    "sequence": 2,
    "status": "SUCCEEDED",
}


def connection(monkeypatch, status, body, sequence="2"):
    seen = []

    class Connection:
        def __init__(self, *args, **kwargs):
            pass

        def request(self, method, path, *, headers, body=None):
            seen.append((method, path, headers, body))

        def getresponse(self):
            return SimpleNamespace(
                status=status,
                read=lambda limit: json.dumps(body).encode(),
                getheader=lambda name, default="": sequence,
            )

        def close(self):
            pass

    monkeypatch.setattr(warehouse_http.http.client, "HTTPConnection", Connection)
    return seen


@pytest.mark.parametrize(
    "status,body,sequence,outcome",
    [
        (200, RESULT, "2", "SUCCEEDED"),
        (200, RESULT, "", "UNKNOWN"),
        (200, RESULT, "1", "UNKNOWN"),
        (200, {**RESULT, "request_hash": "a" * 64}, "2", "UNKNOWN"),
        (200, {**RESULT, "application_id": "other"}, "2", "UNKNOWN"),
        (404, {"provider": PROVIDER_ID, "code": "NOT_FOUND", "accepted": False}, "", "NOT_FOUND"),
        (404, {"code": "NOT_FOUND"}, "", "UNKNOWN"),
        (503, {"provider": PROVIDER_ID, "code": "QUERY_UNAVAILABLE"}, "", "UNKNOWN"),
    ],
)
def test_query_never_turns_not_found_into_a_post_or_released_reservation(
    monkeypatch, status, body, sequence, outcome
):
    seen = connection(monkeypatch, status, body, sequence)
    result = WarehouseHTTPClient("http://127.0.0.1:8140", TOKEN).query(
        PAYLOAD["effect_key"], PAYLOAD["application_id"], HASH
    )
    assert result.outcome == outcome
    assert result.provider_sequence == (2 if outcome == "SUCCEEDED" else None)
    assert len(seen) == 1 and seen[0][0] == "GET"
    assert seen[0][1] == "/mock-warehouse/shipments/by-key/exchange%3Aquery-1"
    assert seen[0][3] is None


@pytest.mark.parametrize(
    "status,body,outcome",
    [
        (
            200,
            {"status": "PROCESSED", "inbox_id": "inbox-1", "event_id": EVENT["event_id"]},
            "ACKNOWLEDGED",
        ),
        (
            202,
            {"status": "PENDING", "inbox_id": "inbox-1", "event_id": EVENT["event_id"]},
            "ACKNOWLEDGED",
        ),
        (409, {"status": "CONFLICT", "inbox_id": "inbox-1"}, "UNKNOWN"),
        (200, {"status": "SUCCEEDED", "external_shipment_id": "MOCK-query-1"}, "UNKNOWN"),
    ],
)
def test_callback_acknowledgement_only_means_durable_inbox_receipt(
    monkeypatch, status, body, outcome
):
    seen = connection(monkeypatch, status, body)
    result = WarehouseCallbackHTTPClient("http://127.0.0.1:8134", SECRET).send_event(EVENT)
    assert result.outcome == outcome
    assert result.external_shipment_id is None and result.provider_sequence is None
    assert len(seen) == 1 and seen[0][0] == "POST"
    assert seen[0][2]["X-Warehouse-Source"] == PROVIDER_ID
    assert SECRET not in str(seen[0][2])


def test_callbacks_require_independent_valid_credentials_and_hide_them():
    with pytest.raises(ValidationError, match="WAREHOUSE_CALLBACK_SECRET_REQUIRED"):
        Settings(_env_file=None, warehouse_callbacks_enabled=True)
    with pytest.raises(ValidationError, match="WAREHOUSE_CALLBACK_SECRET_MUST_DIFFER"):
        Settings(
            _env_file=None,
            warehouse_delivery_enabled=True,
            warehouse_token=TOKEN,
            warehouse_callbacks_enabled=True,
            warehouse_callback_secret=TOKEN,
        )
    settings = Settings(
        _env_file=None,
        warehouse_delivery_enabled=True,
        warehouse_token=TOKEN,
        warehouse_callbacks_enabled=True,
        warehouse_callback_secret=SECRET,
    )
    assert settings.public_summary()["warehouse_callbacks_enabled"] is True
    assert SECRET not in repr(settings) and SECRET not in str(settings.public_summary())
