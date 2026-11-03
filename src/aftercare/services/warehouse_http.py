"""One bounded local HTTP attempt; no redirects, proxy discovery or implicit retry."""

import http.client
import json
import re
import time
from dataclasses import dataclass
from urllib.parse import quote, urlsplit

from pydantic import ValidationError

from aftercare.config import validate_warehouse_url
from aftercare.domain.warehouse import (
    PROVIDER_ID,
    ShipmentResult,
    WarehouseEvent,
    shipment_request_hash,
    sign_event,
)

MAX_RESPONSE_BYTES = 65536


@dataclass(frozen=True)
class DeliveryResult:
    outcome: str
    external_shipment_id: str | None = None
    error_code: str | None = None
    provider_sequence: int | None = None


class WarehouseHTTPClient:
    def __init__(self, base_url, token, *, timeout=2.0):
        parsed = urlsplit(validate_warehouse_url(base_url))
        if not 32 <= len(token) <= 256 or any(not 33 <= ord(c) <= 126 for c in token):
            raise ValueError("WAREHOUSE_TOKEN_REQUIRED")
        if not 0.1 <= timeout <= 5.0:
            raise ValueError("WAREHOUSE_TIMEOUT_INVALID")
        self.host, self.port, self.token, self.timeout = (
            parsed.hostname,
            parsed.port,
            token,
            timeout,
        )

    def deliver(self, payload, request_hash):
        # Refuse corrupt local data before creating a socket. Never report it as success.
        try:
            if shipment_request_hash(**payload) != request_hash:
                return DeliveryResult("FAILED", error_code="DELIVERY_PAYLOAD_CONFLICT")
        except (TypeError, ValueError):
            return DeliveryResult("FAILED", error_code="DELIVERY_PAYLOAD_CONFLICT")
        connection = http.client.HTTPConnection(self.host, self.port, timeout=self.timeout)
        try:
            encoded = json.dumps(
                {**payload, "request_hash": request_hash},
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode()
            connection.request(
                "POST",
                "/mock-warehouse/shipments",
                body=encoded,
                headers={
                    "Content-Type": "application/json",
                    "Authorization": "Bearer " + self.token,
                },
            )
            response = connection.getresponse()
            raw = response.read(MAX_RESPONSE_BYTES + 1)
            if len(raw) > MAX_RESPONSE_BYTES:
                return DeliveryResult("UNKNOWN", error_code="WAREHOUSE_RESPONSE_TOO_LARGE")
            return self._decode(response.status, raw, payload, request_hash)
        except (OSError, http.client.HTTPException, ValueError, UnicodeError):
            # Even a reset can follow acceptance; error text may contain credentials.
            return DeliveryResult("UNKNOWN", error_code="WAREHOUSE_TRANSPORT_UNKNOWN")
        finally:
            connection.close()

    def query(self, effect_key, application_id, request_hash):
        if (
            not isinstance(effect_key, str)
            or not 0 < len(effect_key) <= 200
            or not isinstance(application_id, str)
            or not 0 < len(application_id) <= 100
            or not isinstance(request_hash, str)
            or re.fullmatch(r"[0-9a-f]{64}", request_hash) is None
        ):
            return DeliveryResult("UNKNOWN", error_code="WAREHOUSE_QUERY_BINDING_INVALID")
        connection = http.client.HTTPConnection(self.host, self.port, timeout=self.timeout)
        try:
            connection.request(
                "GET",
                "/mock-warehouse/shipments/by-key/" + quote(effect_key, safe=""),
                headers={"Authorization": "Bearer " + self.token},
            )
            response = connection.getresponse()
            raw = response.read(MAX_RESPONSE_BYTES + 1)
            if len(raw) > MAX_RESPONSE_BYTES:
                return DeliveryResult("UNKNOWN", error_code="WAREHOUSE_RESPONSE_TOO_LARGE")
            if response.status == 404:
                try:
                    data = json.loads(raw)
                except (ValueError, UnicodeError):
                    data = None
                if (
                    isinstance(data, dict)
                    and data.get("provider") == PROVIDER_ID
                    and data.get("code") == "NOT_FOUND"
                ):
                    # This is a query observation, never permission to re-POST or release stock.
                    return DeliveryResult("NOT_FOUND", error_code="WAREHOUSE_QUERY_NOT_FOUND")
            result = self._decode(
                response.status,
                raw,
                {"effect_key": effect_key, "application_id": application_id},
                request_hash,
            )
            if result.outcome != "SUCCEEDED":
                return DeliveryResult("UNKNOWN", error_code="WAREHOUSE_QUERY_UNAVAILABLE")
            sequence = response.getheader("X-Warehouse-Sequence", "")
            if re.fullmatch(r"[0-9]{1,9}", sequence) is None or int(sequence) < 2:
                return DeliveryResult("UNKNOWN", error_code="WAREHOUSE_QUERY_SEQUENCE_INVALID")
            return DeliveryResult(
                "SUCCEEDED",
                external_shipment_id=result.external_shipment_id,
                provider_sequence=int(sequence),
            )
        except (OSError, http.client.HTTPException, ValueError, UnicodeError):
            return DeliveryResult("UNKNOWN", error_code="WAREHOUSE_QUERY_TRANSPORT_UNKNOWN")
        finally:
            connection.close()

    @staticmethod
    def _decode(status, raw, payload, request_hash):
        try:
            data = json.loads(raw)
        except (ValueError, UnicodeError):
            return DeliveryResult("UNKNOWN", error_code="WAREHOUSE_RESPONSE_INVALID")
        if status == 200:
            try:
                result = ShipmentResult.model_validate(data)
            except ValidationError:
                return DeliveryResult("UNKNOWN", error_code="WAREHOUSE_RESPONSE_INVALID")
            if (
                result.effect_key != payload["effect_key"]
                or result.application_id != payload["application_id"]
                or result.request_hash != request_hash
            ):
                return DeliveryResult("UNKNOWN", error_code="WAREHOUSE_RESPONSE_MISMATCH")
            return DeliveryResult("SUCCEEDED", external_shipment_id=result.external_shipment_id)
        if isinstance(data, dict) and data.get("provider") == PROVIDER_ID:
            not_accepted = data.get("accepted") is False
            if (
                status == 503
                and data.get("code") == "TEMPORARY_UNAVAILABLE"
                and not_accepted
                and data.get("retryable") is True
            ):
                return DeliveryResult("RETRYABLE", error_code="WAREHOUSE_TEMPORARY_REJECTION")
            if (
                status in {400, 403, 409, 422}
                and data.get("code") == "PERMANENT_REJECTED"
                and not_accepted
                and data.get("retryable") is False
            ):
                return DeliveryResult("FAILED", error_code="WAREHOUSE_PERMANENT_REJECTION")
        return DeliveryResult("UNKNOWN", error_code="WAREHOUSE_HTTP_UNKNOWN")


class WarehouseCallbackHTTPClient(WarehouseHTTPClient):
    """A signed Inbox acknowledgement is distinct from a shipped business result."""

    def send_event(self, event):
        try:
            payload = WarehouseEvent.model_validate(event).model_dump()
        except ValidationError:
            return DeliveryResult("UNKNOWN", error_code="WAREHOUSE_EVENT_INVALID")
        raw = json.dumps(
            payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode()
        timestamp = str(int(time.time()))
        connection = http.client.HTTPConnection(self.host, self.port, timeout=self.timeout)
        try:
            connection.request(
                "POST",
                "/api/integrations/warehouse/events",
                body=raw,
                headers={
                    "Content-Type": "application/json",
                    "X-Warehouse-Source": PROVIDER_ID,
                    "X-Warehouse-Timestamp": timestamp,
                    "X-Warehouse-Signature": sign_event(self.token, raw, timestamp),
                },
            )
            response = connection.getresponse()
            encoded = response.read(MAX_RESPONSE_BYTES + 1)
            if len(encoded) > MAX_RESPONSE_BYTES:
                return DeliveryResult("UNKNOWN", error_code="WAREHOUSE_CALLBACK_RESPONSE_INVALID")
            data = json.loads(encoded)
            if (
                response.status in {200, 202}
                and isinstance(data, dict)
                and data.get("event_id") == payload["event_id"]
                and isinstance(data.get("inbox_id"), str)
                and bool(data["inbox_id"])
                and data.get("status") in {"PROCESSED", "STALE", "DUPLICATE", "PENDING", "RECEIVED"}
            ):
                return DeliveryResult("ACKNOWLEDGED")
            return DeliveryResult("UNKNOWN", error_code="WAREHOUSE_CALLBACK_NOT_ACKNOWLEDGED")
        except (OSError, http.client.HTTPException, ValueError, UnicodeError):
            return DeliveryResult("UNKNOWN", error_code="WAREHOUSE_CALLBACK_TRANSPORT_UNKNOWN")
        finally:
            connection.close()
