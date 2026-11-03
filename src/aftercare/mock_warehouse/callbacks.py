"""Durable at-least-once callback delivery, without holding SQLite during HTTP."""

import ipaddress
import math
import sqlite3
import uuid
from urllib.parse import urlsplit

from aftercare.domain.warehouse import validate_callback_secret
from aftercare.mock_warehouse.store import WarehouseStateError, WarehouseStore


def validate_callback_url(value):
    # Keep the independent provider free of product settings/database imports.
    try:
        parsed = urlsplit(value)
        valid = (
            isinstance(value, str)
            and parsed.scheme == "http"
            and ipaddress.ip_address(parsed.hostname or "").is_loopback
            and parsed.username is None
            and parsed.password is None
            and parsed.path == ""
            and not parsed.query
            and not parsed.fragment
            and "?" not in value
            and "#" not in value
            and parsed.port is not None
            and 1024 <= parsed.port <= 65535
            and not any(character.isspace() for character in value)
        )
    except (ValueError, TypeError, AttributeError):
        valid = False
    if not valid:
        raise ValueError("WAREHOUSE_CALLBACK_URL_MUST_BE_LITERAL_LOOPBACK_HTTP")
    return value


class CallbackSender:
    def __init__(
        self,
        state_path,
        callback_base_url,
        callback_secret,
        *,
        timeout=2.0,
        test_mode=False,
        client=None,
    ):
        validate_callback_url(callback_base_url)
        validate_callback_secret(callback_secret)
        if type(test_mode) is not bool:
            raise ValueError("WAREHOUSE_TEST_MODE_INVALID")
        if (
            type(timeout) not in {int, float}
            or not math.isfinite(timeout)
            or not 0.1 <= timeout <= 5.0
        ):
            raise ValueError("WAREHOUSE_TIMEOUT_INVALID")
        if client is None:
            # Import only when needed, so the SQLite store and unit-injected
            # transports have no dependency on the product HTTP implementation.
            from aftercare.services.warehouse_attempt import BoundedCallbackClient

            client = BoundedCallbackClient(callback_base_url, callback_secret, timeout=timeout)
        self.client = client
        self.store = WarehouseStore(state_path)
        # The transport has a total deadline plus <=1s process reaping. A crashed
        # sender's lease remains finite, while ordinary cleanup cannot outlive it.
        self.lease_seconds = max(5.0, timeout + 3.0)

    def work_once(self, effect_key=None):
        try:
            claim = self.store.claim_event(
                uuid.uuid4().hex, effect_key=effect_key, lease_seconds=self.lease_seconds
            )
        except (OSError, sqlite3.Error, ValueError, WarehouseStateError):
            return {"worked": False, "status": "STATE_ERROR"}
        if claim is None:
            return {"worked": False, "status": "IDLE"}
        acknowledged = False
        try:
            result = self.client.send_event(claim["event"])
            acknowledged = result.outcome == "ACKNOWLEDGED"
        except Exception:
            # Exception text can contain tokens, payloads or URLs. Persist only a
            # fixed result, and retain the event for bounded later attempts.
            pass
        try:
            status = self.store.finish_event(claim, acknowledged=acknowledged)
        except (OSError, sqlite3.Error, ValueError, WarehouseStateError):
            status = None
        return {
            "worked": True,
            "status": status or "UNCONFIRMED",
            "event_id": claim["event"]["event_id"],
            "sequence": claim["event"]["sequence"],
            "attempts": claim["attempts"],
            "outcome": "ACKNOWLEDGED" if acknowledged else "UNKNOWN",
        }
