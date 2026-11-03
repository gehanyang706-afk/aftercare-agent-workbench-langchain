"""A single HTTP attempt in a disposable child with an elapsed-time budget.

The monotonic parent budget includes process startup and pipe communication. At
expiry the child is killed and reaped with up to one additional second of cleanup.
OS process creation/scheduling is not a hard real-time guarantee. A POSIX child
alarm also bounds header/body drip responses if its parent disappears.
"""

from __future__ import annotations

import json
import re
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from aftercare.services.warehouse_http import DeliveryResult

MAX_INPUT_BYTES = 8192
MAX_OUTPUT_BYTES = 4096
OUTCOMES = {"SUCCEEDED", "RETRYABLE", "FAILED", "UNKNOWN", "NOT_FOUND", "ACKNOWLEDGED"}
ERROR_PATTERN = re.compile(r"[A-Z0-9_]{1,100}\Z")


def _unknown(code) -> DeliveryResult:
    from aftercare.services.warehouse_http import DeliveryResult

    return DeliveryResult("UNKNOWN", error_code=code)


def _safe_result(data):
    if not isinstance(data, dict) or set(data) != {
        "outcome",
        "external_shipment_id",
        "error_code",
        "provider_sequence",
    }:
        return None
    outcome = data["outcome"]
    external_id = data["external_shipment_id"]
    error_code = data["error_code"]
    if not isinstance(outcome, str) or outcome not in OUTCOMES:
        return None
    if error_code is not None and (
        not isinstance(error_code, str) or ERROR_PATTERN.fullmatch(error_code) is None
    ):
        return None
    if outcome == "SUCCEEDED":
        if not isinstance(external_id, str) or not 0 < len(external_id.strip()) <= 100:
            return None
        if error_code is not None:
            return None
    elif external_id is not None:
        return None
    sequence = data["provider_sequence"]
    if sequence is not None and (
        outcome != "SUCCEEDED" or type(sequence) is not int or not 2 <= sequence <= 2147483647
    ):
        return None
    return data


def _decode_child(raw) -> DeliveryResult:
    from aftercare.services.warehouse_http import DeliveryResult

    if len(raw) > MAX_OUTPUT_BYTES:
        return _unknown("WAREHOUSE_CHILD_RESPONSE_INVALID")
    try:
        data = _safe_result(json.loads(raw))
    except (ValueError, UnicodeError):
        data = None
    if data is None:
        return _unknown("WAREHOUSE_CHILD_RESPONSE_INVALID")
    return DeliveryResult(**data)


def _child_environment():
    # Fixed source path, not an inherited user PYTHONPATH or provider/model credentials.
    return {
        "PYTHONPATH": str(Path(__file__).resolve().parents[2]),
        "PYTHONIOENCODING": "utf-8",
        "PYTHONUTF8": "1",
        "PYTHONNOUSERSITE": "1",
        "PYTHONUNBUFFERED": "1",
    }


def _reap(process):
    if process is None:
        return True
    try:
        if process.poll() is None:
            try:
                process.kill()
            except OSError:
                pass
            try:
                process.wait(timeout=1.0)
            except subprocess.TimeoutExpired:
                pass
        return process.poll() is not None
    finally:
        for stream in (process.stdin, process.stdout, process.stderr):
            if stream is not None:
                try:
                    stream.close()
                except (OSError, ValueError):
                    pass


class BoundedWarehouseClient:
    def __init__(self, base_url, token, *, timeout=2.0):
        from aftercare.services.warehouse_http import WarehouseHTTPClient

        validated = WarehouseHTTPClient(base_url, token, timeout=timeout)
        self.base_url, self.token, self.timeout = base_url, token, float(validated.timeout)

    def deliver(self, payload, request_hash) -> DeliveryResult:
        from aftercare.domain.warehouse import shipment_request_hash
        from aftercare.services.warehouse_http import DeliveryResult

        try:
            if shipment_request_hash(**payload) != request_hash:
                return DeliveryResult("FAILED", error_code="DELIVERY_PAYLOAD_CONFLICT")
        except (TypeError, ValueError, UnicodeError):
            return DeliveryResult("FAILED", error_code="DELIVERY_PAYLOAD_CONFLICT")
        return self._run("deliver", payload, request_hash)

    def query(self, effect_key, application_id, request_hash) -> DeliveryResult:
        result = self._run(
            "query", {"effect_key": effect_key, "application_id": application_id}, request_hash
        )
        if result.outcome == "SUCCEEDED" and result.provider_sequence is None:
            return _unknown("WAREHOUSE_QUERY_SEQUENCE_INVALID")
        return result

    def _run(self, operation, payload, request_hash=None) -> DeliveryResult:

        try:
            encoded = json.dumps(
                {
                    "operation": operation,
                    "base_url": self.base_url,
                    "token": self.token,
                    "timeout": self.timeout,
                    "payload": payload,
                    "request_hash": request_hash,
                },
                ensure_ascii=False,
                separators=(",", ":"),
            ).encode()
        except (TypeError, ValueError, UnicodeError):
            return _unknown("WAREHOUSE_INPUT_INVALID")
        if len(encoded) > MAX_INPUT_BYTES:
            return _unknown("WAREHOUSE_INPUT_INVALID")
        deadline = time.monotonic() + self.timeout
        process = None
        result = _unknown("WAREHOUSE_CHILD_FAILED")
        try:
            process = subprocess.Popen(
                [sys.executable, "-m", "aftercare.services.warehouse_attempt"],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                close_fds=True,
                env=_child_environment(),
                cwd=Path(__file__).resolve().parents[3],
            )
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                result = _unknown("WAREHOUSE_DEADLINE_EXCEEDED")
            else:
                raw, _ = process.communicate(input=encoded, timeout=remaining)
                if time.monotonic() >= deadline:
                    result = _unknown("WAREHOUSE_DEADLINE_EXCEEDED")
                elif process.returncode == 0:
                    result = _decode_child(raw)
        except subprocess.TimeoutExpired:
            result = _unknown("WAREHOUSE_DEADLINE_EXCEEDED")
        except (OSError, ValueError):
            result = _unknown("WAREHOUSE_CHILD_FAILED")
        finally:
            if not _reap(process):
                result = _unknown("WAREHOUSE_CHILD_CLEANUP_FAILED")
        allowed = {
            "deliver": {"SUCCEEDED", "RETRYABLE", "FAILED", "UNKNOWN"},
            "query": {"SUCCEEDED", "NOT_FOUND", "UNKNOWN", "RETRYABLE", "FAILED"},
            "callback": {"ACKNOWLEDGED", "UNKNOWN"},
        }
        if result.outcome not in allowed.get(operation, set()):
            return _unknown("WAREHOUSE_CHILD_RESPONSE_INVALID")
        return result


class BoundedCallbackClient(BoundedWarehouseClient):
    def send_event(self, event) -> DeliveryResult:
        return self._run("callback", event)


class _ChildDeadline(Exception):
    pass


def _alarm(signum, frame):
    raise _ChildDeadline()


def _child_main():
    # Keep module imports above stdlib-only so this alarm covers expensive imports too.
    result = {
        "outcome": "UNKNOWN",
        "external_shipment_id": None,
        "error_code": "WAREHOUSE_CHILD_FAILED",
        "provider_sequence": None,
    }
    armed = False
    try:
        if not hasattr(signal, "setitimer"):
            result["error_code"] = "WAREHOUSE_CHILD_TIMER_UNAVAILABLE"
        else:
            signal.signal(signal.SIGALRM, _alarm)
            signal.setitimer(signal.ITIMER_REAL, 7.0)
            armed = True
            raw = sys.stdin.buffer.read(MAX_INPUT_BYTES + 1)
            if len(raw) > MAX_INPUT_BYTES:
                raise ValueError("INPUT_TOO_LARGE")
            data = json.loads(raw)
            if not isinstance(data, dict) or set(data) != {
                "operation",
                "base_url",
                "token",
                "timeout",
                "payload",
                "request_hash",
            }:
                raise ValueError("INPUT_INVALID")
            from aftercare.offline import install_offline_guard
            from aftercare.services.warehouse_http import (
                WarehouseCallbackHTTPClient,
                WarehouseHTTPClient,
            )

            install_offline_guard()
            operation = data["operation"]
            client_type = (
                WarehouseCallbackHTTPClient if operation == "callback" else WarehouseHTTPClient
            )
            client = client_type(data["base_url"], data["token"], timeout=data["timeout"])
            signal.setitimer(signal.ITIMER_REAL, client.timeout)
            if operation == "deliver":
                delivery = client.deliver(data["payload"], data["request_hash"])
            elif operation == "query":
                delivery = client.query(**data["payload"], request_hash=data["request_hash"])
            elif operation == "callback":
                delivery = client.send_event(data["payload"])
            else:
                raise ValueError("OPERATION_INVALID")
            candidate = {
                "outcome": delivery.outcome,
                "external_shipment_id": delivery.external_shipment_id,
                "error_code": delivery.error_code,
                "provider_sequence": delivery.provider_sequence,
            }
            if _safe_result(candidate) is not None:
                result = candidate
    except _ChildDeadline:
        result = {
            "outcome": "UNKNOWN",
            "external_shipment_id": None,
            "error_code": "WAREHOUSE_DEADLINE_EXCEEDED",
            "provider_sequence": None,
        }
    except Exception:
        # Never emit exception text, the input envelope, provider token or environment.
        pass
    finally:
        if armed:
            signal.setitimer(signal.ITIMER_REAL, 0)
    sys.stdout.write(json.dumps(result, separators=(",", ":")) + "\n")
    sys.stdout.flush()


if __name__ == "__main__":
    _child_main()
