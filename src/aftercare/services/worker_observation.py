"""Optional worker observations; database fences remain the only execution authority.

The built-in sink offers bounded records to a daemon writer without waiting for
stderr. Full queues and failed writes lose observations, never retry business work.
Callers owning a sink should close it with a bounded timeout when stopping. A
SIGKILL or blocked output can lose its tail; these logs are not an audit ledger.
Custom collectors are intended for nonblocking tests and must honor that contract.
"""

import json
import logging
import math
import os
import queue
import re
import sys
import threading
import time
from datetime import UTC, datetime
from uuid import uuid4

from aftercare.domain.contracts import BusinessError

_ID = re.compile(r"[A-Za-z0-9_.:-]{1,160}\Z", re.ASCII)
_CODE = re.compile(r"[A-Z][A-Z0-9_]{0,99}\Z", re.ASCII)
_IDS = {
    "run_id",
    "owner",
    "application_id",
    "effect_key",
    "shipment_request_id",
    "outbox_id",
    "task_id",
}
_COUNTS = {"generation", "attempt", "cycle"}
_STAGES = {"cycle", "maintenance", "claim", "permission", "transport", "finish"}
_PHASES = {"started", "finished", "failed"}
_OUTCOMES = {
    "IDLE",
    "CLAIMED",
    "LEASE_LOST",
    "DELIVERY_ALREADY_STARTED",
    "PENDING",
    "SUCCEEDED",
    "RETRYABLE",
    "FAILED",
    "UNKNOWN",
    "NOT_FOUND",
    "MANUAL_REVIEW",
}


def _safe_record(values, sensitive_values=()):
    """Copy only finite, bounded scalar fields; never serialize arbitrary objects."""
    result = {}
    for key, value in values.items():
        if isinstance(value, str) and any(secret in value for secret in sensitive_values):
            continue
        if key in _IDS:
            if type(value) is str and _ID.fullmatch(value):
                result[key] = value
        elif key in _COUNTS:
            if type(value) is int and 0 < value <= 2**63 - 1:
                result[key] = value
        elif key == "schema" and type(value) is str and value == "worker-observation-v1":
            result[key] = value
        elif key == "worker_kind" and type(value) is str and value in {"outbox", "reconciliation"}:
            result[key] = value
        elif key == "stage" and type(value) is str and value in _STAGES:
            result[key] = value
        elif key == "phase" and type(value) is str and value in _PHASES:
            result[key] = value
        elif key == "outcome" and type(value) is str and value in _OUTCOMES:
            result[key] = value
        elif key == "error_code" and type(value) is str and _CODE.fullmatch(value):
            result[key] = value
        elif key == "duration_ms" and type(value) in {int, float}:
            if 0 <= value <= 10**15 and math.isfinite(value):
                result[key] = round(value, 3)
        elif key == "ts" and type(value) is str and len(value) <= 40:
            # Only this module's generated UTC timestamp is accepted by its sink.
            if re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}\+00:00", value):
                result[key] = value
    return result


class _DisabledStage:
    def __enter__(self):
        return self

    def result(self, **fields):
        pass

    def receipt(self, value):
        pass

    def __exit__(self, exc_type, exc_value, traceback):
        return False


_DISABLED_STAGE = _DisabledStage()


class _Stage:
    def __init__(self, observation, stage):
        self.observation, self.stage = observation, stage
        self.fields = {}

    def __enter__(self):
        self.started = time.monotonic()
        self.observation._emit(self.stage, "started")
        return self

    def result(self, *, outcome=None, error_code=None):
        self.fields.update(_safe_record({"outcome": outcome, "error_code": error_code}))

    def receipt(self, value):
        if type(value) is dict:
            request = value.get("request")
            outcome = request.get("status") if type(request) is dict else value.get("status")
            self.result(outcome=outcome, error_code=value.get("error_code"))

    def __exit__(self, exc_type, exc_value, traceback):
        fields = {**self.fields, "duration_ms": (time.monotonic() - self.started) * 1000}
        phase = "finished"
        if exc_type is not None:
            phase = "failed"
            code = exc_value.code if isinstance(exc_value, BusinessError) else None
            fields["error_code"] = _safe_record({"error_code": code}).get(
                "error_code", "WORKER_OPERATION_FAILED"
            )
        self.observation._emit(self.stage, phase, **fields)
        return False


class WorkerObservation:
    """A per-cycle observer with an explicit field allowlist and no database work."""

    def __init__(self, worker_kind, owner, sink=None):
        self.sink = sink
        self.context = {}
        if sink is not None:
            self.context = _safe_record(
                {
                    "schema": "worker-observation-v1",
                    "worker_kind": worker_kind,
                    "owner": owner,
                    "run_id": uuid4().hex,
                }
            )
        self.worker_kind = worker_kind

    def bind_claim(self, claim):
        if self.sink is None:
            return
        try:
            fields = {
                "shipment_request_id": claim.request_id,
                "outbox_id": claim.outbox_id,
                "generation": claim.generation,
            }
            if self.worker_kind == "outbox":
                fields.update(
                    attempt=claim.attempts,
                    application_id=claim.payload.get("application_id"),
                    effect_key=claim.payload.get("effect_key"),
                )
            else:
                fields.update(
                    task_id=claim.task_id,
                    cycle=claim.cycle,
                    application_id=claim.application_id,
                    effect_key=claim.effect_key,
                )
            self.context.update(_safe_record(fields))
        except Exception:
            # Correlation failure cannot change a claim or its execution rights.
            pass

    def stage(self, name):
        return _DISABLED_STAGE if self.sink is None else _Stage(self, name)

    def _emit(self, stage, phase, **fields):
        try:
            self.sink(
                _safe_record(
                    {
                        **self.context,
                        "ts": datetime.now(UTC).isoformat(timespec="milliseconds"),
                        "stage": stage,
                        "phase": phase,
                        **fields,
                    }
                )
            )
        except Exception:
            # Do not expose logging errors, exception messages or provider bodies.
            pass


class _QueueHandler(logging.Handler):
    def __init__(self, sink):
        super().__init__()
        self.sink = sink

    def emit(self, record):
        self.sink._offer(record.worker_event)


class _JsonWorkerSink:
    def __init__(self, stream, max_queue, sensitive_values):
        self._queue = queue.Queue(maxsize=max_queue)
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._closed = self._write_failed = False
        self._dropped_full = self._dropped_write_error = self._dropped_closed = 0
        self._sensitive_values = tuple(
            value for value in sensitive_values if type(value) is str and value
        )
        self._stream, self._fd, self._thread = stream, None, None
        # This logger is private: no global handlers, propagation or basicConfig.
        self._logger = logging.Logger("aftercare.worker-observation", level=logging.INFO)
        self._logger.propagate = False
        self._logger.addHandler(_QueueHandler(self))
        try:
            try:
                descriptor = stream.fileno()
            except (AttributeError, OSError, ValueError):
                descriptor = None  # StringIO/capture streams used by unit tests.
            if descriptor is not None:
                # A blocked daemon never owns stderr's Python TextIO shutdown lock.
                self._fd = os.dup(descriptor)
            self._thread = threading.Thread(
                target=self._write, name="aftercare-worker-log", daemon=True
            )
            self._thread.start()
        except Exception:
            self._write_failed = True
            self._dropped_write_error += 1
            if self._fd is not None:
                try:
                    os.close(self._fd)
                except OSError:
                    pass
                self._fd = None

    def __call__(self, event):
        try:
            safe = _safe_record(event, self._sensitive_values)
            self._logger.info("worker_stage", extra={"worker_event": safe})
        except Exception:
            # Formatting/handler failure must not create a new worker failure.
            pass

    def _offer(self, event):
        with self._lock:
            if self._closed:
                self._dropped_closed += 1
            elif self._write_failed:
                self._dropped_write_error += 1
            else:
                try:
                    self._queue.put_nowait(event)
                except queue.Full:
                    self._dropped_full += 1

    def _write(self):
        try:
            while not self._stop.is_set() or not self._queue.empty():
                try:
                    record = self._queue.get(timeout=0.02)
                except queue.Empty:
                    continue
                try:
                    with self._lock:
                        failed = self._write_failed
                        dropped = self._dropped_full + self._dropped_write_error
                    if failed:
                        with self._lock:
                            self._dropped_write_error += 1
                        continue
                    if dropped:
                        record = {**record, "logging_dropped": dropped}
                    line = json.dumps(record, ensure_ascii=True, allow_nan=False) + "\n"
                    if self._fd is None:
                        self._stream.write(line)
                        self._stream.flush()
                    else:
                        pending = line.encode("utf-8")
                        while pending:
                            written = os.write(self._fd, pending)
                            if written <= 0:
                                raise OSError("WORKER_LOG_WRITE_FAILED")
                            pending = pending[written:]
                except Exception:
                    with self._lock:
                        self._write_failed = True
                        self._dropped_write_error += 1
                finally:
                    self._queue.task_done()
        finally:
            # Only the writer closes its fd, including after a slow write returns.
            if self._fd is not None:
                try:
                    os.close(self._fd)
                except OSError:
                    pass

    def stats(self):
        with self._lock:
            return {
                "dropped_full": self._dropped_full,
                "dropped_write_error": self._dropped_write_error,
                "dropped_closed": self._dropped_closed,
                "pending": self._queue.qsize(),
                "writer_alive": self._thread is not None and self._thread.is_alive(),
            }

    def close(self, timeout=0.25):
        with self._lock:
            self._closed = True
        self._stop.set()
        if self._thread is not None:
            try:
                self._thread.join(timeout=max(0, min(timeout, 1)))
            except Exception:
                pass
        return self.stats()


def json_worker_sink(stream=None, *, max_queue=256, sensitive_values=()):
    """Create an opt-in stderr sink; normal and repeated close calls are bounded."""
    if type(max_queue) is not int or not 1 <= max_queue <= 4096:
        max_queue = 256
    return _JsonWorkerSink(sys.stderr if stream is None else stream, max_queue, sensitive_values)
