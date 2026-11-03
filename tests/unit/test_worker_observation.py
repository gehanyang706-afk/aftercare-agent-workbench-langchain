"""Optional worker observations must not become authority, retries or a secret sink."""

import json
import logging
import os
import subprocess
import sys
import time
from copy import deepcopy
from io import StringIO
from threading import Event
from types import SimpleNamespace

import pytest

from aftercare.domain.contracts import BusinessError
from aftercare.services.outbox import OutboxClaim
from aftercare.services.recovery import ReconciliationClaim
from aftercare.services.worker_observation import WorkerObservation, json_worker_sink
from aftercare.workers.outbox_worker import OutboxWorker
from aftercare.workers.reconciliation_worker import ReconciliationWorker

SECRET = "never-log-this-provider-token"
PAYLOAD = {
    "application_id": "app-observe",
    "effect_key": "exchange:app-observe",
    "variant_id": "tee-white-L",
    "quantity": 1,
}


def fixture(kind, sink=None, *, outcome="SUCCEEDED", permission_error=None, finish_error=None):
    calls = []
    claim = (
        OutboxClaim(
            "outbox-observe", "request-observe", "owner-observe", 7, 3, deepcopy(PAYLOAD), "a" * 64
        )
        if kind == "outbox"
        else ReconciliationClaim(
            "task-observe",
            "request-observe",
            "outbox-observe",
            "owner-observe",
            7,
            2,
            PAYLOAD["effect_key"],
            PAYLOAD["application_id"],
            "a" * 64,
        )
    )
    receipt = {
        "request": {"id": "request-observe", "status": outcome},
        "outbox": {"status": "DELIVERED"},
    }

    class Service:
        def expire_leases(self):
            calls.append("expire")
            return 0

        def ensure_pending(self):
            calls.append("ensure")

        def claim(self, owner):
            calls.append("claim")
            assert owner == "owner-observe"
            return claim

        def begin_delivery(self, value):
            self.permission(value)

        def check_claim(self, value):
            self.permission(value)

        def permission(self, value):
            calls.append("permission")
            assert value is claim
            if permission_error is not None:
                raise permission_error

        def finish(self, value, result, **fields):
            calls.append("finish")
            assert value is claim and result == outcome
            if finish_error is not None:
                raise finish_error
            return receipt

    class Client:
        def deliver(self, payload, request_hash):
            assert payload == PAYLOAD and request_hash == "a" * 64
            return self.result()

        def query(self, effect_key, application_id, request_hash):
            assert effect_key == PAYLOAD["effect_key"] and application_id == "app-observe"
            assert request_hash == "a" * 64
            return self.result()

        def result(self):
            calls.append("transport")
            return SimpleNamespace(
                outcome=outcome,
                external_shipment_id="MOCK-observe" if outcome == "SUCCEEDED" else None,
                provider_sequence=2 if outcome == "SUCCEEDED" else None,
                error_code="WAREHOUSE_TRANSPORT_UNKNOWN" if outcome == "UNKNOWN" else None,
            )

    service, client = Service(), Client()
    worker = (
        OutboxWorker(service, client, owner="owner-observe", observation=sink)
        if kind == "outbox"
        else ReconciliationWorker(service, client, owner="owner-observe", observation=sink)
    )
    return SimpleNamespace(
        worker=worker, service=service, client=client, claim=claim, calls=calls, receipt=receipt
    )


@pytest.mark.parametrize("kind", ["outbox", "reconciliation"])
def test_observation_off_preserves_result_and_call_order_without_output(kind, capsys):
    f = fixture(kind)
    assert f.worker.run_once() is f.receipt
    assert f.calls == (
        ["expire", "claim", "permission", "transport", "finish"]
        if kind == "outbox"
        else ["ensure", "expire", "claim", "permission", "transport", "finish"]
    )
    captured = capsys.readouterr()
    assert captured.out == captured.err == ""


@pytest.mark.parametrize("kind", ["outbox", "reconciliation"])
def test_observation_off_does_not_start_threads_or_allocate_run_ids(kind, monkeypatch):
    from aftercare.services import worker_observation

    def forbidden(*args, **kwargs):
        pytest.fail("Disabled observations must not start log threads or allocate run IDs")

    monkeypatch.setattr(worker_observation.threading, "Thread", forbidden)
    monkeypatch.setattr(worker_observation, "uuid4", forbidden)
    f = fixture(kind)
    assert f.worker.run_once() is f.receipt


@pytest.mark.parametrize("kind", ["outbox", "reconciliation"])
def test_enabled_observation_links_stages_without_replacing_original_receipt(kind):
    events = []
    f = fixture(kind, events.append)
    assert f.worker.run_once() is f.receipt
    assert [r["stage"] for r in events if r["phase"] == "started"] == [
        "cycle",
        "maintenance",
        "claim",
        "permission",
        "transport",
        "finish",
    ]
    assert len({r["run_id"] for r in events}) == 1
    assert events[-1]["stage"] == "cycle" and events[-1]["outcome"] == "SUCCEEDED"
    assert all(r["schema"] == "worker-observation-v1" and r["worker_kind"] == kind for r in events)
    claim_end = next(r for r in events if r["stage"] == "claim" and r["phase"] == "finished")
    assert claim_end["application_id"] == "app-observe"
    assert claim_end["effect_key"] == "exchange:app-observe"
    assert claim_end["shipment_request_id"] == "request-observe"
    assert claim_end["outbox_id"] == "outbox-observe" and claim_end["generation"] == 7
    if kind == "outbox":
        assert claim_end["attempt"] == 3 and "cycle" not in claim_end
    else:
        assert claim_end["task_id"] == "task-observe" and claim_end["cycle"] == 2
        assert "attempt" not in claim_end  # The claim has no attempts counter; do not invent one.
    assert all(r["duration_ms"] >= 0 for r in events if r["phase"] == "finished")
    assert (
        f.claim.payload == PAYLOAD
        if kind == "outbox"
        else f.claim.effect_key == PAYLOAD["effect_key"]
    )


@pytest.mark.parametrize("kind", ["outbox", "reconciliation"])
def test_unknown_transport_still_uses_existing_transactional_finish(kind):
    events = []
    f = fixture(kind, events.append, outcome="UNKNOWN")
    assert f.worker.run_once() is f.receipt
    assert f.calls[-2:] == ["transport", "finish"]
    transport = next(r for r in events if r["stage"] == "transport" and r["phase"] == "finished")
    assert transport["outcome"] == "UNKNOWN"
    assert transport["error_code"] == "WAREHOUSE_TRANSPORT_UNKNOWN"
    assert events[-1]["outcome"] == "UNKNOWN"


@pytest.mark.parametrize(
    "kind,code",
    [
        ("outbox", "OUTBOX_LEASE_LOST"),
        ("outbox", "DELIVERY_ALREADY_STARTED"),
        ("reconciliation", "RECONCILIATION_LEASE_LOST"),
    ],
)
def test_permission_failure_never_opens_external_transport(kind, code):
    events = []
    f = fixture(kind, events.append, permission_error=BusinessError(code))
    result = f.worker.run_once()
    assert result["error_code"] == code and "transport" not in f.calls and "finish" not in f.calls
    assert result["status"] == (
        "DELIVERY_ALREADY_STARTED" if code == "DELIVERY_ALREADY_STARTED" else "LEASE_LOST"
    )
    assert events[-1]["outcome"] == result["status"]
    assert any(r["phase"] == "failed" and r["error_code"] == code for r in events)


@pytest.mark.parametrize(
    "kind,code", [("outbox", "OUTBOX_LEASE_LOST"), ("reconciliation", "RECONCILIATION_LEASE_LOST")]
)
def test_successful_provider_does_not_become_business_success_after_lost_finish(kind, code):
    events = []
    f = fixture(kind, events.append, finish_error=BusinessError(code))
    assert f.worker.run_once()["status"] == "LEASE_LOST"
    assert (
        next(r for r in events if r["stage"] == "transport" and r["phase"] == "finished")["outcome"]
        == "SUCCEEDED"
    )
    assert events[-1]["stage"] == "cycle" and events[-1]["outcome"] == "LEASE_LOST"
    assert not any(r["stage"] == "finish" and r["phase"] == "finished" for r in events)


@pytest.mark.parametrize("kind", ["outbox", "reconciliation"])
def test_unexpected_business_error_is_reraised_unchanged(kind):
    events = []
    error = BusinessError("PAYLOAD_INVARIANT")
    f = fixture(kind, events.append, permission_error=error)
    with pytest.raises(BusinessError) as caught:
        f.worker.run_once()
    assert caught.value is error and "transport" not in f.calls
    assert events[-1]["phase"] == "failed" and events[-1]["error_code"] == "PAYLOAD_INVARIANT"


@pytest.mark.parametrize("kind", ["outbox", "reconciliation"])
def test_unknown_exception_message_and_provider_secret_never_enter_log(kind):
    events = []
    f = fixture(kind, events.append)
    failure = OSError(SECRET + "\nAuthorization: Bearer customer-message")

    def failed(*args):
        raise failure

    setattr(f.client, "deliver" if kind == "outbox" else "query", failed)
    with pytest.raises(OSError) as caught:
        f.worker.run_once()
    assert caught.value is failure
    serialized = json.dumps(events)
    assert (
        SECRET not in serialized
        and "Authorization" not in serialized
        and "customer-message" not in serialized
    )
    assert events[-1]["error_code"] == "WORKER_OPERATION_FAILED"


@pytest.mark.parametrize("kind", ["outbox", "reconciliation"])
def test_collector_failure_cannot_change_the_business_result_or_invoke_retry(kind):
    def broken(event):
        raise RuntimeError(SECRET)

    f = fixture(kind, broken)
    assert f.worker.run_once() is f.receipt
    assert f.calls.count("claim") == f.calls.count("transport") == f.calls.count("finish") == 1


def test_schema_allowlist_drops_bodies_tokens_unbounded_and_injected_identifiers():
    events = []
    observer = WorkerObservation("outbox", "owner\nAuthorization: " + SECRET, events.append)
    observer.bind_claim(
        SimpleNamespace(
            outbox_id="outbox-safe",
            request_id="request-safe",
            generation=True,
            attempts=-3,
            payload={**PAYLOAD, "authorization": SECRET, "text": SECRET},
            headers={"Authorization": SECRET},
            token=SECRET,
            request_hash=SECRET,
        )
    )
    with observer.stage("transport") as span:
        span.result(outcome="SUCCEEDED\n" + SECRET, error_code="token=" + SECRET)
    encoded = json.dumps(events)
    assert SECRET not in encoded and "Authorization" not in encoded
    assert all(
        "owner" not in row and "generation" not in row and "attempt" not in row for row in events
    )
    assert all(
        set(row)
        <= {
            "schema",
            "ts",
            "worker_kind",
            "run_id",
            "owner",
            "stage",
            "phase",
            "duration_ms",
            "application_id",
            "effect_key",
            "shipment_request_id",
            "outbox_id",
            "task_id",
            "generation",
            "attempt",
            "cycle",
            "outcome",
            "error_code",
        }
        for row in events
    )


def test_json_sink_is_stderr_only_and_does_not_mutate_global_logging(capsys):
    root_handlers = list(logging.getLogger().handlers)
    sink = json_worker_sink()
    observer = WorkerObservation("outbox", "owner-safe", sink)
    with observer.stage("claim") as span:
        span.result(outcome="IDLE")
    assert not sink.close(timeout=0.5)["writer_alive"]
    captured = capsys.readouterr()
    assert captured.out == ""
    rows = [json.loads(line) for line in captured.err.splitlines()]
    assert len(rows) == 2 and rows[-1]["outcome"] == "IDLE"
    assert list(logging.getLogger().handlers) == root_handlers


def test_broken_log_stream_is_ignored_without_printing_logging_error(capsys):
    class Broken(StringIO):
        def write(self, value):
            raise OSError(SECRET)

    sink = json_worker_sink(Broken())
    observer = WorkerObservation("outbox", "owner-safe", sink)
    with observer.stage("claim"):
        pass
    stats = sink.close(timeout=0.5)
    assert stats["dropped_write_error"] >= 1 and not stats["writer_alive"]
    assert capsys.readouterr().err == ""


def test_production_sink_redacts_a_known_secret_even_inside_an_otherwise_valid_id():
    stream = StringIO()
    sink = json_worker_sink(stream, sensitive_values=(SECRET,))
    observer = WorkerObservation("outbox", "owner-" + SECRET, sink)
    with observer.stage("claim") as span:
        span.result(outcome="IDLE")
    assert not sink.close(timeout=0.5)["writer_alive"]
    assert SECRET not in stream.getvalue()
    assert all("owner" not in json.loads(line) for line in stream.getvalue().splitlines())


def test_slow_stderr_drops_bounded_records_and_does_not_block_worker_or_close():
    entered, release = Event(), Event()

    class Slow(StringIO):
        def write(self, value):
            entered.set()
            release.wait(timeout=2)
            return super().write(value)

    sink = json_worker_sink(Slow(), max_queue=2)
    try:
        with WorkerObservation("outbox", "owner-safe", sink).stage("claim"):
            pass
        assert entered.wait(timeout=1)
        started = time.monotonic()
        f = fixture("outbox", sink)
        assert f.worker.run_once() is f.receipt
        assert time.monotonic() - started < 0.25
        started = time.monotonic()
        stats = sink.close(timeout=0.02)
        assert time.monotonic() - started < 0.25
        assert stats["dropped_full"] > 0 and stats["pending"] <= 2 and stats["writer_alive"]
    finally:
        release.set()
        sink.close(timeout=0.5)


def test_full_real_stderr_pipe_does_not_hold_the_worker_process_at_exit():
    read_fd, write_fd = os.pipe()
    child = None
    try:
        os.set_blocking(write_fd, False)
        while True:
            try:
                os.write(write_fd, b"x" * 4096)
            except BlockingIOError:
                break
        os.set_blocking(write_fd, True)
        # No reader drains stderr until the child exits: its daemon's os.write is
        # genuinely blocked on a full pipe, without owning a TextIO shutdown lock.
        script = """
import json
import time
from aftercare.services.worker_observation import WorkerObservation, json_worker_sink
sink = json_worker_sink(max_queue=2)
started = time.monotonic()
for _ in range(100):
    with WorkerObservation('outbox', 'owner-safe', sink).stage('claim') as span:
        span.result(outcome='IDLE')
stats = sink.close(timeout=.02)
print(json.dumps({'elapsed': time.monotonic()-started, 'stats': stats}), flush=True)
"""
        child = subprocess.Popen(
            [sys.executable, "-c", script], stdout=subprocess.PIPE, stderr=write_fd, text=True
        )
        child.wait(timeout=5)
        result = json.loads(child.stdout.read())
        assert child.returncode == 0 and result["elapsed"] < 0.5
        assert result["stats"]["writer_alive"] and result["stats"]["dropped_full"] > 0
    finally:
        if child is not None:
            if child.poll() is None:
                child.kill()
                child.wait(timeout=5)
            child.stdout.close()
        os.close(write_fd)
        os.close(read_fd)


@pytest.mark.parametrize("command", ["outbox-worker", "reconciliation-worker"])
@pytest.mark.parametrize("enabled", [False, True])
def test_cli_log_json_flag_is_opt_in_and_old_stdout_contract_stays_intact(
    monkeypatch, capsys, command, enabled
):
    from aftercare import cli
    from aftercare.workers import outbox_worker, reconciliation_worker

    settings, calls = object(), []
    monkeypatch.setattr(cli, "Settings", lambda **kwargs: settings)

    def work(value, **kwargs):
        assert value is settings
        calls.append(kwargs)
        kwargs["emit"]({"status": "IDLE", "expired_leases": 0})
        return {"status": "IDLE", "expired_leases": 0}

    monkeypatch.setattr(outbox_worker, "work", work)
    monkeypatch.setattr(reconciliation_worker, "work", work)
    args = [command, "--once", "--owner", "owner-safe"]
    if enabled:
        args.append("--log-json")
    assert cli.main(args) == 0
    assert calls[0]["once"] and calls[0]["owner"] == "owner-safe"
    if enabled:
        assert calls[0]["log_json"] is True
    else:
        assert "log_json" not in calls[0]
    captured = capsys.readouterr()
    assert json.loads(captured.out) == {"status": "IDLE", "expired_leases": 0}
    assert captured.err == ""


@pytest.mark.parametrize("kind", ["outbox", "reconciliation"])
def test_repeated_cycles_have_distinct_run_ids_without_mutating_claim_payload(kind):
    events = []
    f = fixture(kind, events.append)
    f.worker.run_once()
    first = {r["run_id"] for r in events}
    events.clear()
    f.worker.run_once()
    assert first.isdisjoint({r["run_id"] for r in events})
