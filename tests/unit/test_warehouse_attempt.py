"""Real loopback drip responses cannot hold the warehouse child indefinitely."""

import json
import threading
import time
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from aftercare.domain.warehouse import shipment_request_hash
from aftercare.services import warehouse_attempt
from aftercare.services.warehouse_attempt import BoundedWarehouseClient

TOKEN = "attempt-local-test-token-" + "q" * 32
PAYLOAD = {
    "effect_key": "exchange:attempt-1",
    "application_id": "attempt-1",
    "variant_id": "white-L",
    "quantity": 1,
}
HASH = shipment_request_hash(**PAYLOAD)


@contextmanager
def server(mode):
    stop = threading.Event()
    seen = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_POST(self):
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            seen.append(
                {
                    "method": "POST",
                    "body": body,
                    "authorization_matches": self.headers.get("Authorization") == "Bearer " + TOKEN,
                }
            )
            try:
                if mode == "header-drip":
                    self.connection.sendall(b"HTTP/1.1 200 OK\r\nX-Slow: ")
                elif mode == "body-drip":
                    self.connection.sendall(
                        b"HTTP/1.1 200 OK\r\nContent-Length: 60000\r\n"
                        b"Content-Type: application/json\r\n\r\n"
                    )
                else:
                    result = {
                        "effect_key": body["effect_key"],
                        "application_id": body["application_id"],
                        "request_hash": body["request_hash"],
                        "external_shipment_id": "MOCK-attempt-1",
                        "status": "SUCCEEDED",
                    }
                    encoded = json.dumps(result).encode()
                    self.send_response(200)
                    self.send_header("Content-Length", str(len(encoded)))
                    self.send_header("Content-Type", "application/json")
                    self.end_headers()
                    self.wfile.write(encoded)
                    return
                while not stop.is_set():
                    self.connection.sendall(b"x")
                    stop.wait(0.03)
            except OSError:
                pass

    http = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(
        target=http.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True
    )
    thread.start()
    try:
        yield f"http://127.0.0.1:{http.server_port}", seen
    finally:
        stop.set()
        http.shutdown()
        http.server_close()
        thread.join(timeout=1)


def capture_processes(monkeypatch):
    original = warehouse_attempt.subprocess.Popen
    observed = []

    def create(*args, **kwargs):
        process = original(*args, **kwargs)
        observed.append((args, kwargs, process))
        return process

    monkeypatch.setattr(warehouse_attempt.subprocess, "Popen", create)
    return observed


def assert_reaped(observed):
    assert len(observed) == 1
    process = observed[0][2]
    assert process.poll() is not None
    for stream in (process.stdin, process.stdout, process.stderr):
        assert stream is None or stream.closed


def test_normal_binding_succeeds_with_default_budget_and_private_stdin(monkeypatch):
    observed = capture_processes(monkeypatch)
    monkeypatch.setenv("WAREHOUSE_TOKEN", TOKEN)
    monkeypatch.setenv("DASHSCOPE_API_KEY", "do-not-inherit-provider-secret")
    monkeypatch.setenv("DATABASE_URL", "do-not-inherit-database-secret")
    monkeypatch.setenv("PYTHONPATH", "/untrusted/user-path")
    with server("success") as (url, seen):
        result = BoundedWarehouseClient(url, TOKEN).deliver(PAYLOAD, HASH)
    assert result.outcome == "SUCCEEDED"
    assert result.external_shipment_id == "MOCK-attempt-1"
    assert len(seen) == 1
    assert seen[0]["body"] == {**PAYLOAD, "request_hash": HASH}
    assert seen[0]["authorization_matches"]
    argv, options, _process = observed[0]
    assert TOKEN not in repr(argv)
    assert TOKEN not in repr(options["env"])
    assert "/untrusted/user-path" not in repr(options["env"])
    assert "DASHSCOPE_API_KEY" not in options["env"]
    assert "WAREHOUSE_TOKEN" not in options["env"]
    assert "DATABASE_URL" not in options["env"]
    assert options["stderr"] == warehouse_attempt.subprocess.DEVNULL
    assert_reaped(observed)


@pytest.mark.parametrize("mode", ["header-drip", "body-drip"])
def test_continuous_drip_obeys_total_budget_and_reaps_child(monkeypatch, mode):
    observed = capture_processes(monkeypatch)
    with server(mode) as (url, seen):
        started = time.monotonic()
        result = BoundedWarehouseClient(url, TOKEN, timeout=1.5).deliver(PAYLOAD, HASH)
        elapsed = time.monotonic() - started
        assert elapsed < 3.0
        assert len(seen) == 1
    assert result.outcome == "UNKNOWN"
    assert result.error_code == "WAREHOUSE_DEADLINE_EXCEEDED"
    assert TOKEN not in repr(result)
    assert_reaped(observed)


def test_spawn_error_is_safe_and_does_not_echo_token(monkeypatch):
    def failure(*args, **kwargs):
        raise OSError("sensitive diagnostic: " + TOKEN)

    monkeypatch.setattr(warehouse_attempt.subprocess, "Popen", failure)
    result = BoundedWarehouseClient("http://127.0.0.1:8140", TOKEN).deliver(PAYLOAD, HASH)
    assert result.outcome == "UNKNOWN"
    assert result.error_code == "WAREHOUSE_CHILD_FAILED"
    assert TOKEN not in repr(result)


def test_local_corruption_does_not_spawn_a_child(monkeypatch):
    def forbidden(*args, **kwargs):
        pytest.fail("Corrupt payload must not spawn a warehouse attempt")

    monkeypatch.setattr(warehouse_attempt.subprocess, "Popen", forbidden)
    result = BoundedWarehouseClient("http://127.0.0.1:8140", TOKEN).deliver(PAYLOAD, "wrong")
    assert result.outcome == "FAILED" and result.error_code == "DELIVERY_PAYLOAD_CONFLICT"
