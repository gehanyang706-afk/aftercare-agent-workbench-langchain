"""Bounded C1 acceptance through real local CLI processes/TCP; retain first evidence.

Requires the isolated, already-migrated 55434/aftercare_test. No resets, downloads,
models or existing outbox mutations. New business/SQLite evidence is retained.
"""

import argparse
import json
import os
import re
import secrets
import signal
import socket
import sqlite3
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from datetime import datetime, timedelta
from pathlib import Path
from threading import Barrier
from urllib.parse import quote

import httpx
from sqlalchemy import func, select

from aftercare.config import PROJECT_ROOT, Settings
from aftercare.domain.contracts import ActionInput, utc_now
from aftercare.domain.warehouse import shipment_request_hash
from aftercare.offline import install_offline_guard
from aftercare.repositories.database import database_health, session_factory
from aftercare.repositories.models import (
    Inventory,
    Order,
    OrderLine,
    Reservation,
    StaffAccount,
    Variant,
    WarehouseOutbox,
)
from aftercare.services.business import BusinessService, columns
from aftercare.services.demo import seed
from scripts.backend_baseline import TEST_URL, validate_target


def require(condition, code):
    if not condition:
        raise RuntimeError(code)


KILL_CHILD = r"""
import json, os, sys, threading
from pathlib import Path
from aftercare.config import Settings
from aftercare.repositories.database import session_factory
from aftercare.services.outbox import OutboxService
from aftercare.services.warehouse_http import WarehouseHTTPClient
sessions = None
try:
    settings = Settings(_env_file=None, warehouse_timeout_seconds=1.0, outbox_lease_seconds=3)
    sessions = session_factory(settings)
    service = OutboxService(sessions, lease_seconds=3)
    claim = service.claim("c1-kill-probe-" + str(os.getpid()))
    if claim is None or claim.request_id != sys.argv[2]:
        raise RuntimeError("KILL_PROBE_WRONG_CLAIM")
    service.begin_delivery(claim)
    result = WarehouseHTTPClient(settings.warehouse_base_url,
        settings.warehouse_token.get_secret_value(), timeout=1.0).deliver(
            claim.payload, claim.request_hash)
    if result.outcome != "SUCCEEDED":
        raise RuntimeError("KILL_PROBE_PROVIDER_NOT_ACCEPTED")
    marker = {"status":"ACCEPTED_BEFORE_FINISH", "pid":os.getpid(),
        "request_id":claim.request_id, "outbox_id":claim.outbox_id,
        "external_shipment_id":result.external_shipment_id}
    destination = Path(sys.argv[1])
    temporary = Path(str(destination) + ".tmp")
    with temporary.open("x") as output:
        output.write(json.dumps(marker) + "\n")
        output.flush()
        os.fsync(output.fileno())
    os.rename(temporary, destination)
    print(json.dumps({"status":"WAITING_BEFORE_FINISH", "pid":os.getpid()}), flush=True)
    threading.Event().wait(timeout=60)
    raise RuntimeError("KILL_PROBE_NOT_KILLED")
except Exception as error:
    print(json.dumps({"status":"STOPPED", "error_code":type(error).__name__}), flush=True)
    sys.exit(1)
finally:
    if sessions is not None:
        sessions.kw["bind"].dispose()
"""


class Smoke:
    def __init__(self, output, env, token):
        self.output, self.env, self.token = output, env, token
        self.started = time.monotonic()
        self.deadline = self.started + 150  # Reserve 30 of the 180 seconds for cleanup.
        self.processes, self.children = [], []
        self.scenarios = []
        self.journal = (output / "processes.jsonl").open("x")

    def remaining(self, cap=10, *, cleanup=False):
        deadline = self.started + 179 if cleanup else self.deadline
        remaining = deadline - time.monotonic()
        require(remaining > 0, "SMOKE_TIME_BUDGET_EXHAUSTED")
        return min(cap, remaining)

    def record(self, value):
        encoded = json.dumps(value, ensure_ascii=False)
        require(self.token not in encoded, "SECRET_IN_EVIDENCE")
        self.journal.write(encoded + "\n")
        self.journal.flush()

    def spawn(self, arguments, label, *, cli=True):
        self.remaining()
        argv = [sys.executable, "-m", "aftercare", *arguments] if cli else arguments
        child = subprocess.Popen(
            argv,
            cwd=PROJECT_ROOT,
            env=self.env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self.children.append(child)
        item = {"label": label, "pid": child.pid, "returncode": None}
        self.processes.append(item)
        self.record({"event": "started", **item})
        return child

    def stop(self, child, *, kill=False):
        if child.poll() is None:
            child.kill() if kill else child.terminate()
            try:
                child.wait(timeout=self.remaining(2, cleanup=True))
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait(timeout=self.remaining(2, cleanup=True))
        for item in self.processes:
            if item["pid"] == child.pid:
                item["returncode"] = child.returncode
        self.record({"event": "stopped", "pid": child.pid, "returncode": child.returncode})

    def cli(self, arguments, label, *, stdin=None, cleanup=False):
        child = subprocess.Popen(
            [sys.executable, "-m", "aftercare", *arguments],
            cwd=PROJECT_ROOT,
            env=self.env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        self.children.append(child)
        item = {"label": label, "pid": child.pid, "returncode": None}
        self.processes.append(item)
        try:
            stdout, _stderr = child.communicate(
                input=stdin, timeout=self.remaining(15 if not cleanup else 5, cleanup=cleanup)
            )
        except BaseException:
            self.stop(child, kill=True)
            raise
        item["returncode"] = child.returncode
        # CLI results are structured and secret-safe; raw stderr/environment are never saved.
        result = None
        try:
            result = json.loads(stdout)
        except (ValueError, TypeError):
            pass
        self.record({"event": "cli_finished", **item, "result": result})
        require(child.returncode == 0 and isinstance(result, dict), "CLI_COMMAND_FAILED")
        return {**result, "_process": dict(item)}

    def wait_ready(self, child, client):
        deadline = time.monotonic() + self.remaining(15)
        while time.monotonic() < deadline:
            require(child.poll() is None, "SERVER_PROCESS_EXITED")
            try:
                if client.get("/health/ready", timeout=self.remaining(1)).status_code == 200:
                    return
            except httpx.TransportError:
                pass
            time.sleep(0.1)
        raise RuntimeError("SERVER_READINESS_TIMEOUT")

    def request(self, client, method, path, *, status=200, **kwargs):
        response = client.request(method, path, timeout=self.remaining(5), **kwargs)
        require(response.status_code == status, "HTTP_UNEXPECTED_STATUS")
        return response.json()

    def consumer(self, label):
        return self.cli(["outbox-worker", "--once"], label)


def provider_count(path, effect_key):
    with sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=2) as connection:
        return connection.execute(
            "SELECT count(*) FROM shipments WHERE effect_key=?", (effect_key,)
        ).fetchone()[0]


def new_variant(sessions, prefix):
    identity = prefix + "-target"
    with sessions.begin() as session:
        source = session.get(Variant, "tee-white-L")
        require(source is not None, "SOURCE_VARIANT_MISSING")
        payload = columns(source)
        payload["id"] = identity
        session.add(Variant(**payload))
        session.flush()
        session.add(Inventory(variant_id=identity, available=20, reserved=0, version=1))
    return identity


def new_case(sessions, prefix, name, target):
    order_id = prefix + "-" + name
    now = utc_now()
    with sessions.begin() as session:
        source = session.get(Variant, "tee-white-M")
        require(source is not None, "SOURCE_VARIANT_MISSING")
        session.add(
            Order(
                id=order_id,
                customer_id="alice",
                status="DELIVERED",
                paid_fen=source.price_fen,
                delivered_at=now - timedelta(days=2),
                version=1,
                policy_version="v1",
            )
        )
        session.flush()
        session.add(
            OrderLine(
                id=order_id + "-1",
                order_id=order_id,
                variant_id=source.id,
                quantity=1,
                occupied=0,
                unused=True,
            )
        )
    business = BusinessService(sessions)
    conversation = business.create_conversation("alice")
    preview = business.preview(
        "alice",
        conversation,
        ActionInput(
            action="exchange",
            order_id=order_id,
            line_id=order_id + "-1",
            quantity=1,
            target_variant=target,
        ),
    )
    business.confirm("alice", preview["id"], True)
    receipt = business.commit("alice", preview["id"])
    return {
        "name": name,
        "application_id": receipt["application_id"],
        "order_id": order_id,
        "original_receipt": receipt,
        "http_commands": [],
    }


def command(smoke, client, case, path, version, expected, **data):
    payload = {"expected_version": version, **data}
    headers = {"Idempotency-Key": case["name"] + ":" + path}
    url = f"/api/applications/{case['application_id']}/{path}"
    receipt = smoke.request(client, "POST", url, json=payload, headers=headers)
    repeated = smoke.request(client, "POST", url, json=payload, headers=headers)
    require(receipt == repeated and receipt["status"] == expected, "HTTP_COMMAND_REPLAY_MISMATCH")
    case["http_commands"].append({"path": path, "receipt": receipt, "exact_replay": True})
    return receipt


def enqueue(smoke, sessions, prefix, name, target, clients, provider, sqlite_path):
    owner, support, warehouse = clients
    case = new_case(sessions, prefix, name, target)
    case.update(http_server_pid=smoke.product_pid, provider_pid=smoke.provider_pid)
    smoke.scenarios.append(case)
    command(smoke, support, case, "review", 1, "AWAITING_RETURN", decision="approve")
    command(
        smoke,
        owner,
        case,
        "return-shipment",
        2,
        "RETURN_IN_TRANSIT",
        carrier="local-simulator",
        tracking_number=prefix + "-" + name,
    )
    command(smoke, warehouse, case, "receive", 3, "RETURN_RECEIVED", note="Accepted return")
    command(smoke, warehouse, case, "replacement-shipments", 4, "REPLACEMENT_PENDING")
    detail = smoke.request(owner, "GET", f"/api/applications/{case['application_id']}")
    request = detail["shipment_request"]
    case.update(request_id=request["id"], effect_key=request["effect_key"])
    lookup(smoke, provider, case["effect_key"], status=404)
    require(
        provider_count(sqlite_path, case["effect_key"]) == 0, "PROVIDER_ACCEPTED_BEFORE_CONSUMER"
    )
    state = snapshot(smoke, sessions, owner, case, target, sqlite_path)
    require(
        state["application"]["version"] == 5
        and state["request"]["status"] == "PENDING"
        and state["outbox"]["status"] == "PENDING"
        and not state["attempts"],
        "ENQUEUE_NOT_PENDING",
    )
    case["before_consumer"] = state
    return case


def lookup(smoke, provider, effect_key, *, status=200):
    return smoke.request(
        provider,
        "GET",
        "/mock-warehouse/shipments/by-key/" + quote(effect_key, safe=""),
        status=status,
    )


def snapshot(smoke, sessions, owner, case, target, sqlite_path):
    detail = smoke.request(owner, "GET", f"/api/applications/{case['application_id']}")
    delivery = smoke.request(owner, "GET", f"/api/fulfillment-requests/{case['request_id']}")
    require(detail["receipt"] == case["original_receipt"], "ORIGINAL_RECEIPT_CHANGED")
    with sessions() as session:
        reservation = columns(session.get(Reservation, case["application_id"]))
        stock = columns(session.get(Inventory, target))
        line = session.get(OrderLine, case["order_id"] + "-1")
        occupied = line.occupied
    return {
        "application": {
            "id": case["application_id"],
            "status": detail["status"],
            "version": detail["version"],
        },
        **delivery,
        "reservation": reservation,
        "inventory": stock,
        "line_occupied": occupied,
        "original_receipt": detail["receipt"],
        "receipt_unchanged": True,
        "history_count": len(detail["history"]),
        "provider_shipment_count": provider_count(sqlite_path, case["effect_key"]),
    }


def assert_outcome(state, request, outbox, version, reservation, count):
    require(
        state["request"]["status"] == request and state["outbox"]["status"] == outbox,
        "DELIVERY_STATE_MISMATCH",
    )
    require(
        state["application"]["version"] == version
        and state["reservation"]["status"] == reservation
        and state["provider_shipment_count"] == count
        and state["line_occupied"] == 1,
        "DELIVERY_INVARIANT_MISMATCH",
    )


def fault(smoke, provider, case, mode):
    smoke.request(
        provider, "POST", "/__test/faults", json={"effect_key": case["effect_key"], "mode": mode}
    )


def wait_due(smoke, timestamp):
    due = datetime.fromisoformat(timestamp)
    deadline = time.monotonic() + smoke.remaining(6)
    while utc_now() <= due:
        require(time.monotonic() < deadline, "BACKOFF_OR_LEASE_WAIT_TIMEOUT")
        time.sleep(min(0.05, smoke.remaining(0.05)))


def provider_protocol(smoke, provider, sqlite_path, prefix, target):
    payload = {
        "effect_key": prefix + ":provider-only",
        "application_id": prefix + "-provider",
        "variant_id": target,
        "quantity": 1,
    }
    wire = {**payload, "request_hash": shipment_request_hash(**payload)}
    barrier = Barrier(2)

    def send():
        barrier.wait(timeout=5)
        with httpx.Client(
            base_url=str(provider.base_url), trust_env=False, headers=provider.headers, timeout=5
        ) as client:
            return smoke.request(client, "POST", "/mock-warehouse/shipments", json=wire)

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(send), pool.submit(send)]
        results = [future.result(timeout=smoke.remaining(10)) for future in futures]
    require(
        results[0] == results[1] and provider_count(sqlite_path, payload["effect_key"]) == 1,
        "PROVIDER_CONCURRENT_IDEMPOTENCY_FAILED",
    )
    changed = {**payload, "quantity": 2}
    conflict = smoke.request(
        provider,
        "POST",
        "/mock-warehouse/shipments",
        status=409,
        json={**changed, "request_hash": shipment_request_hash(**changed)},
    )
    return {
        "scope": "provider-only protocol, no product inventory change",
        "wire": wire,
        "first_result": results[0],
        "concurrent_posts": 2,
        "persisted_rows": 1,
        "different_payload_status": 409,
        "conflict_code": conflict.get("code"),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir", type=Path, default=PROJECT_ROOT / "runtime/backend-C1/http-first"
    )
    args = parser.parse_args()
    raw_url = os.environ.get("DATABASE_URL", "")
    validate_target(raw_url)
    require(
        raw_url == TEST_URL and os.environ.get("ALLOW_REAL_MODEL_CALLS") == "false",
        "EXPLICIT_ISOLATED_OFFLINE_ENVIRONMENT_REQUIRED",
    )
    output = args.output_dir.resolve()
    require(output.is_relative_to((PROJECT_ROOT / "runtime").resolve()), "OUTPUT_MUST_BE_RUNTIME")
    require(not output.exists(), "FIRST_EVIDENCE_EXISTS_USE_NEW_DIRECTORY")
    install_offline_guard()
    for port in (8133, 8140):
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", port))
    output.mkdir(parents=True, mode=0o700)
    token = secrets.token_urlsafe(40)
    env = {
        **os.environ,
        "DATABASE_URL": TEST_URL,
        "ALLOW_REAL_MODEL_CALLS": "false",
        "PYTHONPATH": "src:.",
        "WAREHOUSE_DELIVERY_ENABLED": "true",
        "WAREHOUSE_SIMULATION_ENABLED": "false",
        "DEMO_LOGIN_ENABLED": "true",
        "WAREHOUSE_BASE_URL": "http://127.0.0.1:8140",
        "WAREHOUSE_TOKEN": token,
        "MOCK_WAREHOUSE_TOKEN": token,
        "WAREHOUSE_TIMEOUT_SECONDS": "2",
        "OUTBOX_LEASE_SECONDS": "15",
        "OUTBOX_MAX_ATTEMPTS": "3",
    }
    smoke = Smoke(output, env, token)
    settings = Settings(
        _env_file=None,
        database_url=TEST_URL,
        warehouse_delivery_enabled=False,
        warehouse_simulation_enabled=False,
    )
    sessions = session_factory(settings)
    prefix = "c1-" + secrets.token_hex(6)
    accounts, attempted_usernames, failure = [], [], None
    report = {
        "status": "RUNNING",
        "transport": "real localhost TCP + CLI subprocesses",
        "database": "127.0.0.1:55434/aftercare_test",
        "real_model_requests": 0,
        "time_budget_seconds": 180,
        "processes": smoke.processes,
        "scenarios": smoke.scenarios,
        "scope": "C1 delivery; UNKNOWN is retained, no C2 recovery is claimed",
    }
    sqlite_path = output / "warehouse.sqlite3"
    warehouse_args = [
        "mock-warehouse",
        "--port",
        "8140",
        "--state-path",
        str(sqlite_path),
        "--test-faults",
    ]
    try:
        require(database_health(settings).status == "ready", "DATABASE_NOT_READY")
        with sessions() as session:
            pending = session.scalar(
                select(func.count())
                .select_from(WarehouseOutbox)
                .where(WarehouseOutbox.status.in_(["PENDING", "CLAIMED"]))
            )
            require(pending == 0, "EXISTING_ACTIVE_OUTBOX_REFUSED")
        seed(sessions)
        target = new_variant(sessions, prefix)
        with sessions() as session:
            original_stock = columns(session.get(Inventory, "tee-white-M"))
        credentials = {
            role: {"username": prefix + "-" + role, "password": secrets.token_urlsafe(24)}
            for role in ("support", "warehouse")
        }
        for role, value in credentials.items():
            with sessions() as session:
                require(
                    session.scalar(
                        select(StaffAccount.id).where(StaffAccount.username == value["username"])
                    )
                    is None,
                    "TEMPORARY_STAFF_USERNAME_EXISTS",
                )
            attempted_usernames.append(value["username"])
            result = smoke.cli(
                [
                    "staff",
                    "create",
                    "--username",
                    value["username"],
                    "--role",
                    role,
                    "--password-stdin",
                ],
                "create-" + role,
                stdin=value["password"] + "\n",
            )
            accounts.append(result["id"])
        with ExitStack() as stack:
            provider = stack.enter_context(
                httpx.Client(
                    base_url="http://127.0.0.1:8140",
                    trust_env=False,
                    headers={"Authorization": "Bearer " + token},
                    timeout=5,
                )
            )
            clients = [
                stack.enter_context(
                    httpx.Client(base_url="http://127.0.0.1:8133", trust_env=False, timeout=5)
                )
                for _ in range(3)
            ]
            owner, support, warehouse = clients
            provider_process = smoke.spawn(warehouse_args, "provider-first")
            smoke.provider_pid = provider_process.pid
            smoke.wait_ready(provider_process, provider)
            app_process = smoke.spawn(
                ["serve", "--offline", "--demo-login", "--async-warehouse", "--port", "8133"],
                "product-http",
            )
            smoke.product_pid = app_process.pid
            smoke.wait_ready(app_process, owner)
            signed = smoke.request(
                owner, "POST", "/api/demo-sessions", json={"customer_id": "alice"}
            )
            owner.headers["X-CSRF-Token"] = signed["csrf_token"]
            for client, role in ((support, "support"), (warehouse, "warehouse")):
                signed = smoke.request(
                    client, "POST", "/api/staff/sessions", json=credentials[role]
                )
                client.headers["X-CSRF-Token"] = signed["csrf_token"]

            normal = enqueue(
                smoke, sessions, prefix, "normal", target, clients, provider, sqlite_path
            )
            normal["consumer"] = smoke.consumer("normal-consumer")
            shipped = snapshot(smoke, sessions, owner, normal, target, sqlite_path)
            assert_outcome(shipped, "SUCCEEDED", "DELIVERED", 6, "CONSUMED", 1)
            command(smoke, owner, normal, "complete", 6, "COMPLETED")
            normal["final"] = snapshot(smoke, sessions, owner, normal, target, sqlite_path)
            require(normal["final"]["application"]["version"] == 7, "NORMAL_NOT_COMPLETED")

            protocol = provider_protocol(smoke, provider, sqlite_path, prefix, target)
            normal_external = lookup(smoke, provider, normal["effect_key"])
            smoke.stop(provider_process)
            restarted = smoke.spawn(warehouse_args, "provider-restarted")
            smoke.provider_pid = restarted.pid
            smoke.wait_ready(restarted, provider)
            require(restarted.pid != provider_process.pid, "PROVIDER_PROCESS_NOT_RESTARTED")
            repeated = smoke.request(
                provider, "POST", "/mock-warehouse/shipments", json=protocol["wire"]
            )
            require(repeated == protocol["first_result"], "PROVIDER_RESTART_LOST_IDEMPOTENCY")
            require(
                lookup(smoke, provider, normal["effect_key"]) == normal_external,
                "PROVIDER_RESTART_LOST_NORMAL_RECORD",
            )
            protocol.update(
                first_pid=provider_process.pid,
                restarted_pid=restarted.pid,
                restart_result=repeated,
                normal_record_retained=True,
            )
            report["provider_protocol"] = protocol

            unknown = enqueue(
                smoke, sessions, prefix, "accepted-lost", target, clients, provider, sqlite_path
            )
            fault(smoke, provider, unknown, "error_after_accept_once")
            unknown["consumer"] = smoke.consumer("accepted-lost-consumer")
            unknown["final"] = snapshot(smoke, sessions, owner, unknown, target, sqlite_path)
            assert_outcome(unknown["final"], "UNKNOWN", "BLOCKED", 5, "RESERVED", 1)
            unknown["manual_provider_lookup"] = lookup(smoke, provider, unknown["effect_key"])
            unknown["second_consumer"] = smoke.consumer("unknown-no-resend-consumer")
            require(unknown["second_consumer"]["status"] == "IDLE", "UNKNOWN_WAS_REDELIVERED")
            require(
                snapshot(smoke, sessions, owner, unknown, target, sqlite_path) == unknown["final"],
                "UNKNOWN_CHANGED_WITHOUT_RECONCILIATION",
            )
            unknown["no_resend_evidence"] = (
                "CLI IDLE and unchanged attempts/state; no packet counter"
            )

            retry = enqueue(
                smoke, sessions, prefix, "retryable", target, clients, provider, sqlite_path
            )
            fault(smoke, provider, retry, "retryable_reject_once")
            retry["first_consumer"] = smoke.consumer("retry-first-consumer")
            retry["after_rejection"] = snapshot(smoke, sessions, owner, retry, target, sqlite_path)
            assert_outcome(retry["after_rejection"], "PENDING", "PENDING", 5, "RESERVED", 0)
            lookup(smoke, provider, retry["effect_key"], status=404)
            wait_due(smoke, retry["after_rejection"]["outbox"]["next_attempt_at"])
            retry["second_consumer"] = smoke.consumer("retry-second-consumer")
            retry["final"] = snapshot(smoke, sessions, owner, retry, target, sqlite_path)
            assert_outcome(retry["final"], "SUCCEEDED", "DELIVERED", 6, "CONSUMED", 1)
            require(retry["final"]["outbox"]["attempts"] == 2, "RETRY_ATTEMPT_COUNT_MISMATCH")

            permanent = enqueue(
                smoke, sessions, prefix, "permanent", target, clients, provider, sqlite_path
            )
            fault(smoke, provider, permanent, "permanent_reject")
            permanent["consumer"] = smoke.consumer("permanent-consumer")
            permanent["final"] = snapshot(smoke, sessions, owner, permanent, target, sqlite_path)
            assert_outcome(permanent["final"], "FAILED", "DEAD", 5, "RESERVED", 0)
            lookup(smoke, provider, permanent["effect_key"], status=404)

            if smoke.remaining(150) > 20:
                killed = enqueue(
                    smoke, sessions, prefix, "killed", target, clients, provider, sqlite_path
                )
                marker = output / "accepted-before-finish.json"
                child = smoke.spawn(
                    [sys.executable, "-c", KILL_CHILD, str(marker), killed["request_id"]],
                    "killed-after-provider-accept",
                    cli=False,
                )
                deadline = time.monotonic() + smoke.remaining(10)
                while not marker.exists():
                    require(child.poll() is None, "KILL_PROBE_EXITED_BEFORE_MARKER")
                    require(time.monotonic() < deadline, "KILL_PROBE_MARKER_TIMEOUT")
                    time.sleep(0.05)
                accepted = json.loads(marker.read_text())
                require(accepted["pid"] == child.pid, "KILL_PROBE_PID_MISMATCH")
                smoke.stop(child, kill=True)
                require(child.returncode == -signal.SIGKILL, "KILL_PROBE_WAS_NOT_SIGKILL")
                with sessions() as session:
                    row = session.get(WarehouseOutbox, accepted["outbox_id"])
                    require(row.status == "CLAIMED", "KILL_PROBE_UNEXPECTED_STATE")
                    lease_deadline = row.lease_expires_at.isoformat()
                wait_due(smoke, lease_deadline)
                killed["after_kill_consumer"] = smoke.consumer("expire-killed-lease-consumer")
                killed["final"] = snapshot(smoke, sessions, owner, killed, target, sqlite_path)
                assert_outcome(killed["final"], "UNKNOWN", "BLOCKED", 5, "RESERVED", 1)
                require(
                    killed["after_kill_consumer"]["status"] == "IDLE"
                    and killed["final"]["outbox"]["attempts"] == 1,
                    "KILLED_ATTEMPT_WAS_REDELIVERED",
                )
                killed.update(
                    marker=accepted,
                    process_returncode=child.returncode,
                    manual_provider_lookup=lookup(smoke, provider, killed["effect_key"]),
                    c2_recovery_completed=False,
                )
            else:
                report["kill_probe"] = {"status": "NOT_EXECUTED", "reason": "bounded time budget"}
            with sessions() as session:
                require(
                    columns(session.get(Inventory, "tee-white-M")) == original_stock,
                    "ORIGINAL_SKU_STOCK_CHANGED",
                )
                report["synthetic_inventory_final"] = columns(session.get(Inventory, target))
            stock = report["synthetic_inventory_final"]
            require(
                stock["available"] == 20 - len(report["scenarios"])
                and stock["reserved"] == len(report["scenarios"]) - 2,
                "FINAL_SYNTHETIC_INVENTORY_MISMATCH",
            )
            report["status"] = "PASSED"
    except BaseException as error:
        failure = error
        candidate = str(error) if isinstance(error, RuntimeError) else type(error).__name__
        report.update(
            status="FAILED",
            error_code=candidate if re.fullmatch(r"[A-Z_]+", candidate) else type(error).__name__,
        )
    finally:
        cleanup_errors = []
        for child in reversed(smoke.children):
            if child.poll() is None:
                try:
                    smoke.stop(child)
                except Exception:
                    # Cleanup must still kill our children after the work budget expires.
                    try:
                        child.kill()
                        child.wait(timeout=1)
                    except Exception:
                        cleanup_errors.append("CHILD_CLEANUP_FAILED")
        for item in smoke.processes:
            for child in smoke.children:
                if item["pid"] == child.pid:
                    item["returncode"] = child.poll()
        # A CLI create can commit before its stdout is lost. Recover only this run's
        # prechecked, exact usernames so such accounts are disabled as well.
        try:
            with sessions() as session:
                recovered = list(
                    session.scalars(
                        select(StaffAccount.id).where(
                            StaffAccount.username.in_(attempted_usernames)
                        )
                    )
                )
            accounts = sorted(set(accounts + recovered))
        except Exception:
            cleanup_errors.append("TEMPORARY_STAFF_LOOKUP_FAILED")
        for staff_id in accounts:
            try:
                smoke.cli(
                    ["staff", "disable", "--staff-id", staff_id],
                    "disable-temporary-staff",
                    cleanup=True,
                )
            except Exception:
                cleanup_errors.append("STAFF_DISABLE_FAILED")
        try:
            with sessions() as session:
                disabled = all(
                    not session.get(StaffAccount, identity).active for identity in accounts
                )
            require(disabled, "TEMPORARY_STAFF_STILL_ACTIVE")
        except Exception:
            disabled = False
            cleanup_errors.append("STAFF_DISABLE_NOT_VERIFIED")
        sessions.kw["bind"].dispose()
        report.update(
            elapsed_seconds=round(time.monotonic() - smoke.started, 3),
            cleanup={
                "temporary_staff_disabled": disabled,
                "children_stopped": all(child.poll() is not None for child in smoke.children),
                "errors": cleanup_errors,
            },
            evidence_retained={"provider_sqlite": str(sqlite_path), "business_audit": True},
        )
        if cleanup_errors:
            report["status"] = "FAILED"
        encoded = json.dumps(report, ensure_ascii=False, indent=2)
        require(token not in encoded, "SECRET_IN_REPORT")
        with (output / "http-first.json").open("x") as evidence:
            evidence.write(encoded + "\n")
        smoke.journal.close()
    print(
        json.dumps(
            {
                "status": report["status"],
                "output": str(output / "http-first.json"),
                "scenarios": len(report["scenarios"]),
                "real_model_requests": 0,
            }
        ),
        flush=True,
    )
    return 0 if failure is None and report["status"] == "PASSED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
