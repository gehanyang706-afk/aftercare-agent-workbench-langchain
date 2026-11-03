"""Real C2 process/HTTP recovery matrix; only isolated 55434/aftercare_test.

No resets, model calls or old evidence edits. All synthetic business/provider facts
remain for audit. Passwords, Bearer and HMAC secrets exist only in memory/stdin/env.
"""

import argparse
import json
import os
import re
import secrets
import signal
import socket
import sqlite3
import sys
import time
from contextlib import ExitStack
from pathlib import Path

import httpx
from sqlalchemy import func, select

from aftercare.config import PROJECT_ROOT, Settings
from aftercare.domain.warehouse import PROVIDER_ID, WarehouseEvent, sign_event
from aftercare.mock_warehouse.store import WarehouseStore
from aftercare.repositories.database import database_health, session_factory
from aftercare.repositories.models import (
    InboxConflict,
    Inventory,
    ReconciliationTask,
    StaffAccount,
    WarehouseInbox,
    WarehouseOutbox,
)
from aftercare.services.business import columns
from aftercare.services.demo import seed
from scripts.backend_baseline import TEST_URL, validate_target
from scripts.backend_c1_http_smoke import (
    KILL_CHILD,
    Smoke,
    assert_outcome,
    command,
    enqueue,
    fault,
    new_variant,
    require,
    snapshot,
    wait_due,
)

REQUIRED = [
    "before-send-exit",
    "query-recovers-unknown",
    "post-accept-kill",
    "query-takeover",
    "early-callback",
    "callback-ordering",
    "query-exhaustion",
    "authorized-redrive",
]


WAITING_CONSUMER = r"""
import json, os, sys, threading
from pathlib import Path
from aftercare.config import Settings
from aftercare.repositories.database import session_factory
from aftercare.services.outbox import OutboxService
from aftercare.services.warehouse_attempt import BoundedWarehouseClient
from aftercare.workers.outbox_worker import OutboxWorker
sessions = None
try:
    settings = Settings(_env_file=None)
    sessions = session_factory(settings)
    worker = OutboxWorker(OutboxService(sessions), BoundedWarehouseClient(
        settings.warehouse_base_url, settings.warehouse_token.get_secret_value(), timeout=2))
    destination = Path(sys.argv[1]); temporary = Path(str(destination) + ".tmp")
    with temporary.open("x") as file:
        file.write(json.dumps({"status":"WAITING_BEFORE_CLAIM", "pid":os.getpid()}))
        file.flush(); os.fsync(file.fileno())
    os.rename(temporary, destination)
    threading.Event().wait(timeout=60)
    worker.run_once()
except Exception as error:
    print(json.dumps({"error_code":type(error).__name__}), flush=True)
    sys.exit(1)
finally:
    if sessions is not None: sessions.kw["bind"].dispose()
"""


QUERY_CHILD = r"""
import json, os, sys, time
from pathlib import Path
from aftercare.config import Settings
from aftercare.domain.contracts import BusinessError
from aftercare.repositories.database import session_factory
from aftercare.services.recovery import RecoveryService
from aftercare.services.warehouse_http import WarehouseHTTPClient
sessions = None
def write_atomic(path, value):
    path = Path(path); temporary = Path(str(path) + ".tmp")
    with temporary.open("x") as file:
        file.write(json.dumps(value)); file.flush(); os.fsync(file.fileno())
    os.rename(temporary, path)
try:
    settings = Settings(_env_file=None)
    sessions = session_factory(settings)
    service = RecoveryService(sessions, lease_seconds=3, max_attempts=3, deadline_seconds=60)
    service.ensure_pending()
    claim = service.claim("c2-old-query-" + str(os.getpid()))
    if claim is None or claim.request_id != sys.argv[4]:
        raise RuntimeError("QUERY_PROBE_WRONG_CLAIM")
    service.check_claim(claim)
    result = WarehouseHTTPClient(settings.warehouse_base_url,
        settings.warehouse_token.get_secret_value(), timeout=1).query(
            claim.effect_key, claim.application_id, claim.request_hash)
    if result.outcome != "SUCCEEDED": raise RuntimeError("QUERY_PROBE_NO_SUCCESS")
    write_atomic(sys.argv[1], {"status":"GET_SUCCEEDED_BEFORE_FINISH", "pid":os.getpid(),
        "task_id":claim.task_id, "generation":claim.generation, "cycle":claim.cycle,
        "request_id":claim.request_id, "external_shipment_id":result.external_shipment_id})
    deadline = time.monotonic() + 60
    while not Path(sys.argv[2]).exists():
        if time.monotonic() >= deadline: raise RuntimeError("QUERY_PROBE_GO_TIMEOUT")
        time.sleep(.05)
    try:
        service.finish(claim, result.outcome, external_shipment_id=result.external_shipment_id,
            provider_sequence=result.provider_sequence, error_code=result.error_code)
    except BusinessError as error:
        if error.code != "RECONCILIATION_LEASE_LOST": raise
        write_atomic(sys.argv[3], {"status":"LEASE_LOST", "error_code":error.code,
            "pid":os.getpid(), "task_id":claim.task_id})
    else:
        raise RuntimeError("STALE_QUERY_WAS_ALLOWED_TO_COMMIT")
except Exception as error:
    print(json.dumps({"error_code":type(error).__name__}), flush=True)
    sys.exit(1)
finally:
    if sessions is not None: sessions.kw["bind"].dispose()
"""


class C2Smoke(Smoke):
    def __init__(self, output, env, bearer, callback_secret):
        self.callback_secret = callback_secret
        super().__init__(output, env, bearer)

    def record(self, value):
        require(self.callback_secret not in json.dumps(value), "SECRET_IN_EVIDENCE")
        super().record(value)

    def reconcile(self, label):
        return self.cli(["reconciliation-worker", "--once"], label)


def wait_marker(smoke, child, marker):
    deadline = time.monotonic() + smoke.remaining(12)
    while not marker.exists():
        require(child.poll() is None, "PROBE_EXITED_BEFORE_MARKER")
        require(time.monotonic() < deadline, "PROBE_MARKER_TIMEOUT")
        time.sleep(0.05)
    value = json.loads(marker.read_text())
    require(value["pid"] == child.pid, "PROBE_PID_MISMATCH")
    return value


def state(smoke, sessions, clients, case, target, sqlite_path):
    return snapshot(smoke, sessions, clients[0], case, target, sqlite_path)


def final(smoke, sessions, clients, case, target, sqlite_path):
    case["final"] = state(smoke, sessions, clients, case, target, sqlite_path)
    return case["final"]


def post_fault(smoke, provider, case, mode, **extra):
    return smoke.request(
        provider,
        "POST",
        "/__test/faults",
        json={"effect_key": case["effect_key"], "mode": mode, **extra},
    )


def support_state(smoke, support, case):
    return smoke.request(support, "GET", f"/api/fulfillment-requests/{case['request_id']}")


def manual(smoke, client, resource, action, generation, *, key, status=202):
    path = (
        f"/api/fulfillment-requests/{resource}/reconcile"
        if action == "reconcile"
        else (f"/api/outbox/{resource}/redrive")
    )
    return smoke.request(
        client,
        "POST",
        path,
        status=status,
        json={"expected_generation": generation, "reason": "C2 bounded acceptance"},
        headers={"Idempotency-Key": key},
    )


def send_event(smoke, client, event, *, status=200, bad_source=False, bad_signature=False):
    raw = json.dumps(event, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    timestamp = str(int(time.time()))
    headers = {
        "Content-Type": "application/json",
        "X-Warehouse-Source": "forged-source" if bad_source else PROVIDER_ID,
        "X-Warehouse-Timestamp": timestamp,
        "X-Warehouse-Signature": "v1=" + "0" * 64
        if bad_signature
        else sign_event(smoke.callback_secret, raw, timestamp),
    }
    return smoke.request(
        client,
        "POST",
        "/api/integrations/warehouse/events",
        status=status,
        content=raw,
        headers=headers,
    )


def inbox_counts(sessions):
    with sessions() as session:
        return {
            model.__tablename__: session.scalar(select(func.count()).select_from(model))
            for model in (WarehouseInbox, InboxConflict)
        }


def copied_provider_events(source, copy_path, effect_key):
    # SQLite's online backup includes committed WAL facts. Opening WarehouseStore only
    # touches this private copy, never the live provider file or its delivery metadata.
    with sqlite3.connect(source.as_uri() + "?mode=ro", uri=True, timeout=2) as incoming:
        with sqlite3.connect(copy_path, timeout=2) as outgoing:
            incoming.backup(outgoing)
    events = WarehouseStore(copy_path).events(effect_key)
    require(len(events) == 2, "PROVIDER_EVENTS_NOT_PERSISTED")
    return [WarehouseEvent.model_validate(event).model_dump() for event in events]


def provider_start(smoke, provider, sqlite_path, *, callbacks, label):
    original = smoke.env
    smoke.env = dict(original)
    if callbacks:
        smoke.env.update(
            MOCK_WAREHOUSE_CALLBACK_BASE_URL="http://127.0.0.1:8134",
            MOCK_WAREHOUSE_CALLBACK_SECRET=smoke.callback_secret,
        )
    else:
        smoke.env.pop("MOCK_WAREHOUSE_CALLBACK_BASE_URL", None)
        smoke.env.pop("MOCK_WAREHOUSE_CALLBACK_SECRET", None)
    try:
        child = smoke.spawn(
            ["mock-warehouse", "--port", "8142", "--state-path", str(sqlite_path), "--test-faults"],
            label,
        )
    finally:
        smoke.env = original
    smoke.provider_pid = child.pid
    smoke.wait_ready(child, provider)
    return child


def callback_cli(smoke, sqlite_path, effect_key, label):
    original = smoke.env
    smoke.env = {
        **original,
        "MOCK_WAREHOUSE_CALLBACK_BASE_URL": "http://127.0.0.1:8134",
        "MOCK_WAREHOUSE_CALLBACK_SECRET": smoke.callback_secret,
    }
    try:
        return smoke.cli(
            [
                "mock-warehouse-callbacks",
                "--once",
                "--effect-key",
                effect_key,
                "--state-path",
                str(sqlite_path),
            ],
            label,
        )
    finally:
        smoke.env = original


def accepted_unknown(smoke, sessions, prefix, name, target, clients, provider, sqlite_path):
    case = enqueue(smoke, sessions, prefix, name, target, clients, provider, sqlite_path)
    fault(smoke, provider, case, "error_after_accept_once")
    case["delivery"] = smoke.consumer(name + "-delivery")
    case["unknown"] = state(smoke, sessions, clients, case, target, sqlite_path)
    assert_outcome(case["unknown"], "UNKNOWN", "BLOCKED", 5, "RESERVED", 1)
    return case


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir", type=Path, default=PROJECT_ROOT / "runtime/backend-C2/http-first"
    )
    args = parser.parse_args()
    raw = os.environ.get("DATABASE_URL", "")
    validate_target(raw)
    require(
        raw == TEST_URL and os.environ.get("ALLOW_REAL_MODEL_CALLS") == "false",
        "EXPLICIT_ISOLATED_OFFLINE_ENVIRONMENT_REQUIRED",
    )
    output = args.output_dir.resolve()
    require(output.is_relative_to((PROJECT_ROOT / "runtime").resolve()), "OUTPUT_MUST_BE_RUNTIME")
    require(not output.exists(), "FIRST_EVIDENCE_EXISTS_USE_NEW_DIRECTORY")
    from aftercare.offline import install_offline_guard

    install_offline_guard()
    for port in (8134, 8142):
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", port))
    output.mkdir(parents=True, mode=0o700)
    bearer, callback_secret = secrets.token_urlsafe(40), secrets.token_urlsafe(40)
    require(bearer != callback_secret, "CALLBACK_AND_BEARER_KEYS_MUST_DIFFER")
    env = {
        **os.environ,
        "DATABASE_URL": TEST_URL,
        "ALLOW_REAL_MODEL_CALLS": "false",
        "PYTHONPATH": "src:.",
        "WAREHOUSE_DELIVERY_ENABLED": "true",
        "WAREHOUSE_SIMULATION_ENABLED": "false",
        "DEMO_LOGIN_ENABLED": "true",
        "WAREHOUSE_BASE_URL": "http://127.0.0.1:8142",
        "WAREHOUSE_TOKEN": bearer,
        "MOCK_WAREHOUSE_TOKEN": bearer,
        "WAREHOUSE_CALLBACKS_ENABLED": "true",
        "WAREHOUSE_CALLBACK_SECRET": callback_secret,
        "WAREHOUSE_TIMEOUT_SECONDS": "2",
        "OUTBOX_LEASE_SECONDS": "15",
        "OUTBOX_MAX_ATTEMPTS": "3",
        "RECONCILIATION_MAX_ATTEMPTS": "3",
        "RECONCILIATION_DEADLINE_SECONDS": "60",
        "RECONCILIATION_MAX_CYCLES": "3",
    }
    env.pop("MOCK_WAREHOUSE_CALLBACK_BASE_URL", None)
    env.pop("MOCK_WAREHOUSE_CALLBACK_SECRET", None)
    smoke = C2Smoke(output, env, bearer, callback_secret)
    settings = Settings(
        _env_file=None,
        database_url=TEST_URL,
        warehouse_delivery_enabled=False,
        warehouse_callbacks_enabled=False,
        warehouse_simulation_enabled=False,
    )
    sessions = session_factory(settings)
    accounts, usernames, credentials, failure = [], [], {}, None
    sqlite_path = output / "warehouse.sqlite3"
    prefix = "c2-" + secrets.token_hex(6)
    report = {
        "status": "RUNNING",
        "scope": "local controlled C2 process/HTTP matrix",
        "database": "127.0.0.1:55434/aftercare_test",
        "real_model_requests": 0,
        "time_budget_seconds": 180,
        "required_scenarios": REQUIRED,
        "completed_scenarios": [],
        "scenarios": smoke.scenarios,
        "processes": smoke.processes,
    }
    try:
        require(database_health(settings).status == "ready", "DATABASE_NOT_READY")
        with sessions() as session:
            active_outbox = session.scalar(
                select(func.count())
                .select_from(WarehouseOutbox)
                .where(WarehouseOutbox.status.in_(["PENDING", "CLAIMED"]))
            )
            active_queries = session.scalar(
                select(func.count())
                .select_from(ReconciliationTask)
                .where(ReconciliationTask.status.in_(["PENDING", "CLAIMED"]))
            )
            require(active_outbox == active_queries == 0, "EXISTING_ACTIVE_WORK_REFUSED")
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
                    "STAFF_USERNAME_EXISTS",
                )
            usernames.append(value["username"])
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
                    base_url="http://127.0.0.1:8142",
                    trust_env=False,
                    headers={"Authorization": "Bearer " + bearer},
                    timeout=5,
                )
            )
            clients = [
                stack.enter_context(
                    httpx.Client(base_url="http://127.0.0.1:8134", trust_env=False, timeout=5)
                )
                for _ in range(3)
            ]
            event_client = stack.enter_context(
                httpx.Client(base_url="http://127.0.0.1:8134", trust_env=False, timeout=5)
            )
            owner, support, warehouse = clients
            provider_process = provider_start(
                smoke, provider, sqlite_path, callbacks=False, label="provider-callbacks-off"
            )
            app_process = smoke.spawn(
                ["serve", "--offline", "--demo-login", "--async-warehouse", "--port", "8134"],
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

            case = enqueue(
                smoke, sessions, prefix, REQUIRED[0], target, clients, provider, sqlite_path
            )
            marker = output / "before-claim.json"
            child = smoke.spawn(
                [sys.executable, "-c", WAITING_CONSUMER, str(marker)],
                "consumer-stopped-before-claim",
                cli=False,
            )
            case["waiting_marker"] = wait_marker(smoke, child, marker)
            smoke.stop(child)
            require(
                child.returncode in {-signal.SIGTERM, -signal.SIGKILL},
                "WAITING_CONSUMER_NOT_TERMINATED",
            )
            pending = state(smoke, sessions, clients, case, target, sqlite_path)
            require(
                pending["outbox"]["attempts"] == 0 and pending["provider_shipment_count"] == 0,
                "WAITING_CONSUMER_ALREADY_SENT",
            )
            case["consumer"] = smoke.consumer("replacement-consumer-after-before-send-exit")
            assert_outcome(
                final(smoke, sessions, clients, case, target, sqlite_path),
                "SUCCEEDED",
                "DELIVERED",
                6,
                "CONSUMED",
                1,
            )
            command(smoke, owner, case, "complete", 6, "COMPLETED")
            final(smoke, sessions, clients, case, target, sqlite_path)
            report["completed_scenarios"].append(case["name"])

            case = accepted_unknown(
                smoke, sessions, prefix, REQUIRED[1], target, clients, provider, sqlite_path
            )
            generation = support_state(smoke, support, case)["management"]["outbox_generation"]
            outbox_id = case["unknown"]["outbox"]["id"]
            case["unknown_redrive_denied"] = manual(
                smoke,
                support,
                outbox_id,
                "redrive",
                generation,
                key="unknown-cannot-post",
                status=409,
            )
            require(
                case["unknown_redrive_denied"]["error_code"] == "REDRIVE_NOT_ALLOWED",
                "UNKNOWN_REDRIVE_NOT_REFUSED",
            )
            case["query"] = smoke.reconcile("recover-accepted-response-loss")
            assert_outcome(
                final(smoke, sessions, clients, case, target, sqlite_path),
                "SUCCEEDED",
                "DELIVERED",
                6,
                "CONSUMED",
                1,
            )
            report["completed_scenarios"].append(case["name"])

            case = enqueue(
                smoke, sessions, prefix, REQUIRED[2], target, clients, provider, sqlite_path
            )
            marker = output / "delivery-accepted-before-finish.json"
            child = smoke.spawn(
                [sys.executable, "-c", KILL_CHILD, str(marker), case["request_id"]],
                "delivery-killed-after-provider-accept",
                cli=False,
            )
            case["marker"] = wait_marker(smoke, child, marker)
            smoke.stop(child, kill=True)
            require(child.returncode == -signal.SIGKILL, "DELIVERY_NOT_SIGKILLED")
            with sessions() as session:
                lease_end = session.get(
                    WarehouseOutbox, case["marker"]["outbox_id"]
                ).lease_expires_at
            wait_due(smoke, lease_end.isoformat())
            case["query"] = smoke.reconcile("query-after-delivery-process-kill")
            assert_outcome(
                final(smoke, sessions, clients, case, target, sqlite_path),
                "SUCCEEDED",
                "DELIVERED",
                6,
                "CONSUMED",
                1,
            )
            report["completed_scenarios"].append(case["name"])

            case = accepted_unknown(
                smoke, sessions, prefix, REQUIRED[3], target, clients, provider, sqlite_path
            )
            marker, go, resumed = (
                output / name
                for name in ("query-accepted.json", "query-go.flag", "query-old-finish.json")
            )
            child = smoke.spawn(
                [
                    sys.executable,
                    "-c",
                    QUERY_CHILD,
                    str(marker),
                    str(go),
                    str(resumed),
                    case["request_id"],
                ],
                "old-query-paused-after-get",
                cli=False,
            )
            case["old_query_marker"] = wait_marker(smoke, child, marker)
            with sessions() as session:
                lease_end = session.get(
                    ReconciliationTask, case["old_query_marker"]["task_id"]
                ).lease_expires_at
            wait_due(smoke, lease_end.isoformat())
            case["replacement_query"] = smoke.reconcile("replacement-query-after-expired-lease")
            recovered = final(smoke, sessions, clients, case, target, sqlite_path)
            assert_outcome(recovered, "SUCCEEDED", "DELIVERED", 6, "CONSUMED", 1)
            with go.open("x") as file:
                file.write("resume old query\n")
            child.wait(timeout=smoke.remaining(10))
            smoke.stop(child)
            require(child.returncode == 0 and resumed.exists(), "OLD_QUERY_PROBE_DID_NOT_FINISH")
            case["old_finish"] = json.loads(resumed.read_text())
            require(
                case["old_finish"]["error_code"] == "RECONCILIATION_LEASE_LOST",
                "OLD_QUERY_WAS_NOT_FENCED",
            )
            require(
                state(smoke, sessions, clients, case, target, sqlite_path) == recovered,
                "OLD_QUERY_CHANGED_COMMITTED_RESULT",
            )
            report["completed_scenarios"].append(case["name"])

            smoke.stop(provider_process)
            provider_process = provider_start(
                smoke, provider, sqlite_path, callbacks=True, label="provider-early-callbacks-on"
            )
            case = enqueue(
                smoke, sessions, prefix, REQUIRED[4], target, clients, provider, sqlite_path
            )
            post_fault(smoke, provider, case, "delay_after_accept_once", delay_seconds=1.0)
            case["delivery"] = smoke.consumer("delivery-races-early-callbacks")
            early = final(smoke, sessions, clients, case, target, sqlite_path)
            assert_outcome(early, "SUCCEEDED", "DELIVERED", 6, "CONSUMED", 1)
            require(
                case["delivery"].get("status") == "LEASE_LOST" and len(early["inbox"]) == 2,
                "CALLBACK_DID_NOT_PRECEDE_DELIVERY_FINISH",
            )
            require(early["history_count"] == 6, "EARLY_CALLBACK_DUPLICATED_BUSINESS_AUDIT")
            report["completed_scenarios"].append(case["name"])
            smoke.stop(provider_process)
            provider_process = provider_start(
                smoke, provider, sqlite_path, callbacks=False, label="provider-callbacks-off-again"
            )

            case = accepted_unknown(
                smoke, sessions, prefix, REQUIRED[5], target, clients, provider, sqlite_path
            )
            events = copied_provider_events(
                sqlite_path, output / "callback-events-copy.sqlite3", case["effect_key"]
            )
            require(
                [event["sequence"] for event in events] == [1, 2], "PROVIDER_EVENT_ORDER_INVALID"
            )
            case["sequence_two_first"] = send_event(smoke, event_client, events[1], status=202)
            after_gap = state(smoke, sessions, clients, case, target, sqlite_path)
            require(after_gap["application"]["version"] == 5, "GAP_EVENT_CHANGED_BUSINESS")
            before_duplicates = inbox_counts(sessions)
            case["sequence_two_duplicate"] = send_event(smoke, event_client, events[1], status=202)
            require(inbox_counts(sessions) == before_duplicates, "DUPLICATE_INBOX_RECORD_CREATED")
            case["sequence_one"] = send_event(smoke, event_client, events[0])
            merged = state(smoke, sessions, clients, case, target, sqlite_path)
            assert_outcome(merged, "SUCCEEDED", "DELIVERED", 6, "CONSUMED", 1)
            case["old_sequence_replay"] = send_event(smoke, event_client, events[0])
            stale = {**events[0], "event_id": events[0]["event_id"] + ":stale-proof"}
            case["old_sequence_stale"] = send_event(smoke, event_client, stale)
            require(case["old_sequence_stale"]["status"] == "STALE", "OLD_EVENT_NOT_STALE")
            for field, value in (
                ("external_shipment_id", "C2-CONFLICT-EXTERNAL"),
                ("request_hash", "0" * 64),
            ):
                result = send_event(smoke, event_client, {**events[1], field: value}, status=409)
                require(result["status"] == "CONFLICT", "EVENT_ID_CONFLICT_NOT_RETAINED")
            before_bad_auth = inbox_counts(sessions)
            send_event(smoke, event_client, events[1], status=401, bad_signature=True)
            send_event(smoke, event_client, events[1], status=401, bad_source=True)
            require(inbox_counts(sessions) == before_bad_auth, "INVALID_AUTH_WROTE_INBOX")
            unknown = {
                **events[0],
                "event_id": prefix + ":unknown-event",
                "effect_key": prefix + ":unknown-effect",
                "application_id": prefix + "-unknown",
            }
            case["unknown_event"] = send_event(smoke, event_client, unknown, status=202)
            case["provider_cli_callbacks"] = [
                callback_cli(smoke, sqlite_path, case["effect_key"], "callback-sender-sequence-1"),
                callback_cli(smoke, sqlite_path, case["effect_key"], "callback-sender-sequence-2"),
            ]
            current = final(smoke, sessions, clients, case, target, sqlite_path)
            assert_outcome(current, "SUCCEEDED", "DELIVERED", 6, "CONSUMED", 1)
            require(
                current["history_count"] == 6 and len(current["conflicts"]) == 2,
                "CALLBACK_CONFLICT_OR_SIDE_EFFECT_COUNT_WRONG",
            )
            report["completed_scenarios"].append(case["name"])

            case = accepted_unknown(
                smoke, sessions, prefix, REQUIRED[6], target, clients, provider, sqlite_path
            )
            post_fault(smoke, provider, case, "query_unavailable")
            case["queries"] = []
            for attempt in range(3):
                observed = state(smoke, sessions, clients, case, target, sqlite_path)
                task = observed["reconciliation"]
                if task.get("next_attempt_at"):
                    wait_due(smoke, task["next_attempt_at"])
                case["queries"].append(smoke.reconcile(f"unavailable-query-{attempt + 1}"))
            exhausted = final(smoke, sessions, clients, case, target, sqlite_path)
            assert_outcome(exhausted, "MANUAL_REVIEW", "DEAD", 5, "RESERVED", 1)
            require(
                exhausted["reconciliation"]["status"] == "EXHAUSTED"
                and exhausted["reconciliation"]["attempts"] == 3,
                "QUERY_BUDGET_NOT_EXHAUSTED",
            )
            case["budget_probe"] = smoke.reconcile("exhausted-budget-does-not-reopen")
            require(case["budget_probe"]["status"] == "IDLE", "EXHAUSTED_QUERY_REOPENED")
            require(
                state(smoke, sessions, clients, case, target, sqlite_path) == exhausted,
                "EXHAUSTED_QUERY_BUDGET_CHANGED",
            )
            generation = support_state(smoke, support, case)["management"]["outbox_generation"]
            case["ambiguous_redrive_denied"] = manual(
                smoke,
                support,
                exhausted["outbox"]["id"],
                "redrive",
                generation,
                key="ambiguous-cannot-post",
                status=409,
            )
            require(
                case["ambiguous_redrive_denied"]["error_code"] == "REDRIVE_NOT_ALLOWED",
                "AMBIGUOUS_FAILURE_REDRIVEN",
            )
            report["completed_scenarios"].append(case["name"])

            case = enqueue(
                smoke, sessions, prefix, REQUIRED[7], target, clients, provider, sqlite_path
            )
            fault(smoke, provider, case, "permanent_reject")
            case["first_delivery"] = smoke.consumer("permanent-rejection-before-authorized-redrive")
            rejected = state(smoke, sessions, clients, case, target, sqlite_path)
            assert_outcome(rejected, "FAILED", "DEAD", 5, "RESERVED", 0)
            managed = support_state(smoke, support, case)
            generation, outbox_id = (
                managed["management"]["outbox_generation"],
                rejected["outbox"]["id"],
            )
            case["warehouse_denied"] = manual(
                smoke,
                warehouse,
                outbox_id,
                "redrive",
                generation,
                key="warehouse-cannot-redrive",
                status=403,
            )
            case["wrong_generation"] = manual(
                smoke, support, outbox_id, "redrive", generation + 99, key="wrong-cas", status=409
            )
            require(
                case["wrong_generation"]["error_code"] == "GENERATION_CONFLICT",
                "MANUAL_CAS_NOT_ENFORCED",
            )
            require(
                state(smoke, sessions, clients, case, target, sqlite_path) == rejected,
                "FORBIDDEN_MANUAL_COMMAND_CHANGED_BUSINESS",
            )
            post_fault(smoke, provider, case, "clear")
            receipt = manual(
                smoke, support, outbox_id, "redrive", generation, key="approved-redrive"
            )
            repeated = manual(
                smoke, support, outbox_id, "redrive", generation, key="approved-redrive"
            )
            require(
                receipt == repeated and receipt["status"] == "PENDING", "REDRIVE_REPLAY_MISMATCH"
            )
            case["redrive"] = receipt
            case["second_delivery"] = smoke.consumer("delivery-after-authorized-redrive")
            recovered = final(smoke, sessions, clients, case, target, sqlite_path)
            assert_outcome(recovered, "SUCCEEDED", "DELIVERED", 6, "CONSUMED", 1)
            after_generation = support_state(smoke, support, case)["management"][
                "outbox_generation"
            ]
            require(
                generation < receipt["outbox_generation"] < after_generation
                and recovered["outbox"]["attempts"] == 2
                and recovered["request"]["request_hash"] == rejected["request"]["request_hash"]
                and recovered["request"]["effect_key"] == rejected["request"]["effect_key"],
                "REDRIVE_CHANGED_BUSINESS_KEY_OR_COUNTERS",
            )
            case["generations"] = [generation, receipt["outbox_generation"], after_generation]
            report["completed_scenarios"].append(case["name"])

            with sessions() as session:
                require(
                    columns(session.get(Inventory, "tee-white-M")) == original_stock,
                    "ORIGINAL_SKU_STOCK_CHANGED",
                )
                stock = columns(session.get(Inventory, target))
            require(
                stock["available"] == 12 and stock["reserved"] == 1,
                "FINAL_SYNTHETIC_STOCK_MISMATCH",
            )
            require(len(report["completed_scenarios"]) == len(REQUIRED), "MATRIX_INCOMPLETE")
            report.update(status="PASSED", synthetic_inventory_final=stock)
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
                    try:
                        child.kill()
                        child.wait(timeout=1)
                    except Exception:
                        cleanup_errors.append("CHILD_CLEANUP_FAILED")
        try:
            with sessions() as session:
                recovered = list(
                    session.scalars(
                        select(StaffAccount.id).where(StaffAccount.username.in_(usernames))
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
        for item in smoke.processes:
            for child in smoke.children:
                if item["pid"] == child.pid:
                    item["returncode"] = child.poll()
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
            not_completed=[name for name in REQUIRED if name not in report["completed_scenarios"]],
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
        require(
            bearer not in encoded
            and callback_secret not in encoded
            and all(value["password"] not in encoded for value in credentials.values()),
            "SECRET_IN_REPORT",
        )
        with (output / "http-first.json").open("x") as file:
            file.write(encoded + "\n")
        smoke.journal.close()
    print(
        json.dumps(
            {
                "status": report["status"],
                "output": str(output / "http-first.json"),
                "completed": len(report["completed_scenarios"]),
                "required": len(REQUIRED),
                "real_model_requests": 0,
            }
        ),
        flush=True,
    )
    return 0 if failure is None and report["status"] == "PASSED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
