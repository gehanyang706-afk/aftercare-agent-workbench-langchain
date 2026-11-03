"""Bounded real-process D3 consumer matrix, only on isolated 55434/aftercare_test.

Each phase runs 12 approved exchanges x 1/2/4 consumers x 3 rounds, separately
for delivery and GET reconciliation. The original CLI's 0.5s loop cadence is
retained. Instrumentation wraps calls, never claims/results or side effects.
Successful cohorts are archived then deleted by exact owned IDs; failures stay.
"""

import argparse
import hashlib
import json
import math
import os
import platform
import re
import secrets
import signal
import socket
import sqlite3
import statistics
import subprocess
import sys
import time
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

import httpx
from sqlalchemy import create_engine, delete, func, select, text

from aftercare.config import PROJECT_ROOT, Settings
from aftercare.domain.fulfillment import Actor, FulfillmentCommandInput
from aftercare.offline import install_offline_guard
from aftercare.repositories.database import database_health, session_factory
from aftercare.repositories.models import (
    Application,
    Base,
    ExchangeFulfillment,
    FulfillmentAudit,
    Inventory,
    Operation,
    OutboxAttempt,
    Preview,
    ReconciliationTask,
    Reservation,
    ShipmentRequest,
    WarehouseInbox,
    WarehouseOutbox,
)
from aftercare.services.business import columns
from aftercare.services.fulfillment import FulfillmentService
from aftercare.services.outbox import OutboxService
from aftercare.services.recovery import RecoveryService
from aftercare.services.warehouse_attempt import BoundedWarehouseClient
from aftercare.workers.outbox_worker import OutboxWorker
from scripts.backend_baseline import TEST_URL, validate_target
from scripts.backend_c1_http_smoke import new_case, new_variant

FORMAT = "backend-d3-consumer-matrix-v1"
REVISION = "0011_dispatch_intent"
PORT = 8150
COHORT_SIZE = 12
ROUNDS = 3
WORKERS = (1, 2, 4)
SUPPORT = Actor("d3-support", "support", "test_fixture")
CUSTOMER = Actor("alice", "customer", "test_fixture")
WAREHOUSE = Actor("d3-warehouse", "warehouse", "test_fixture")


CHILD = r"""
import json, os, signal, sys, time
from datetime import datetime, UTC
from pathlib import Path
from sqlalchemy import event
from sqlalchemy.engine import Engine
from aftercare.cli import main
from aftercare.services.warehouse_attempt import BoundedWarehouseClient
from aftercare.workers.outbox_worker import OutboxWorker
from aftercare.workers.reconciliation_worker import ReconciliationWorker
root, mode, owner, observe = Path(sys.argv[1]), sys.argv[2], sys.argv[3], sys.argv[4]=='true'
journal = root / (owner + '.jsonl')
calls=0; sql_ms=0.; locking=0; in_sql={}; first=True
def write(value):
    with journal.open('a') as f:
        f.write(json.dumps(value,sort_keys=True)+'\n');f.flush()
def before(conn,cursor,statement,parameters,context,executemany):
    global calls,locking
    calls+=1;locking+=int('FOR UPDATE' in statement.upper())
    in_sql[id(context)]=time.perf_counter()
def after(conn,cursor,statement,parameters,context,executemany):
    global sql_ms
    start=in_sql.pop(id(context),None)
    if start is not None:sql_ms+=(time.perf_counter()-start)*1000
event.listen(Engine,'before_cursor_execute',before)
event.listen(Engine,'after_cursor_execute',after)
for method in ('deliver','query'):
    original=getattr(BoundedWarehouseClient,method)
    def transport(self,*args,_original=original,_method=method,**kwargs):
        start=time.perf_counter();outcome='EXCEPTION';code=None
        if _method=='deliver':
            effect_key=args[0].get('effect_key'); application_id=args[0].get('application_id')
        else:effect_key,application_id=args[:2]
        try:
            value=_original(self,*args,**kwargs);outcome=value.outcome;code=value.error_code
            return value
        finally:
            write({'type':'transport','method':_method,'effect_key':effect_key,
                'application_id':application_id,'duration_ms':(time.perf_counter()-start)*1000,
                'outcome':outcome,'error_code':code,'pid':os.getpid()})
    setattr(BoundedWarehouseClient,method,transport)
worker=OutboxWorker if mode=='outbox' else ReconciliationWorker
original_run=worker.run_once
def measured(self):
    global first,calls,sql_ms,locking
    if first:
        first=False
        ready=root/(owner+'.ready.json')
        with ready.open('x') as f:json.dump({'pid':os.getpid(),'owner':owner},f)
        deadline=time.monotonic()+20
        while not (root/'go.json').exists():
            if time.monotonic()>=deadline:raise RuntimeError('GATE_TIMEOUT')
            time.sleep(.01)
        calls=0;sql_ms=0.;locking=0;in_sql.clear()
    start=time.perf_counter();wall=datetime.now(UTC).isoformat()
    prior,prior_ms,prior_locks=calls,sql_ms,locking
    value=None;error=None
    try:
        value=original_run(self);return value
    except Exception as failure:
        error=getattr(failure,'code',type(failure).__name__);raise
    finally:
        request=(value or {}).get('request',{})
        write({'type':'iteration','owner':owner,'pid':os.getpid(),'started_at':wall,
            'finished_after_commit_at':datetime.now(UTC).isoformat(),
            'duration_ms':(time.perf_counter()-start)*1000,
            'request_id':request.get('id'),'application_id':request.get('application_id'),
            'status':request.get('status',(value or {}).get('status')),'error_code':error,
            'sql_calls':calls-prior,'sql_duration_ms':sql_ms-prior_ms,
            'locking_sql_calls':locking-prior_locks})
worker.run_once=measured
args=['outbox-worker' if mode=='outbox' else 'reconciliation-worker','--owner',owner]
if observe:args.append('--log-json')
def terminate(signum,frame):raise SystemExit(128+signum)
signal.signal(signal.SIGTERM,terminate)
try:raise SystemExit(main(args))
finally:
    write({'type':'totals','owner':owner,'pid':os.getpid(),'sql_calls':calls,
        'sql_duration_ms':sql_ms,'locking_sql_calls':locking})
"""


def require(condition, code):
    if not condition:
        raise RuntimeError(code)


def canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"), default=str)


def fingerprint(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def write_json(path, value):
    with path.open("x") as handle:
        handle.write(json.dumps(json.loads(canonical(value)), ensure_ascii=False, indent=2) + "\n")


def output_directory(value):
    path = value.expanduser().resolve()
    require(path.is_relative_to((PROJECT_ROOT / "runtime").resolve()), "OUTPUT_MUST_BE_RUNTIME")
    require(not path.exists(), "EVIDENCE_DIRECTORY_EXISTS")
    path.mkdir(parents=True, mode=0o700)
    return path


def source_info():
    entries = {
        str(path.relative_to(PROJECT_ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted((PROJECT_ROOT / "src/aftercare").rglob("*.py"))
    }
    return {
        "product_entries": entries,
        "product_fingerprint": fingerprint(entries),
        "harness_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "requirements_sha256": hashlib.sha256(
            (PROJECT_ROOT / "requirements.lock").read_bytes()
        ).hexdigest(),
    }


def database_fingerprint(sessions):
    """Keep only hashes/counts of unrelated rows, never credentials or bodies."""
    result = {}
    with sessions() as session:
        session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
        for table in Base.metadata.sorted_tables:
            rows = [dict(row) for row in session.execute(select(table)).mappings()]
            result[table.name] = {
                "rows": len(rows),
                "sha256": fingerprint(sorted(rows, key=canonical)),
            }
    return result


def quiescent(sessions):
    with sessions() as session:
        for model in (WarehouseOutbox, ReconciliationTask):
            require(
                session.scalar(
                    select(func.count())
                    .select_from(model)
                    .where(model.status.in_(["PENDING", "CLAIMED"]))
                )
                == 0,
                "OTHER_ACTIVE_CONSUMER_WORK_REFUSED",
            )
        # ensure_pending can schedule these, even if no task is currently active.
        unsafe = session.scalar(
            select(func.count())
            .select_from(ShipmentRequest)
            .outerjoin(ReconciliationTask, ReconciliationTask.request_id == ShipmentRequest.id)
            .where(
                ShipmentRequest.status != "SUCCEEDED",
                ReconciliationTask.id.is_(None),
            )
        )
        require(unsafe == 0, "OTHER_UNSCHEDULED_REQUEST_REFUSED")


def command(name, version, data=None):
    return FulfillmentCommandInput(
        command=name,
        expected_version=version,
        idempotency_key=f"d3-{name}-{version}",
        data=data or {},
    )


def prepare_cases(sessions, prefix):
    target = new_variant(sessions, prefix)
    fulfillment = FulfillmentService(sessions, async_delivery=True)
    cases = []
    for index in range(COHORT_SIZE):
        case = new_case(sessions, prefix, f"order-{index:02d}", target)
        appid = case["application_id"]
        fulfillment.execute(SUPPORT, appid, command("approve", 1, {"note": "D3 fixture"}))
        fulfillment.execute(
            CUSTOMER,
            appid,
            command("submit_return", 2, {"carrier": "local", "tracking_number": case["order_id"]}),
        )
        fulfillment.execute(WAREHOUSE, appid, command("receive", 3, {"note": "D3 fixture"}))
        fulfillment.execute(WAREHOUSE, appid, command("request_shipment", 4))
        with sessions() as session:
            request = session.scalar(
                select(ShipmentRequest).where(ShipmentRequest.application_id == appid)
            )
            op = session.get(Operation, case["original_receipt"]["operation_id"])
            preview = session.get(Preview, op.preview_id)
            case.update(
                request_id=request.id,
                effect_key=request.effect_key,
                request_hash=request.request_hash,
                payload=request.payload,
                operation_id=op.id,
                preview_id=preview.id,
                conversation_id=preview.conversation_id,
            )
        cases.append(case)
    return target, cases


def state(sessions, cases, target):
    appids = [case["application_id"] for case in cases]
    requestids = [case["request_id"] for case in cases]
    with sessions() as session:
        outboxes = list(
            session.scalars(
                select(WarehouseOutbox).where(WarehouseOutbox.request_id.in_(requestids))
            )
        )
        outboxids = [row.id for row in outboxes]
        models = {
            "applications": (Application, Application.id.in_(appids)),
            "requests": (ShipmentRequest, ShipmentRequest.id.in_(requestids)),
            "outboxes": (WarehouseOutbox, WarehouseOutbox.id.in_(outboxids)),
            "attempts": (OutboxAttempt, OutboxAttempt.outbox_id.in_(outboxids)),
            "tasks": (ReconciliationTask, ReconciliationTask.request_id.in_(requestids)),
            "reservations": (Reservation, Reservation.application_id.in_(appids)),
            "fulfillments": (ExchangeFulfillment, ExchangeFulfillment.application_id.in_(appids)),
            "audits": (FulfillmentAudit, FulfillmentAudit.application_id.in_(appids)),
        }
        result = {
            key: [columns(row) for row in session.scalars(select(model).where(predicate))]
            for key, (model, predicate) in models.items()
        }
        result["stock"] = columns(session.get(Inventory, target))
        result["original_receipts"] = {
            case["application_id"]: session.get(Operation, case["operation_id"]).receipt
            for case in cases
        }
        return result


def cleanup_cases(sessions, cases, target):
    appids = [case["application_id"] for case in cases]
    requestids = [case["request_id"] for case in cases]
    opids = [case["operation_id"] for case in cases]
    previewids = [case["preview_id"] for case in cases]
    convids = [case["conversation_id"] for case in cases]
    orderids = [case["order_id"] for case in cases]
    effects = [case["effect_key"] for case in cases]
    with sessions.begin() as session:
        outboxids = list(
            session.scalars(
                select(WarehouseOutbox.id).where(WarehouseOutbox.request_id.in_(requestids))
            )
        )
        inboxids = list(
            session.scalars(select(WarehouseInbox.id).where(WarehouseInbox.effect_key.in_(effects)))
        )
        for table in reversed(Base.metadata.sorted_tables):
            name, col = table.name, table.c
            predicate = None
            if name == "orders":
                predicate = col.id.in_(orderids)
            elif name == "order_lines":
                predicate = col.order_id.in_(orderids)
            elif name == "variants":
                predicate = col.id == target
            elif name == "inventory":
                predicate = col.variant_id == target
            elif name == "conversations":
                predicate = col.id.in_(convids)
            elif name == "action_previews":
                predicate = col.id.in_(previewids)
            elif name == "operations":
                predicate = col.id.in_(opids)
            elif name == "approvals":
                predicate = col.preview_id.in_(previewids)
            elif name == "aftercare_requests":
                predicate = col.id.in_(appids)
            elif name == "shipment_requests":
                predicate = col.id.in_(requestids)
            elif name == "warehouse_outbox":
                predicate = col.id.in_(outboxids)
            elif name == "warehouse_inbox":
                predicate = col.id.in_(inboxids)
            elif name == "inbox_conflicts":
                predicate = col.inbox_id.in_(inboxids)
            elif "conversation_id" in col:
                predicate = col.conversation_id.in_(convids)
            elif "application_id" in col:
                predicate = col.application_id.in_(appids)
            elif "request_id" in col:
                predicate = col.request_id.in_(requestids)
            elif "outbox_id" in col:
                predicate = col.outbox_id.in_(outboxids)
            elif "operation_id" in col:
                predicate = col.operation_id.in_(opids)
            if predicate is not None:
                session.execute(delete(table).where(predicate))


def provider_snapshot(source, destination):
    with sqlite3.connect(source.as_uri() + "?mode=ro", uri=True) as src:
        with sqlite3.connect(destination) as dst:
            src.backup(dst)
            require(dst.execute("PRAGMA integrity_check").fetchone()[0] == "ok", "SQLITE_INTEGRITY")
            shipments = [
                dict(zip([d[0] for d in cursor.description], row, strict=True))
                for cursor in [dst.execute("SELECT * FROM shipments ORDER BY effect_key")]
                for row in cursor.fetchall()
            ]
            events = dst.execute("SELECT count(*) FROM callback_events").fetchone()[0]
    return {"shipments": shipments, "events": events}


def percentile(values, q):
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * q) - 1)]


def summary(values):
    return {
        "count": len(values),
        "p50": round(statistics.median(values), 3),
        "p95": round(percentile(values, 0.95), 3),
        "min": round(min(values), 3),
        "max": round(max(values), 3),
    }


def utc(value):
    return datetime.fromisoformat(value)


def cpu_seconds(value):
    days, clock = value.split("-", 1) if "-" in value else ("0", value)
    return int(days) * 86400 + sum(
        float(number) * 60**index for index, number in enumerate(reversed(clock.split(":")))
    )


class Sampler:
    def __init__(self, raw, app_prefix, provider_pid):
        self.engine = create_engine(
            raw,
            connect_args={"connect_timeout": 2, "options": "-c statement_timeout=1000"},
            isolation_level="AUTOCOMMIT",
        )
        self.prefix, self.provider_pid = app_prefix, provider_pid
        self.samples, self.lock_samples, self.cpu_max = [], [], {}
        self.query_count, self.errors = 0, []
        self.next_resource = 0

    def sample(self, worker_pids):
        now = time.perf_counter()
        with self.engine.connect() as connection:
            rows = list(
                connection.execute(
                    text(
                        "SELECT pid,wait_event_type,wait_event,pg_blocking_pids(pid) AS blockers "
                        "FROM pg_stat_activity WHERE datname=current_database() "
                        "AND application_name LIKE :prefix ORDER BY pid"
                    ),
                    {"prefix": self.prefix + "%"},
                ).mappings()
            )
        self.query_count += 1
        for row in rows:
            if row["wait_event_type"] == "Lock":
                self.lock_samples.append({"at_monotonic": now, **dict(row)})
        if now < self.next_resource:
            return
        self.next_resource = now + 0.1
        result = subprocess.run(
            ["ps", "-axo", "pid=,ppid=,rss=,time="],
            text=True,
            capture_output=True,
            timeout=1,
            check=False,
        )
        if result.returncode:
            self.errors.append("PS_SAMPLE_FAILED")
            return
        values = {}
        for line in result.stdout.splitlines():
            parts = line.split()
            if len(parts) == 4:
                try:
                    values[int(parts[0])] = (int(parts[1]), int(parts[2]), cpu_seconds(parts[3]))
                except ValueError:
                    continue
        owned = set(worker_pids)
        changed = True
        while changed:
            extra = {pid for pid, value in values.items() if value[0] in owned}
            changed = bool(extra - owned)
            owned |= extra
        for pid in owned | {self.provider_pid}:
            if pid in values:
                self.cpu_max[pid] = max(self.cpu_max.get(pid, 0), values[pid][2])
        self.samples.append(
            {
                "at_monotonic": now,
                "fleet_rss_kib": sum(values[pid][1] for pid in owned if pid in values),
                "provider_rss_kib": values.get(self.provider_pid, (0, 0, 0))[1],
                "sampled_fleet_pids": sorted(owned & values.keys()),
            }
        )

    def finish(self):
        self.engine.dispose()
        return {
            "pg_sample_interval_seconds": 0.02,
            "resource_sample_min_interval_seconds": 0.1,
            "observer_queries_excluded_from_worker_sql": self.query_count,
            "lock_wait_samples": self.lock_samples,
            "lock_observed_pids": sorted({row["pid"] for row in self.lock_samples}),
            "resource_samples": self.samples,
            "sampled_fleet_peak_rss_kib": max(
                (x["fleet_rss_kib"] for x in self.samples), default=0
            ),
            "sampled_provider_peak_rss_kib": max(
                (x["provider_rss_kib"] for x in self.samples), default=0
            ),
            "sampled_fleet_cpu_seconds_lower_bound": round(
                sum(value for pid, value in self.cpu_max.items() if pid != self.provider_pid), 3
            ),
            "sampled_provider_cpu_seconds": self.cpu_max.get(self.provider_pid, 0),
            "sampling_errors": self.errors,
            "limits": (
                "ps samples can miss short-lived children/peaks; excludes parent and PostgreSQL; "
                "no lock samples does not prove no waits"
            ),
        }


def stop(process):
    # Each owned child starts its own session. Include a bounded HTTP subprocess
    # if a failed consumer was stopped during a network call.
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    if process.poll() is None:
        try:
            process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait(timeout=1)


def journal_rows(directory):
    result = []
    for path in sorted(directory.glob("worker-*.jsonl")):
        for line in path.read_text().splitlines():
            try:
                result.append(json.loads(line))
            except ValueError:
                raise RuntimeError("TRUNCATED_WORKER_METRIC") from None
    return result


def wait_until(predicate, deadline, processes=()):
    while not predicate():
        require(time.monotonic() < deadline, "COHORT_DEADLINE_EXCEEDED")
        require(all(p.poll() is None for p in processes), "CONSUMER_EXITED_EARLY")
        time.sleep(0.02)


def run_cohort(args, output, sessions, phase_started, mode, count, round_number):
    started = time.monotonic()

    def remaining():
        return min(45.0, args.budget - (time.monotonic() - phase_started))

    require(remaining() > 5, "PHASE_BUDGET_EXHAUSTED")
    key = f"{mode}-w{count}-r{round_number}"
    directory = output / key
    directory.mkdir()
    prefix = (
        "benchmark_d3_"
        + hashlib.sha256(f"{args.seed}:{args.label}:{key}".encode()).hexdigest()[:16]
    )
    before = database_fingerprint(sessions)
    quiescent(sessions)
    token = secrets.token_urlsafe(40)
    env = {
        **os.environ,
        "DATABASE_URL": TEST_URL,
        "ALLOW_REAL_MODEL_CALLS": "false",
        "PYTHONPATH": "src:.",
        "WAREHOUSE_DELIVERY_ENABLED": "true",
        "WAREHOUSE_SIMULATION_ENABLED": "false",
        "WAREHOUSE_CALLBACKS_ENABLED": "false",
        "WAREHOUSE_BASE_URL": f"http://127.0.0.1:{PORT}",
        "WAREHOUSE_TOKEN": token,
        "MOCK_WAREHOUSE_TOKEN": token,
        "WAREHOUSE_TIMEOUT_SECONDS": "2",
        "OUTBOX_LEASE_SECONDS": "15",
        "OUTBOX_MAX_ATTEMPTS": "3",
        "RECONCILIATION_MAX_ATTEMPTS": "3",
        "RECONCILIATION_DEADLINE_SECONDS": "60",
    }
    env.pop("MOCK_WAREHOUSE_CALLBACK_BASE_URL", None)
    env.pop("MOCK_WAREHOUSE_CALLBACK_SECRET", None)
    provider = None
    consumers, handles = [], []
    sqlite_path = directory / "warehouse.sqlite3"
    report = {
        "name": key,
        "mode": mode,
        "workers": count,
        "round": round_number,
        "prefix": prefix,
        "status": "RUNNING",
        "tasks": COHORT_SIZE,
        "specification": (
            "one shared target SKU, quantity1, stock20, legal v1->v5; original CLI0.5s loop"
        ),
    }
    target, cases, sampler = None, [], None
    try:
        provider_log = (directory / "provider.log").open("x")
        handles.append(provider_log)
        provider = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "aftercare",
                "mock-warehouse",
                "--port",
                str(PORT),
                "--state-path",
                str(sqlite_path),
                "--test-faults",
            ],
            cwd=PROJECT_ROOT,
            env=env,
            stdout=provider_log,
            stderr=provider_log,
            start_new_session=True,
        )
        with httpx.Client(
            base_url=env["WAREHOUSE_BASE_URL"],
            headers={"Authorization": "Bearer " + token},
            trust_env=False,
            timeout=3,
        ) as client:

            def ready():
                try:
                    return client.get("/health/ready").status_code == 200
                except httpx.TransportError:
                    return False

            wait_until(ready, time.monotonic() + 8, [provider])
            target, cases = prepare_cases(sessions, prefix)
            write_json(
                directory / "fixture.json",
                {"target": target, "cases": cases, "state": state(sessions, cases, target)},
            )
            if mode == "query":
                for case in cases:
                    require(
                        client.post(
                            "/__test/faults",
                            json={
                                "effect_key": case["effect_key"],
                                "mode": "error_after_accept_once",
                            },
                        ).status_code
                        == 200,
                        "FAULT_PREPARATION_FAILED",
                    )
                preparation = OutboxWorker(
                    OutboxService(sessions),
                    BoundedWarehouseClient(env["WAREHOUSE_BASE_URL"], token, timeout=2),
                )
                for _ in cases:
                    require(
                        preparation.run_once()["request"]["status"] == "UNKNOWN",
                        "QUERY_PREPARATION_NOT_UNKNOWN",
                    )
                require(
                    RecoveryService(sessions).ensure_pending() == COHORT_SIZE,
                    "QUERY_PREPARATION_NOT_SCHEDULED",
                )
                before_provider = provider_snapshot(
                    sqlite_path, directory / "provider-before-query.sqlite3"
                )
                report["query_preparation"] = {
                    "method": "real POST accepted500 before measured GET fleet",
                    "provider_facts": len(before_provider["shipments"]),
                    "state": state(sessions, cases, target),
                }
            startup = time.perf_counter()
            for number in range(count):
                owner = f"worker-{number}"
                stdout = (directory / (owner + ".stdout.log")).open("x")
                stderr = (directory / (owner + ".stderr.log")).open("x")
                handles.extend([stdout, stderr])
                consumers.append(
                    subprocess.Popen(
                        [
                            sys.executable,
                            "-c",
                            CHILD,
                            str(directory),
                            mode,
                            owner,
                            str(args.label == "after").lower(),
                        ],
                        cwd=PROJECT_ROOT,
                        env={**env, "PGAPPNAME": prefix + "-w" + str(number)},
                        stdout=stdout,
                        stderr=stderr,
                        start_new_session=True,
                    )
                )
            wait_until(
                lambda: len(list(directory.glob("worker-*.ready.json"))) == count,
                time.monotonic() + 12,
                consumers,
            )
            report["startup_to_all_ready_ms"] = (time.perf_counter() - startup) * 1000
            gate = datetime.now(UTC)
            write_json(directory / "go.json", {"utc": gate.isoformat()})
            run_started = time.perf_counter()
            sampler = Sampler(TEST_URL, prefix + "-w", provider.pid)
            deadline = time.monotonic() + remaining()
            requestids = [case["request_id"] for case in cases]
            while True:
                require(time.monotonic() < deadline, "COHORT_DEADLINE_EXCEEDED")
                require(all(p.poll() is None for p in consumers), "CONSUMER_EXITED_EARLY")
                sampler.sample([p.pid for p in consumers])
                with sessions() as session:
                    done = session.scalar(
                        select(func.count())
                        .select_from(ShipmentRequest)
                        .where(
                            ShipmentRequest.id.in_(requestids),
                            ShipmentRequest.status == "SUCCEEDED",
                        )
                    )
                complete = [
                    row
                    for row in journal_rows(directory)
                    if row.get("type") == "iteration" and row.get("status") == "SUCCEEDED"
                ]
                if done == COHORT_SIZE and len(complete) == COHORT_SIZE:
                    break
                time.sleep(0.02)
            report["gate_to_observed_all_success_ms"] = (time.perf_counter() - run_started) * 1000
            for process in consumers:
                stop(process)
            report["sampling"] = sampler.finish()
            sampler = None
            final = state(sessions, cases, target)
            write_json(directory / "business-final.json", final)
            stop(provider)
            facts = provider_snapshot(sqlite_path, directory / "provider-final.sqlite3")
            write_json(directory / "provider-final.json", facts)
            require(
                len(facts["shipments"]) == COHORT_SIZE and facts["events"] == 2 * COHORT_SIZE,
                "PROVIDER_FACT_COUNT_MISMATCH",
            )
            require(
                {row["effect_key"] for row in facts["shipments"]}
                == {case["effect_key"] for case in cases},
                "PROVIDER_KEY_MISMATCH",
            )
            require(
                all(row["status"] == "SUCCEEDED" for row in final["requests"])
                and len(final["requests"]) == COHORT_SIZE,
                "REQUESTS_INCOMPLETE",
            )
            require(
                all(
                    row["status"] == "REPLACEMENT_SHIPPED" and row["version"] == 6
                    for row in final["applications"]
                ),
                "APPLICATION_VERSION_MISMATCH",
            )
            require(
                final["stock"]["available"] == 20 - COHORT_SIZE and final["stock"]["reserved"] == 0,
                "STOCK_MISMATCH",
            )
            require(
                all(
                    row["status"] == "CONSUMED" and row["quantity"] == 1
                    for row in final["reservations"]
                )
                and len(final["reservations"]) == COHORT_SIZE,
                "RESERVATION_MISMATCH",
            )
            by_effect = {row["effect_key"]: row for row in facts["shipments"]}
            require(
                all(
                    row["external_shipment_id"]
                    == by_effect[row["effect_key"]]["external_shipment_id"]
                    and row["request_hash"] == by_effect[row["effect_key"]]["request_hash"]
                    for row in final["requests"]
                ),
                "PROVIDER_RECEIPT_MISMATCH",
            )
            require(
                Counter(row["application_id"] for row in final["audits"])
                == Counter({case["application_id"]: 6 for case in cases}),
                "AUDIT_COUNT_MISMATCH",
            )
            require(
                all(
                    final["original_receipts"][case["application_id"]] == case["original_receipt"]
                    for case in cases
                ),
                "ORIGINAL_RECEIPT_CHANGED",
            )
            if mode == "query":
                require(
                    facts["shipments"] == before_provider["shipments"],
                    "QUERY_REPOSTED_OR_CHANGED_FACT",
                )
            rows = journal_rows(directory)
            iterations = [row for row in rows if row["type"] == "iteration"]
            transports = [row for row in rows if row["type"] == "transport"]
            totals = [row for row in rows if row["type"] == "totals"]
            require(len(totals) == count, "SQL_COUNTER_NOT_FLUSHED")
            require(
                len(transports) == COHORT_SIZE
                and all(
                    row["method"] == ("deliver" if mode == "outbox" else "query")
                    for row in transports
                ),
                "UNEXPECTED_TRANSPORT_REPETITION",
            )
            completed = [row for row in iterations if row["status"] == "SUCCEEDED"]
            claim_times = []
            ready_times = []
            finished = []
            if mode == "outbox":
                for row in final["attempts"]:
                    claim_times.append((utc(row["started_at"]) - gate).total_seconds() * 1000)
                    finished.append(utc(row["finished_at"]))
                request_created = {row["id"]: utc(row["created_at"]) for row in final["requests"]}
                outbox_request = {row["id"]: row["request_id"] for row in final["outboxes"]}
                ready_times = [
                    (
                        utc(row["started_at"]) - request_created[outbox_request[row["outbox_id"]]]
                    ).total_seconds()
                    * 1000
                    for row in final["attempts"]
                ]
            else:
                # Query task.updated_at is the durable finish; first run loop timestamp
                # precedes claim/check/query and is retained separately from DB timestamps.
                finished = [utc(row["updated_at"]) for row in final["tasks"]]
                claim_times = [
                    (utc(row["started_at"]) - gate).total_seconds() * 1000 for row in completed
                ]
                by_request = {row["request_id"]: utc(row["created_at"]) for row in final["tasks"]}
                ready_times = [
                    (utc(row["started_at"]) - by_request[row["request_id"]]).total_seconds() * 1000
                    for row in completed
                ]
            # A persisted finished_at is assigned inside the transaction. The
            # successful run_once return proves its commit completed; retain both.
            elapsed = (
                max(utc(row["finished_after_commit_at"]) for row in completed) - gate
            ).total_seconds()
            require(elapsed > 0, "INVALID_DURABLE_DURATION")
            report.update(
                status="PASSED",
                completed=COHORT_SIZE,
                failed=0,
                not_completed=[],
                gate_to_last_durable_success_ms=elapsed * 1000,
                gate_to_last_transaction_timestamp_ms=(max(finished) - gate).total_seconds() * 1000,
                throughput_tasks_per_second=COHORT_SIZE / elapsed,
                gate_to_claim_or_query_iteration_start_ms=summary(claim_times),
                created_to_claim_or_query_iteration_start_ms=summary(ready_times),
                query_start_clock_note=(
                    "query uses original run_once entry before claim; "
                    "no persistent task started_at exists"
                ),
                worker_iteration_ms=summary([row["duration_ms"] for row in completed]),
                external_attempt_ms=summary([row["duration_ms"] for row in transports]),
                worker_cursor_executions=sum(row["sql_calls"] for row in totals),
                worker_cursor_time_ms=sum(row["sql_duration_ms"] for row in totals),
                worker_locking_cursor_executions=sum(row["locking_sql_calls"] for row in totals),
                observed_worker_returncodes=[p.returncode for p in consumers],
                provider_facts=COHORT_SIZE,
                provider_events=COHORT_SIZE * 2,
            )
            cleanup_cases(sessions, cases, target)
            after = database_fingerprint(sessions)
            require(after == before, "UNRELATED_ROWS_OR_CLEANUP_CHANGED")
            report["cleanup"] = {
                "exact_owned_ids_deleted": True,
                "original_rows_and_counts_restored": True,
                "sequences_not_rewound": True,
            }
    except BaseException as failure:
        report["status"] = "FAILED"
        candidate = str(failure)
        report["error_code"] = (
            candidate if re.fullmatch(r"[A-Z_]+", candidate) else type(failure).__name__
        )
        report["data_retained_on_failure"] = True
        report["not_completed"] = [case["request_id"] for case in cases]
    finally:
        for process in consumers:
            stop(process)
        if provider is not None:
            stop(provider)
        if sampler is not None:
            report["sampling"] = sampler.finish()
        for handle in handles:
            handle.close()
        report["all_children_stopped"] = all(p.poll() is not None for p in consumers) and (
            provider is None or provider.poll() is not None
        )
        report["elapsed_total_seconds_including_setup_cleanup"] = time.monotonic() - started
        require(token not in canonical(report), "SECRET_IN_REPORT")
        write_json(directory / "result.json", report)
    return report


def measure(args, output):
    validate_target(args.database_url)
    require(args.database_url == TEST_URL, "EXACT_ISOLATED_TARGET_REQUIRED")
    with socket.socket() as probe:
        probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        probe.bind(("127.0.0.1", PORT))
    settings = Settings(
        _env_file=None,
        database_url=TEST_URL,
        allow_real_model_calls=False,
        warehouse_delivery_enabled=False,
        warehouse_callbacks_enabled=False,
        warehouse_simulation_enabled=False,
    )
    require(database_health(settings).status == "ready", "DATABASE_NOT_READY")
    sessions = session_factory(settings)
    started = time.monotonic()
    report = {
        "format": FORMAT,
        "status": "RUNNING",
        "phase": args.label,
        "database": "127.0.0.1:55434/aftercare_test",
        "schema": REVISION,
        "source": source_info(),
        "seed": args.seed,
        "matrix": {
            "modes": ["outbox", "query"],
            "workers": WORKERS,
            "rounds": ROUNDS,
            "tasks_per_cohort": COHORT_SIZE,
            "measured_tasks": 216,
        },
        "phase_work_budget_seconds": args.budget,
        "worker_loop_sleep_seconds": 0.5,
        "host": {
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
            "python": platform.python_version(),
        },
        "cohorts": [],
        "real_model_requests": 0,
        "limits": [
            "One shared-SKU synthetic local workload, not production QPS or linear scaling proof",
            "Observer and ps sampling overhead is present in both phases",
            (
                "One phase uses different generated IDs from the other; "
                "semantic specification and invariants match, not response bytes"
            ),
            (
                "RSS/CPU are sampled for worker/HTTP descendants and provider separately, "
                "excluding parent/DB"
            ),
            "Query clock uses run_once entry, not exact DB claim timestamp",
        ],
    }
    try:
        with sessions.kw["bind"].connect() as gate:
            require(
                gate.scalar(text("SELECT pg_try_advisory_lock(30102026,3)")),
                "D3_HARNESS_ALREADY_RUNNING",
            )
            gate.commit()
            try:
                quiescent(sessions)
                report["initial_database_hashes"] = database_fingerprint(sessions)
                with sessions() as session:
                    report["postgresql_version"] = session.scalar(text("SHOW server_version"))
                    report["database_settings"] = {
                        name: session.scalar(text("SHOW " + name))
                        for name in (
                            "shared_buffers",
                            "work_mem",
                            "effective_cache_size",
                            "fsync",
                            "synchronous_commit",
                            "max_connections",
                            "jit",
                            "track_io_timing",
                        )
                    }
                    report["initial_sequences"] = [
                        dict(row)
                        for row in session.execute(
                            text(
                                "SELECT schemaname,sequencename,last_value FROM pg_sequences "
                                "WHERE schemaname='public' ORDER BY sequencename"
                            )
                        ).mappings()
                    ]
                for round_number in range(1, ROUNDS + 1):
                    for mode in ("outbox", "query"):
                        for count in WORKERS:
                            result = run_cohort(
                                args, output, sessions, started, mode, count, round_number
                            )
                            report["cohorts"].append(result)
                            require(result["status"] == "PASSED", "COHORT_FAILED_STOP_MATRIX")
                            require(
                                time.monotonic() - started < args.budget, "PHASE_BUDGET_EXHAUSTED"
                            )
                report["final_database_hashes"] = database_fingerprint(sessions)
                require(
                    report["initial_database_hashes"] == report["final_database_hashes"],
                    "PHASE_DID_NOT_RESTORE_ORIGINAL_ROWS",
                )
                require(source_info() == report["source"], "SOURCE_CHANGED_DURING_MEASUREMENT")
                report["status"] = "PASSED"
            finally:
                gate.execute(text("SELECT pg_advisory_unlock(30102026,3)"))
                gate.commit()
    except BaseException as failure:
        report["status"] = "FAILED"
        candidate = str(failure)
        report["error_code"] = (
            candidate if re.fullmatch(r"[A-Z_]+", candidate) else type(failure).__name__
        )
    finally:
        sessions.kw["bind"].dispose()
        report["elapsed_seconds"] = time.monotonic() - started
        report["completed_cohorts"] = len([x for x in report["cohorts"] if x["status"] == "PASSED"])
        report["not_started_cohorts"] = 18 - len(report["cohorts"])
        write_json(output / "result.json", report)
    return {
        "status": report["status"],
        "result": str(output / "result.json"),
        "elapsed_seconds": report["elapsed_seconds"],
    }


def compare(args, output):
    before = json.loads(args.before.read_text())
    after = json.loads(args.after.read_text())
    require(before["status"] == after["status"] == "PASSED", "FAILED_MATRIX_NOT_COMPARABLE")
    for key in (
        "format",
        "database",
        "schema",
        "seed",
        "matrix",
        "worker_loop_sleep_seconds",
        "host",
        "postgresql_version",
        "database_settings",
    ):
        require(before[key] == after[key], "MATRIX_CONDITIONS_DIFFER")
    for key in ("harness_sha256", "requirements_sha256"):
        require(before["source"][key] == after["source"][key], "HARNESS_OR_DEPENDENCIES_CHANGED")
    rows = []
    for mode in ("outbox", "query"):
        for count in WORKERS:
            old = [x for x in before["cohorts"] if x["mode"] == mode and x["workers"] == count]
            new = [x for x in after["cohorts"] if x["mode"] == mode and x["workers"] == count]
            rows.append(
                {
                    "mode": mode,
                    "workers": count,
                    "rounds": ROUNDS,
                    "before_throughput_tps": summary(
                        [x["throughput_tasks_per_second"] for x in old]
                    ),
                    "after_throughput_tps": summary(
                        [x["throughput_tasks_per_second"] for x in new]
                    ),
                    "before_sql": [x["worker_cursor_executions"] for x in old],
                    "after_sql": [x["worker_cursor_executions"] for x in new],
                    "queue_note": (
                        "raw per-request wait and stage summaries retained in each cohort; "
                        "only three rounds, no capacity confidence interval"
                    ),
                }
            )
    report = {
        "status": "COMPARABLE",
        "before_sha256": hashlib.sha256(args.before.read_bytes()).hexdigest(),
        "after_sha256": hashlib.sha256(args.after.read_bytes()).hexdigest(),
        "rows": rows,
        "limits": (
            "D3 logging instrumentation overhead and 1/2/4 descriptive fixed-workload scaling; "
            "no causal throughput target or cloud-model claim"
        ),
    }
    write_json(output / "comparison.json", report)
    return {"status": "COMPARABLE", "comparison": str(output / "comparison.json")}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", default=TEST_URL)
    sub = parser.add_subparsers(dest="action", required=True)
    run = sub.add_parser("measure")
    run.add_argument("--label", choices=("before", "after"), required=True)
    run.add_argument("--output-dir", type=Path, required=True)
    run.add_argument("--seed", default="backend-D3-fixed-shared-sku-v1")
    run.add_argument("--budget", type=int, choices=range(120, 601), default=300)
    pair = sub.add_parser("compare")
    pair.add_argument("--before", type=Path, required=True)
    pair.add_argument("--after", type=Path, required=True)
    pair.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    require(os.environ.get("ALLOW_REAL_MODEL_CALLS") == "false", "EXPLICIT_OFFLINE_REQUIRED")
    require(os.environ.get("DATABASE_URL", TEST_URL) == TEST_URL, "DATABASE_ENVIRONMENT_MISMATCH")
    validate_target(args.database_url)
    require(args.database_url == TEST_URL, "EXACT_ISOLATED_TARGET_REQUIRED")
    os.umask(0o077)
    install_offline_guard()
    output = output_directory(args.output_dir)
    result = measure(args, output) if args.action == "measure" else compare(args, output)
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result["status"] in {"PASSED", "COMPARABLE"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
