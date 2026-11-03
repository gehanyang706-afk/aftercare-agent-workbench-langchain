"""D3 bounded real-process contention/fencing probes on isolated 55434/test.

No resets or recovery-protocol changes. New approved business facts and provider
records remain as evidence; credentials exist only in memory/stdin/child env.
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
from contextlib import ExitStack
from pathlib import Path

import httpx
from sqlalchemy import func, select

from aftercare.config import PROJECT_ROOT, Settings
from aftercare.offline import install_offline_guard
from aftercare.repositories.database import database_health, session_factory
from aftercare.repositories.models import (
    Inventory,
    ReconciliationTask,
    Reservation,
    WarehouseOutbox,
)
from aftercare.services.business import columns
from aftercare.services.demo import seed
from scripts.backend_baseline import TEST_URL, validate_target
from scripts.backend_c1_http_smoke import assert_outcome, enqueue, new_variant, require, wait_due
from scripts.backend_c2_http_smoke import C2Smoke, final, state, wait_marker
from scripts.backend_c_reliability_smoke import (
    authority_proof,
    clean_up,
    create_accounts,
    dispatch,
)

PRODUCT_URL = "http://127.0.0.1:8151"
PROVIDER_URL = "http://127.0.0.1:8152"
REQUIRED = [
    "four-workers-one-request",
    "not-started-kill-takeover",
    "accepted-kill-query-recovery",
    "stale-before-send",
    "stale-after-accept",
]

PROVIDER_CHILD = r"""
import json, os, sys, time
from pathlib import Path
from urllib.parse import unquote
import uvicorn
from aftercare.mock_warehouse.app import create_app
from aftercare.offline import install_offline_guard
install_offline_guard()
app = create_app(sys.argv[1], os.environ['MOCK_WAREHOUSE_TOKEN'])
ledger = Path(sys.argv[2])
@app.middleware('http')
async def record(request, call_next):
    effect = None
    if request.method == 'POST' and request.url.path == '/mock-warehouse/shipments':
        try: effect = json.loads(await request.body()).get('effect_key')
        except (ValueError, UnicodeError): pass
    elif (request.method == 'GET' and
            request.url.path.startswith('/mock-warehouse/shipments/by-key/')):
        effect = unquote(request.url.path.split('/by-key/', 1)[1])
    if isinstance(effect, str):
        with ledger.open('a') as file:
            file.write(json.dumps({'method':request.method, 'effect_key':effect,
                'received_ns':time.time_ns()}) + '\n'); file.flush()
    return await call_next(request)
uvicorn.run(app, host='127.0.0.1', port=8152, log_level='critical', access_log=False)
"""

COHORT_CHILD = r"""
import json, os, sys, time
from pathlib import Path
from aftercare.config import Settings
from aftercare.repositories.database import session_factory
from aftercare.services.outbox import OutboxService
from aftercare.services.recovery import RecoveryService
from aftercare.services.warehouse_attempt import BoundedWarehouseClient
from aftercare.services.worker_observation import json_worker_sink
from aftercare.workers.outbox_worker import OutboxWorker
from aftercare.workers.reconciliation_worker import ReconciliationWorker
def write(path, value):
    target=Path(path); temporary=Path(str(target)+'.tmp')
    with temporary.open('x') as file:
        json.dump(value,file); file.flush(); os.fsync(file.fileno())
    os.rename(temporary,target)
sessions=None;sink=None
try:
    settings=Settings(_env_file=None); sessions=session_factory(settings)
    owner=sys.argv[5]; kind=sys.argv[6]
    class ExpectedOutbox(OutboxService):
        def claim(self, owner):
            claim=super().claim(owner)
            if claim is not None and claim.request_id!=sys.argv[4]:
                raise RuntimeError('UNEXPECTED_CONCURRENT_REQUEST')
            return claim
    class ExpectedRecovery(RecoveryService):
        def claim(self, owner):
            claim=super().claim(owner)
            if claim is not None and claim.request_id!=sys.argv[4]:
                raise RuntimeError('UNEXPECTED_CONCURRENT_QUERY')
            return claim
    outbox=ExpectedOutbox(sessions)
    client=BoundedWarehouseClient(settings.warehouse_base_url,
        settings.warehouse_token.get_secret_value(),timeout=2)
    sink=json_worker_sink()
    worker=(OutboxWorker(outbox,client,owner=owner,observation=sink) if kind=='outbox' else
        ReconciliationWorker(ExpectedRecovery(sessions),client,outbox=outbox,
            owner=owner,observation=sink))
    write(sys.argv[2],{'status':'READY','pid':os.getpid(),'owner':owner,'kind':kind})
    deadline=time.monotonic()+30
    while not Path(sys.argv[1]).exists():
        if time.monotonic()>=deadline: raise RuntimeError('GATE_TIMEOUT')
        time.sleep(.02)
    result=worker.run_once();logging=sink.close(timeout=.25)
    write(sys.argv[3],{'status':'DONE','pid':os.getpid(),'owner':owner,'kind':kind,
        'result':result,'logging':logging})
except Exception as error:
    print(json.dumps({'error_code':type(error).__name__}),flush=True);sys.exit(1)
finally:
    if sink is not None:sink.close(timeout=.25)
    if sessions is not None:sessions.kw['bind'].dispose()
"""

PAUSE_CHILD = r"""
import json, os, sys, time
from pathlib import Path
from aftercare.config import Settings
from aftercare.repositories.database import session_factory
from aftercare.services.outbox import OutboxService
from aftercare.services.warehouse_attempt import BoundedWarehouseClient
from aftercare.services.worker_observation import json_worker_sink
from aftercare.workers.outbox_worker import OutboxWorker
def write(path,value):
    target=Path(path);temporary=Path(str(target)+'.tmp')
    with temporary.open('x') as file:
        json.dump(value,file);file.flush();os.fsync(file.fileno())
    os.rename(temporary,target)
sessions=None;sink=None
try:
    settings=Settings(_env_file=None);sessions=session_factory(settings)
    service=OutboxService(sessions,lease_seconds=3)
    client=BoundedWarehouseClient(settings.warehouse_base_url,
        settings.warehouse_token.get_secret_value(),timeout=2)
    mode=sys.argv[3];claim_box=[]
    def pause(claim,status,http_calls):
        write(sys.argv[1],{'status':status,'pid':os.getpid(),'request_id':claim.request_id,
            'outbox_id':claim.outbox_id,'generation':claim.generation,'owner':claim.owner,
            'effect_key':claim.payload['effect_key'],'application_id':claim.payload['application_id'],
            'http_calls':http_calls})
        deadline=time.monotonic()+60
        while not Path(sys.argv[4]).exists():
            if time.monotonic()>=deadline:raise RuntimeError('PAUSE_GO_TIMEOUT')
            time.sleep(.02)
    class PausedService:
        def expire_leases(self):return service.expire_leases()
        def claim(self,owner):
            claim=service.claim(owner)
            if claim is None or claim.request_id!=sys.argv[2]:raise RuntimeError('WRONG_CLAIM')
            claim_box.append(claim);return claim
        def begin_delivery(self,claim):
            if mode=='before':pause(claim,'CLAIMED_NOT_STARTED',0)
            return service.begin_delivery(claim)
        def finish(self,*args,**kwargs):return service.finish(*args,**kwargs)
    class PausedClient:
        def deliver(self,payload,request_hash):
            result=client.deliver(payload,request_hash)
            if mode=='accepted':
                if result.outcome!='SUCCEEDED':raise RuntimeError('PROVIDER_NOT_ACCEPTED')
                pause(claim_box[0],'ACCEPTED_BEFORE_FINISH',1)
            return result
    sink=json_worker_sink()
    result=OutboxWorker(PausedService(),PausedClient(),owner='d3-old-'+str(os.getpid()),
        observation=sink).run_once();logging=sink.close(timeout=.25)
    write(sys.argv[5],{'status':'RETURNED','pid':os.getpid(),'result':result,'logging':logging})
except Exception as error:
    print(json.dumps({'error_code':type(error).__name__}),flush=True);sys.exit(1)
finally:
    if sink is not None:sink.close(timeout=.25)
    if sessions is not None:sessions.kw['bind'].dispose()
"""


class D3Smoke(C2Smoke):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.groups = {}

    def started_group(self, child, label, **paths):
        # start_new_session makes this child's pid the group id. Only groups
        # created by this harness are ever signalled, including orphaned HTTP
        # descendants after their worker has already exited.
        self.children.append(child)
        item = {
            "label": label,
            "pid": child.pid,
            "process_group_id": child.pid,
            "returncode": None,
            **paths,
        }
        self.groups[child.pid] = label
        self.processes.append(item)
        self.record({"event": "started", **item})
        return child

    def spawn(self, arguments, label, *, cli=True):
        self.remaining()
        argv = [sys.executable, "-m", "aftercare", *arguments] if cli else arguments
        child = subprocess.Popen(
            argv,
            cwd=PROJECT_ROOT,
            env=self.env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        return self.started_group(child, label)

    def spawn_worker(self, code, arguments, label):
        self.remaining()
        stdout = self.output / (label + ".stdout")
        stderr = self.output / (label + ".stderr.jsonl")
        with stdout.open("xb") as out, stderr.open("xb") as err:
            child = subprocess.Popen(
                [sys.executable, "-c", code, *arguments],
                cwd=PROJECT_ROOT,
                env=self.env,
                stdout=out,
                stderr=err,
                start_new_session=True,
            )
        return self.started_group(child, label, stdout=str(stdout), worker_log=str(stderr))

    def signal_group(self, group_id, signum):
        require(group_id in self.groups and group_id != os.getpgrp(), "UNOWNED_PROCESS_GROUP")
        try:
            os.killpg(group_id, signum)
            return True
        except ProcessLookupError:
            return False

    def stop(self, child, *, kill=False):
        if child.pid not in self.groups:
            # Existing staff CLI helpers have no transport descendants and keep
            # their original bounded communicate/kill handling.
            return super().stop(child, kill=kill)
        self.signal_group(child.pid, signal.SIGKILL if kill else signal.SIGTERM)
        if child.poll() is None:
            try:
                child.wait(timeout=self.remaining(2, cleanup=True))
            except subprocess.TimeoutExpired:
                self.signal_group(child.pid, signal.SIGKILL)
                child.wait(timeout=self.remaining(2, cleanup=True))
        # A leader can exit before its Bounded HTTP child. Do not leave that
        # descendant running merely because Popen.poll() already returned.
        self.signal_group(child.pid, signal.SIGKILL)
        for item in self.processes:
            if item["pid"] == child.pid:
                item["returncode"] = child.returncode
        self.record(
            {
                "event": "stopped",
                "pid": child.pid,
                "process_group_id": child.pid,
                "returncode": child.returncode,
            }
        )

    def clean_groups(self):
        errors = []
        for group_id in self.groups:
            try:
                self.signal_group(group_id, signal.SIGKILL)
            except OSError:
                errors.append("PROCESS_GROUP_SIGNAL_FAILED")
        # Bounded separately from direct-child reaping. SIGKILL does not wait
        # for OS reaping of an orphan, so preserve any observed remainder.
        deadline = min(self.started + 179, time.monotonic() + 1)
        remaining = []
        while True:
            remaining = []
            for group_id in self.groups:
                try:
                    if self.signal_group(group_id, 0):
                        remaining.append(group_id)
                except OSError:
                    remaining.append(group_id)
            if not remaining or time.monotonic() >= deadline:
                break
            time.sleep(0.02)
        result = {
            "owned_groups": [
                {"id": identity, "label": label} for identity, label in self.groups.items()
            ],
            "remaining_groups": remaining,
            "stopped": not remaining,
            "errors": errors,
        }
        self.record({"event": "process_groups_cleanup", **result})
        return result


def cohort(smoke, output, case, label, *, kind="outbox", size=4):
    gate = output / (label + "-go.flag")
    children = []
    for index in range(size):
        name = f"{label}-{index}"
        ready, result = output / (name + "-ready.json"), output / (name + "-result.json")
        owner = "d3-" + name
        child = smoke.spawn_worker(
            COHORT_CHILD,
            [str(gate), str(ready), str(result), case["request_id"], owner, kind],
            name,
        )
        children.append((child, ready, result))
    markers = [wait_marker(smoke, child, ready) for child, ready, _ in children]
    with gate.open("x") as file:
        file.write("all workers ready\n")
    results = []
    for child, _, result in children:
        child.wait(timeout=smoke.remaining(15))
        smoke.stop(child)
        require(child.returncode == 0 and result.exists(), "COHORT_WORKER_FAILED")
        results.append(json.loads(result.read_text()))
    successes = [r for r in results if r["result"].get("request", {}).get("status") == "SUCCEEDED"]
    idle = [r for r in results if r["result"].get("status") == "IDLE"]
    require(len(successes) == 1 and len(idle) == size - 1, "COHORT_SINGLE_EFFECT_FAILED")
    return {"kind": kind, "ready_markers": markers, "results": results}


def pause(smoke, output, case, mode):
    marker = output / (case["name"] + "-paused.json")
    go = output / (case["name"] + "-resume.flag")
    result = output / (case["name"] + "-old-result.json")
    child = smoke.spawn_worker(
        PAUSE_CHILD,
        [str(marker), case["request_id"], mode, str(go), str(result)],
        case["name"] + "-old-worker",
    )
    case["pause_marker"] = wait_marker(smoke, child, marker)
    return child, go, result


def request_counts(output, effect_key):
    path = output / "provider-http.jsonl"
    if not path.exists():
        return {"POST": 0, "GET": 0}
    records = [json.loads(line) for line in path.read_text().splitlines()]
    return {
        method: sum(row["method"] == method and row["effect_key"] == effect_key for row in records)
        for method in ("POST", "GET")
    }


def post_count(output, effect_key):
    return request_counts(output, effect_key)["POST"]


def expired_claim(smoke, sessions, case, send_state):
    current = dispatch(sessions, case)
    require(
        current["status"] == "CLAIMED"
        and current["attempts"] == 1
        and current["attempt_records"][0]["send_state"] == send_state,
        "PAUSE_PHASE_NOT_COMMITTED",
    )
    case["paused_dispatch"] = current
    wait_due(smoke, current["lease_expires_at"])


def resume_old(smoke, child, go, path):
    with go.open("x") as file:
        file.write("resume old execution\n")
    child.wait(timeout=smoke.remaining(10))
    smoke.stop(child)
    require(child.returncode == 0 and path.exists(), "OLD_WORKER_DID_NOT_RETURN")
    result = json.loads(path.read_text())
    require(
        result["result"].get("status") == "LEASE_LOST"
        and result["result"].get("error_code") == "OUTBOX_LEASE_LOST",
        "OLD_EXECUTION_NOT_FENCED",
    )
    return result


def log_summary(output, cases):
    records = []
    files = sorted(output.glob("*.stderr.jsonl"))
    for path in files:
        for line in path.read_text().splitlines():
            row = json.loads(line)
            require(isinstance(row, dict), "WORKER_LOG_NOT_STRUCTURED")
            records.append(row)
    effects = {r.get("effect_key") for r in records if r.get("effect_key")}
    require({c["effect_key"] for c in cases} <= effects, "WORKER_LOG_CORRELATION_MISSING")
    lost = [r for r in records if r.get("error_code") == "OUTBOX_LEASE_LOST"]
    require(len(lost) >= 2, "WORKER_FENCING_LOG_MISSING")
    fences = []
    for case in cases:
        if not case.get("old_execution"):
            continue
        marker = case["pause_marker"]
        matches = [
            row
            for row in lost
            if row.get("owner") == marker["owner"]
            and row.get("generation") == marker["generation"]
            and row.get("effect_key") == case["effect_key"]
            and row.get("application_id") == case["application_id"]
            and row.get("shipment_request_id") == case["request_id"]
            and row.get("outbox_id") == marker["outbox_id"]
            and row.get("run_id")
        ]
        require(bool(matches), "WORKER_FENCING_IDENTITY_MISSING")
        fences.append({"scenario": case["name"], "records": matches})
    logging_stats = []
    for case in cases:
        for group in (case.get("workers"), case.get("replacement")):
            if group:
                logging_stats.extend(row["logging"] for row in group["results"])
        if case.get("old_execution"):
            logging_stats.append(case["old_execution"]["logging"])
    dropped = {
        key: sum(stats.get(key, 0) for stats in logging_stats)
        for key in ("dropped_full", "dropped_write_error", "dropped_closed", "pending")
    }
    return {
        "files": [str(p) for p in files],
        "record_count": len(records),
        "effect_keys": sorted(effects),
        "owners": sorted({r["owner"] for r in records if r.get("owner")}),
        "lost_execution_records": len(lost),
        "fenced_execution_identities": fences,
        "normal_exit_sink_stats": logging_stats,
        "normal_exit_log_loss": dropped,
        "sigkill_log_tail": "unknown; markers, database and provider records are authoritative",
    }


def interrupted(_signum, _frame):
    raise RuntimeError("SMOKE_INTERRUPTED")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir", type=Path, default=PROJECT_ROOT / "runtime/backend-D3/reliability-first"
    )
    args = parser.parse_args()
    validate_target(os.environ.get("DATABASE_URL", ""))
    require(
        os.environ.get("DATABASE_URL") == TEST_URL
        and os.environ.get("ALLOW_REAL_MODEL_CALLS") == "false",
        "ISOLATED_OFFLINE_ENVIRONMENT_REQUIRED",
    )
    output = args.output_dir.resolve()
    require(output.is_relative_to((PROJECT_ROOT / "runtime").resolve()), "OUTPUT_MUST_BE_RUNTIME")
    require(not output.exists(), "EVIDENCE_DIRECTORY_EXISTS")
    install_offline_guard()
    for port in (8151, 8152):
        with socket.socket() as probe:
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            probe.bind(("127.0.0.1", port))
    output.mkdir(parents=True, mode=0o700)
    token, secret = secrets.token_urlsafe(40), secrets.token_urlsafe(40)
    env = {
        **os.environ,
        "DATABASE_URL": TEST_URL,
        "ALLOW_REAL_MODEL_CALLS": "false",
        "PYTHONPATH": "src:.",
        "WAREHOUSE_DELIVERY_ENABLED": "true",
        "WAREHOUSE_SIMULATION_ENABLED": "false",
        "WAREHOUSE_CALLBACKS_ENABLED": "false",
        "DEMO_LOGIN_ENABLED": "true",
        "WAREHOUSE_BASE_URL": PROVIDER_URL,
        "WAREHOUSE_TOKEN": token,
        "MOCK_WAREHOUSE_TOKEN": token,
        "WAREHOUSE_TIMEOUT_SECONDS": "2",
        "OUTBOX_LEASE_SECONDS": "15",
        "OUTBOX_MAX_ATTEMPTS": "3",
    }
    env.pop("MOCK_WAREHOUSE_CALLBACK_BASE_URL", None)
    env.pop("MOCK_WAREHOUSE_CALLBACK_SECRET", None)
    smoke = D3Smoke(output, env, token, secret)
    settings = Settings(
        _env_file=None,
        database_url=TEST_URL,
        warehouse_delivery_enabled=False,
        warehouse_simulation_enabled=False,
        warehouse_callbacks_enabled=False,
    )
    sessions = session_factory(settings)
    accounts, usernames, credentials, failure = [], [], {}, None
    prefix, sqlite_path = "d3-rel-" + secrets.token_hex(6), output / "warehouse.sqlite3"
    report = {
        "status": "RUNNING",
        "scope": "five controlled real-process shipment concurrency/fencing probes",
        "database": "127.0.0.1:55434/aftercare_test",
        "real_model_requests": 0,
        "time_budget_seconds": 180,
        "required_scenarios": REQUIRED,
        "completed_scenarios": [],
        "scenarios": smoke.scenarios,
        "processes": smoke.processes,
    }
    previous_term = signal.signal(signal.SIGTERM, interrupted)
    try:
        require(database_health(settings).status == "ready", "DATABASE_NOT_READY")
        with sessions() as session:
            for model in (WarehouseOutbox, ReconciliationTask):
                require(
                    session.scalar(
                        select(func.count())
                        .select_from(model)
                        .where(model.status.in_(["PENDING", "CLAIMED"]))
                    )
                    == 0,
                    "EXISTING_ACTIVE_WORK_REFUSED",
                )
        seed(sessions)
        target = new_variant(sessions, prefix)
        with sessions() as session:
            original_stock = columns(session.get(Inventory, "tee-white-M"))
        create_accounts(smoke, sessions, prefix, usernames, accounts, credentials)
        with ExitStack() as stack:
            provider = stack.enter_context(
                httpx.Client(
                    base_url=PROVIDER_URL,
                    trust_env=False,
                    headers={"Authorization": "Bearer " + token},
                    timeout=5,
                )
            )
            clients = [
                stack.enter_context(httpx.Client(base_url=PRODUCT_URL, trust_env=False, timeout=5))
                for _ in range(3)
            ]
            owner, support, warehouse = clients
            child = smoke.spawn(
                [
                    sys.executable,
                    "-c",
                    PROVIDER_CHILD,
                    str(sqlite_path),
                    str(output / "provider-http.jsonl"),
                ],
                "provider",
                cli=False,
            )
            smoke.provider_pid = child.pid
            smoke.wait_ready(child, provider)
            child = smoke.spawn(
                ["serve", "--offline", "--demo-login", "--async-warehouse", "--port", "8151"],
                "product-http",
            )
            smoke.product_pid = child.pid
            smoke.wait_ready(child, owner)
            signed = smoke.request(
                owner, "POST", "/api/demo-sessions", json={"customer_id": "alice"}
            )
            owner.headers["X-CSRF-Token"] = signed["csrf_token"]
            for client, role in ((support, "support"), (warehouse, "warehouse")):
                signed = smoke.request(
                    client, "POST", "/api/staff/sessions", json=credentials[role]
                )
                client.headers["X-CSRF-Token"] = signed["csrf_token"]

            for index, name in enumerate(REQUIRED):
                case = enqueue(
                    smoke, sessions, prefix, name, target, clients, provider, sqlite_path
                )
                case["provider_http_before"] = request_counts(output, case["effect_key"])
                if index == 0:
                    case["workers"] = cohort(smoke, output, case, name)
                else:
                    accepted = index in (2, 4)
                    child, go, resumed = pause(
                        smoke, output, case, "accepted" if accepted else "before"
                    )
                    observed = state(smoke, sessions, clients, case, target, sqlite_path)
                    assert_outcome(observed, "PENDING", "CLAIMED", 5, "RESERVED", int(accepted))
                    require(
                        post_count(output, case["effect_key"]) == int(accepted), "PAUSE_POST_COUNT"
                    )
                    if index in (1, 2):
                        smoke.stop(child, kill=True)
                        require(child.returncode == -signal.SIGKILL, "PROBE_NOT_SIGKILLED")
                    expired_claim(smoke, sessions, case, "STARTED" if accepted else "NOT_STARTED")
                    case["replacement"] = cohort(
                        smoke,
                        output,
                        case,
                        name + "-replacement",
                        kind="query" if accepted else "outbox",
                        size=2 if accepted else 4,
                    )
                    completed = final(smoke, sessions, clients, case, target, sqlite_path)
                    assert_outcome(completed, "SUCCEEDED", "DELIVERED", 6, "CONSUMED", 1)
                    if index in (3, 4):
                        case["old_execution"] = resume_old(smoke, child, go, resumed)
                        require(
                            state(smoke, sessions, clients, case, target, sqlite_path) == completed,
                            "STALE_WORKER_CHANGED_FACTS",
                        )
                completed = final(smoke, sessions, clients, case, target, sqlite_path)
                assert_outcome(completed, "SUCCEEDED", "DELIVERED", 6, "CONSUMED", 1)
                counts = request_counts(output, case["effect_key"])
                require(counts["POST"] == 1, "SIDE_EFFECT_WAS_REPOSTED")
                require(
                    counts["GET"] - case["provider_http_before"]["GET"] == int(index in (2, 4)),
                    "QUERY_RECOVERY_HTTP_COUNT_MISMATCH",
                )
                require(completed["history_count"] == 6, "DUPLICATE_FULFILLMENT_AUDIT")
                case["post_count"] = 1
                case["provider_http_final"] = counts
                case["dispatch"] = dispatch(sessions, case)
                report["completed_scenarios"].append(name)
            with sessions() as session:
                require(
                    columns(session.get(Inventory, "tee-white-M")) == original_stock,
                    "ORIGINAL_SKU_CHANGED",
                )
                stock = columns(session.get(Inventory, target))
                consumed = session.scalar(
                    select(func.sum(Reservation.quantity)).where(
                        Reservation.application_id.in_(
                            [c["application_id"] for c in smoke.scenarios]
                        ),
                        Reservation.status == "CONSUMED",
                    )
                )
            require(
                stock["available"] == 15 and stock["reserved"] == 0 and consumed == 5,
                "FINAL_INVENTORY_MISMATCH",
            )
            report["shipment_authority"] = authority_proof(sessions, smoke.scenarios, accounts[1])
            report["worker_log_correlation"] = log_summary(output, smoke.scenarios)
            report.update(
                status="PASSED", synthetic_inventory_final=stock, consumed_quantity=consumed
            )
    except BaseException as error:
        failure = error
        message = str(error) if isinstance(error, RuntimeError) else type(error).__name__
        report.update(
            status="FAILED",
            error_code=message if re.fullmatch(r"[A-Z_]+", message) else type(error).__name__,
        )
    finally:
        # Finish the bounded teardown if a supervisor sends SIGTERM again.
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        cleanup = clean_up(smoke, sessions, usernames, accounts)
        group_cleanup = smoke.clean_groups()
        cleanup["process_groups"] = group_cleanup
        cleanup["errors"].extend(group_cleanup["errors"])
        if not group_cleanup["stopped"]:
            cleanup["errors"].append("PROCESS_GROUPS_STILL_ALIVE")
        if sqlite_path.exists():
            try:
                snapshot = output / "provider-final.sqlite3"
                with sqlite3.connect(sqlite_path.as_uri() + "?mode=ro", uri=True) as source:
                    with sqlite3.connect(snapshot) as destination:
                        source.backup(destination)
                        report["provider_final"] = {
                            "shipments": destination.execute(
                                "SELECT count(*) FROM shipments"
                            ).fetchone()[0],
                            "duplicate_effect_keys": destination.execute(
                                "SELECT count(*) FROM (SELECT effect_key FROM shipments "
                                "GROUP BY effect_key HAVING count(*)>1)"
                            ).fetchone()[0],
                            "integrity": destination.execute("PRAGMA integrity_check").fetchone()[
                                0
                            ],
                            "snapshot": str(snapshot),
                        }
                require(report["provider_final"]["integrity"] == "ok", "SNAPSHOT_INTEGRITY_FAILED")
                if report["status"] == "PASSED":
                    require(
                        report["provider_final"]["shipments"] == 5
                        and report["provider_final"]["duplicate_effect_keys"] == 0,
                        "FINAL_PROVIDER_FACT_MISMATCH",
                    )
            except Exception:
                cleanup["errors"].append("PROVIDER_SNAPSHOT_FAILED")
        sessions.kw["bind"].dispose()
        report.update(
            elapsed_seconds=round(time.monotonic() - smoke.started, 3),
            cleanup=cleanup,
            not_completed=[name for name in REQUIRED if name not in report["completed_scenarios"]],
        )
        if (
            cleanup["errors"]
            or not cleanup["children_stopped"]
            or not cleanup["temporary_staff_disabled"]
        ):
            report["status"] = "FAILED"
        encoded = json.dumps(report, ensure_ascii=False, indent=2)
        require(
            token not in encoded
            and secret not in encoded
            and all(c["password"] not in encoded for c in credentials.values()),
            "SECRET_IN_REPORT",
        )
        with (output / "reliability-first.json").open("x") as file:
            file.write(encoded + "\n")
        smoke.journal.close()
        signal.signal(signal.SIGTERM, previous_term)
    print(
        json.dumps(
            {
                "status": report["status"],
                "completed": report["completed_scenarios"],
                "elapsed_seconds": report["elapsed_seconds"],
                "report": str(output / "reliability-first.json"),
            }
        )
    )
    if failure is not None or report["status"] != "PASSED":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
