"""Five bounded real-process shipment fault probes, only on isolated 55434/test.

Keeps previous C1/C2 evidence untouched. New orders, provider SQLite facts and
audits remain; secrets exist only in memory, stdin or child environments.
Protocol replays are controlled fault probes, never automatic UNKNOWN retries.
"""

import argparse
import hashlib
import json
import os
import re
import secrets
import signal
import socket
import sqlite3
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, contextmanager
from pathlib import Path
from threading import Barrier

import httpx
from sqlalchemy import func, select

from aftercare.config import PROJECT_ROOT, Settings
from aftercare.domain.warehouse import shipment_request_hash
from aftercare.offline import install_offline_guard
from aftercare.repositories.database import database_health, session_factory
from aftercare.repositories.models import (
    FulfillmentAudit,
    Inventory,
    OutboxAttempt,
    ReconciliationTask,
    Reservation,
    ShipmentRequest,
    StaffAccount,
    WarehouseOutbox,
)
from aftercare.services.business import columns
from aftercare.services.demo import seed
from scripts.backend_baseline import TEST_URL, validate_target
from scripts.backend_c1_http_smoke import (
    assert_outcome,
    enqueue,
    fault,
    lookup,
    new_variant,
    require,
    wait_due,
)
from scripts.backend_c2_http_smoke import C2Smoke, final, manual, state, support_state, wait_marker

REQUIRED = [
    "true-socket-loss",
    "claimed-not-started-kill",
    "stale-delivery-owner",
    "armed-no-http-kill",
    "callback-ack-kill",
]
PRODUCT_URL = "http://127.0.0.1:8135"
PROVIDER_URL = "http://127.0.0.1:8143"
PROXY_URL = "http://127.0.0.1:8144"


PROXY_CHILD = r"""
import hashlib, http.client, json, os, socket, sys, threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from aftercare.domain.warehouse import ShipmentInput, ShipmentResult
root = Path(sys.argv[1]); lock = threading.Lock(); counts = {}; dropped = set()
def persist():
    path = root / 'proxy-counts.json'; temporary = root / 'proxy-counts.tmp'
    with temporary.open('w') as file:
        json.dump({'pid':os.getpid(), 'keys':counts}, file, sort_keys=True)
        file.flush(); os.fsync(file.fileno())
    os.replace(temporary, path)
def marker(value):
    path = root / 'socket-loss.json'; temporary = root / 'socket-loss.tmp'
    with temporary.open('x') as file:
        json.dump(value, file, sort_keys=True); file.flush(); os.fsync(file.fileno())
    os.rename(temporary, path)
class Handler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'
    def log_message(self, *args): pass
    def reply(self, status, raw):
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(raw)))
        self.send_header('Connection', 'close'); self.end_headers()
        self.wfile.write(raw); self.close_connection = True
    def do_GET(self):
        if self.path == '/health/ready': self.reply(200, b'{"status":"ready"}')
        else: self.reply(404, b'{"error_code":"PROXY_ROUTE_NOT_FOUND"}')
    def do_POST(self):
        connection = None; self.connection.settimeout(3)
        try:
            if self.path != '/mock-warehouse/shipments':
                self.reply(404, b'{"error_code":"PROXY_ROUTE_NOT_FOUND"}'); return
            length = int(self.headers.get('Content-Length', '0'))
            if not 0 < length <= 16384: raise ValueError('INVALID_LENGTH')
            raw = self.rfile.read(length)
            if len(raw) != length: raise ValueError('TRUNCATED_REQUEST')
            wire = ShipmentInput.model_validate_json(raw)
            with lock:
                item = counts.setdefault(wire.effect_key, {'post_count':0,
                    'response_bytes_forwarded':0, 'upstream_statuses':[], 'request_hashes':[]})
                item['post_count'] += 1
                item['request_hashes'].append(hashlib.sha256(raw).hexdigest()); persist()
            connection = http.client.HTTPConnection('127.0.0.1', 8143, timeout=2)
            connection.request('POST', self.path, body=raw, headers={
                'Content-Type':'application/json',
                'Authorization':self.headers.get('Authorization', '')})
            upstream = connection.getresponse(); payload = upstream.read(65537)
            if len(payload) > 65536: raise ValueError('UPSTREAM_TOO_LARGE')
            result = ShipmentResult.model_validate_json(payload) if upstream.status == 200 else None
            if result is not None and (result.effect_key != wire.effect_key or
                    result.application_id != wire.application_id or
                    result.request_hash != wire.request_hash): raise ValueError('BINDING_INVALID')
            with lock:
                counts[wire.effect_key]['upstream_statuses'].append(upstream.status)
                control = root / 'drop-control.json'
                lose = (control.exists() and json.loads(control.read_text())['effect_key'] ==
                    wire.effect_key and wire.effect_key not in dropped and result is not None)
                if lose:
                    dropped.add(wire.effect_key)
                    marker({'status':'UPSTREAM_200_CLIENT_RESPONSE_DROPPED', 'pid':os.getpid(),
                        'effect_key':wire.effect_key, 'request_hash':wire.request_hash,
                        'external_shipment_id':result.external_shipment_id,
                        'upstream_status':upstream.status, 'upstream_response_bytes':len(payload),
                        'client_response_bytes':0,
                        'post_count':counts[wire.effect_key]['post_count']})
                    persist()
                else:
                    counts[wire.effect_key]['response_bytes_forwarded'] += len(payload); persist()
            if lose:
                self.close_connection = True
                self.connection.shutdown(socket.SHUT_RDWR); self.connection.close()
                return
            self.reply(upstream.status, payload)
        except Exception:
            # No exception, Authorization header or environment is ever printed.
            self.close_connection = True
            try: self.connection.shutdown(socket.SHUT_RDWR)
            except OSError: pass
            self.connection.close()
        finally:
            if connection is not None: connection.close()
class Server(ThreadingHTTPServer):
    daemon_threads = True
with lock: persist()
Server(('127.0.0.1', 8144), Handler).serve_forever(poll_interval=.05)
"""


DELIVERY_CHILD = r"""
import json, os, sys, threading, time
from pathlib import Path
from aftercare.config import Settings
from aftercare.domain.contracts import BusinessError
from aftercare.repositories.database import session_factory
from aftercare.services.outbox import OutboxService
sessions = None
def write(path, value):
    destination = Path(path); temporary = Path(str(destination) + '.tmp')
    with temporary.open('x') as file:
        json.dump(value, file); file.flush(); os.fsync(file.fileno())
    os.rename(temporary, destination)
try:
    settings = Settings(_env_file=None); sessions = session_factory(settings)
    service = OutboxService(sessions, lease_seconds=3)
    claim = service.claim('c-hardening-' + str(os.getpid()))
    if claim is None or claim.request_id != sys.argv[2]: raise RuntimeError('WRONG_CLAIM')
    service.check_claim(claim)
    mode = sys.argv[3]
    if mode == 'armed': service.begin_delivery(claim)
    write(sys.argv[1], {'status':'STARTED_BEFORE_HTTP' if mode == 'armed' else
        'CLAIMED_NOT_STARTED', 'pid':os.getpid(), 'request_id':claim.request_id,
        'outbox_id':claim.outbox_id, 'generation':claim.generation, 'http_calls':0})
    if mode == 'stale':
        deadline = time.monotonic() + 60
        while not Path(sys.argv[4]).exists():
            if time.monotonic() >= deadline: raise RuntimeError('GO_TIMEOUT')
            time.sleep(.05)
        try: service.begin_delivery(claim)
        except BusinessError as error:
            if error.code != 'OUTBOX_LEASE_LOST': raise
            write(sys.argv[5], {'status':'LEASE_LOST', 'error_code':error.code,
                'pid':os.getpid(), 'generation':claim.generation, 'http_calls':0})
        else: raise RuntimeError('STALE_OWNER_ALLOWED_TO_SEND')
    else:
        threading.Event().wait(timeout=60)
        raise RuntimeError('PROBE_NOT_KILLED')
except Exception as error:
    print(json.dumps({'error_code':type(error).__name__}), flush=True); sys.exit(1)
finally:
    if sessions is not None: sessions.kw['bind'].dispose()
"""


CALLBACK_CHILD = r"""
import hashlib, json, os, sys, threading
from pathlib import Path
from aftercare.mock_warehouse.callbacks import CallbackSender
from aftercare.services.warehouse_attempt import BoundedCallbackClient
class PauseAfterRealAck:
    def __init__(self):
        self.real = BoundedCallbackClient(os.environ['MOCK_WAREHOUSE_CALLBACK_BASE_URL'],
            os.environ['MOCK_WAREHOUSE_CALLBACK_SECRET'], timeout=2)
    def send_event(self, event):
        if event['event_id'] != sys.argv[4] or event['sequence'] != 2:
            raise RuntimeError('WRONG_CALLBACK_CLAIM')
        result = self.real.send_event(event)
        if result.outcome != 'ACKNOWLEDGED': raise RuntimeError('NO_REAL_ACK')
        raw = json.dumps(event, sort_keys=True, ensure_ascii=False, separators=(',', ':')).encode()
        destination = Path(sys.argv[1]); temporary = Path(str(destination) + '.tmp')
        with temporary.open('x') as file:
            json.dump({'status':'REAL_ACK_BEFORE_EVENT_FINISH', 'pid':os.getpid(),
                'event_id':event['event_id'], 'sequence':event['sequence'],
                'event_hash':hashlib.sha256(raw).hexdigest(), 'outcome':result.outcome}, file)
            file.flush(); os.fsync(file.fileno())
        os.rename(temporary, destination)
        threading.Event().wait(timeout=60)
        raise RuntimeError('CALLBACK_PROBE_NOT_KILLED')
try:
    sender = CallbackSender(sys.argv[2], os.environ['MOCK_WAREHOUSE_CALLBACK_BASE_URL'],
        os.environ['MOCK_WAREHOUSE_CALLBACK_SECRET'], timeout=2, client=PauseAfterRealAck())
    result = sender.work_once(effect_key=sys.argv[3])
    print(json.dumps(result), flush=True)
    sys.exit(1)
except Exception as error:
    print(json.dumps({'error_code':type(error).__name__}), flush=True); sys.exit(1)
"""


@contextmanager
def child_environment(smoke, **changes):
    previous = smoke.env
    smoke.env = {**previous, **changes}
    try:
        yield
    finally:
        smoke.env = previous


def provider_start(smoke, provider, sqlite_path, label):
    child = smoke.spawn(
        ["mock-warehouse", "--port", "8143", "--state-path", str(sqlite_path), "--test-faults"],
        label,
    )
    smoke.provider_pid = child.pid
    smoke.wait_ready(child, provider)
    return child


def callback_cli(smoke, sqlite_path, effect_key, label):
    with child_environment(
        smoke,
        MOCK_WAREHOUSE_CALLBACK_BASE_URL=PRODUCT_URL,
        MOCK_WAREHOUSE_CALLBACK_SECRET=smoke.callback_secret,
    ):
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


def dispatch(sessions, case):
    with sessions() as session:
        row = session.scalar(
            select(WarehouseOutbox).where(WarehouseOutbox.request_id == case["request_id"])
        )
        require(row is not None, "OUTBOX_MISSING")
        attempts = list(
            session.scalars(
                select(OutboxAttempt)
                .where(OutboxAttempt.outbox_id == row.id)
                .order_by(OutboxAttempt.generation)
            )
        )
        return {
            "id": row.id,
            "status": row.status,
            "generation": row.generation,
            "attempts": row.attempts,
            "lease_expires_at": row.lease_expires_at.isoformat() if row.lease_expires_at else None,
            "attempt_records": [
                {
                    "generation": item.generation,
                    "send_state": item.send_state,
                    "send_started_at": item.send_started_at.isoformat()
                    if item.send_started_at
                    else None,
                    "outcome": item.outcome,
                    "error_code": item.error_code,
                }
                for item in attempts
            ],
        }


def provider_events(sqlite_path, effect_key):
    with sqlite3.connect(sqlite_path.as_uri() + "?mode=ro", uri=True, timeout=2) as connection:
        connection.row_factory = sqlite3.Row
        rows = connection.execute(
            "SELECT event_id,sequence,payload_json,delivery_status,attempts,generation,"
            "lease_expires_at FROM callback_events WHERE effect_key=? ORDER BY sequence",
            (effect_key,),
        ).fetchall()
        return [
            {
                "event_id": row["event_id"],
                "sequence": row["sequence"],
                "payload_hash": hashlib.sha256(row["payload_json"].encode()).hexdigest(),
                "status": row["delivery_status"],
                "attempts": row["attempts"],
                "generation": row["generation"],
                "lease_expires_at": row["lease_expires_at"],
            }
            for row in rows
        ]


def immutable_events(events):
    return [
        {key: event[key] for key in ("event_id", "sequence", "payload_hash")} for event in events
    ]


def proxy_count(output, effect_key):
    value = json.loads((output / "proxy-counts.json").read_text())
    return value["keys"].get(effect_key, {"post_count": 0})


def wait_epoch(smoke, due):
    deadline = time.monotonic() + smoke.remaining(8)
    while time.time() <= due:
        require(time.monotonic() < deadline, "CALLBACK_LEASE_WAIT_TIMEOUT")
        time.sleep(min(0.05, smoke.remaining(0.05)))


def start_delivery_probe(smoke, output, case, mode):
    marker = output / (case["name"] + "-claim.json")
    go, resumed = output / "old-delivery-go.flag", output / "old-delivery-finish.json"
    with child_environment(smoke, WAREHOUSE_BASE_URL=PROXY_URL):
        child = smoke.spawn(
            [
                sys.executable,
                "-c",
                DELIVERY_CHILD,
                str(marker),
                case["request_id"],
                mode,
                str(go),
                str(resumed),
            ],
            case["name"] + "-probe",
            cli=False,
        )
    case["claim_marker"] = wait_marker(smoke, child, marker)
    return child, go, resumed


def assert_claim(sessions, case, send_state):
    result = dispatch(sessions, case)
    require(
        result["status"] == "CLAIMED"
        and result["attempts"] == 1
        and result["generation"] == case["claim_marker"]["generation"]
        and len(result["attempt_records"]) == 1
        and result["attempt_records"][0]["send_state"] == send_state,
        "DISPATCH_PHASE_NOT_COMMITTED",
    )
    case["claimed_dispatch"] = result
    return result


def protocol_batch(smoke, wire, count):
    barrier = Barrier(count)

    def send():
        barrier.wait(timeout=smoke.remaining(6))
        with httpx.Client(
            base_url=PROVIDER_URL,
            headers={"Authorization": "Bearer " + smoke.token},
            trust_env=False,
            timeout=5,
        ) as client:
            return smoke.request(client, "POST", "/mock-warehouse/shipments", json=wire)

    with ThreadPoolExecutor(max_workers=count) as pool:
        futures = [pool.submit(send) for _ in range(count)]
        return [future.result(timeout=smoke.remaining(12)) for future in futures]


def create_accounts(smoke, sessions, prefix, usernames, accounts, credentials):
    for role in ("support", "warehouse"):
        value = {"username": prefix + "-" + role, "password": secrets.token_urlsafe(24)}
        credentials[role] = value
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


def authority_proof(sessions, scenarios, warehouse_id):
    expected = {"actor_id": warehouse_id, "role": "warehouse", "source": "server_session"}
    result = []
    with sessions() as session:
        for case in scenarios:
            request = session.get(ShipmentRequest, case["request_id"])
            require(
                request.triggered_by == expected
                and case["before_consumer"]["request"]["triggered_by"] == expected,
                "ORIGINAL_WAREHOUSE_TRIGGER_CHANGED",
            )
            audits = list(
                session.scalars(
                    select(FulfillmentAudit).where(
                        FulfillmentAudit.application_id == case["application_id"],
                        FulfillmentAudit.command == "confirm_shipment",
                    )
                )
            )
            require(
                len(audits) == (1 if request.status == "SUCCEEDED" else 0),
                "SHIPMENT_AUDIT_EFFECT_COUNT_MISMATCH",
            )
            for audit in audits:
                require(
                    audit.actor_id == "warehouse-outbox"
                    and audit.actor_source == "trusted_adapter"
                    and audit.details.get("triggered_by") == expected,
                    "SHIPMENT_RESULT_AUTHORITY_INVALID",
                )
                result.append(
                    {
                        "application_id": case["application_id"],
                        "command_id": audit.command_id,
                        "actor_id": audit.actor_id,
                        "actor_source": audit.actor_source,
                        "triggered_by": audit.details["triggered_by"],
                    }
                )
    return result


def clean_up(smoke, sessions, usernames, accounts):
    errors = []
    for child in reversed(smoke.children):
        if child.poll() is None:
            try:
                smoke.stop(child)
            except Exception:
                try:
                    child.kill()
                    child.wait(timeout=1)
                except Exception:
                    errors.append("CHILD_CLEANUP_FAILED")
    try:
        with sessions() as session:
            recovered = list(
                session.scalars(select(StaffAccount.id).where(StaffAccount.username.in_(usernames)))
            )
        accounts = sorted(set(accounts + recovered))
    except Exception:
        errors.append("TEMPORARY_STAFF_LOOKUP_FAILED")
    for staff_id in accounts:
        try:
            smoke.cli(
                ["staff", "disable", "--staff-id", staff_id],
                "disable-temporary-staff",
                cleanup=True,
            )
        except Exception:
            errors.append("STAFF_DISABLE_FAILED")
    disabled = False
    try:
        with sessions() as session:
            disabled = all(not session.get(StaffAccount, identity).active for identity in accounts)
        require(disabled, "TEMPORARY_STAFF_STILL_ACTIVE")
    except Exception:
        errors.append("STAFF_DISABLE_NOT_VERIFIED")
    # Includes cleanup CLI children, even if the main work budget was exhausted.
    for child in reversed(smoke.children):
        if child.poll() is None:
            try:
                child.kill()
                child.wait(timeout=1)
            except Exception:
                errors.append("FINAL_CHILD_REAP_FAILED")
    for item in smoke.processes:
        for child in smoke.children:
            if item["pid"] == child.pid:
                item["returncode"] = child.poll()
    return {
        "temporary_staff_disabled": disabled,
        "children_stopped": all(child.poll() is not None for child in smoke.children),
        "errors": errors,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=PROJECT_ROOT / "runtime/backend-C-hardening/http-first",
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
    install_offline_guard()
    for port in (8135, 8143, 8144):
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
        "WAREHOUSE_BASE_URL": PROVIDER_URL,
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
    prefix, sqlite_path = "c-hard-" + secrets.token_hex(6), output / "warehouse.sqlite3"
    report = {
        "status": "RUNNING",
        "scope": "approved exchange shipment; five local real-process probes",
        "database": "127.0.0.1:55434/aftercare_test",
        "real_model_requests": 0,
        "time_budget_seconds": 180,
        "required_scenarios": REQUIRED,
        "completed_scenarios": [],
        "scenarios": smoke.scenarios,
        "processes": smoke.processes,
        "limits": [
            "Controlled pause points, not arbitrary process instruction or network timing coverage",
            "Protocol replays repeat the approved wire; UNKNOWN workers never auto-POST",
            "One armed unsent ambiguity retains stock for manual review; four business successes",
        ],
    }
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
                    headers={"Authorization": "Bearer " + bearer},
                    timeout=5,
                    limits=httpx.Limits(max_keepalive_connections=0),
                )
            )
            proxy = stack.enter_context(
                httpx.Client(base_url=PROXY_URL, trust_env=False, timeout=5)
            )
            clients = [
                stack.enter_context(httpx.Client(base_url=PRODUCT_URL, trust_env=False, timeout=5))
                for _ in range(3)
            ]
            owner, support, warehouse = clients
            provider_process = provider_start(smoke, provider, sqlite_path, "provider-initial")
            proxy_process = smoke.spawn(
                [sys.executable, "-c", PROXY_CHILD, str(output)], "socket-loss-proxy", cli=False
            )
            smoke.wait_ready(proxy_process, proxy)
            app_process = smoke.spawn(
                ["serve", "--offline", "--demo-login", "--async-warehouse", "--port", "8135"],
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
            with (output / "drop-control.json").open("x") as file:
                json.dump({"effect_key": case["effect_key"]}, file)
            with child_environment(smoke, WAREHOUSE_BASE_URL=PROXY_URL):
                case["delivery"] = smoke.consumer("socket-loss-real-consumer")
            case["drop_marker"] = wait_marker(smoke, proxy_process, output / "socket-loss.json")
            case["unknown"] = state(smoke, sessions, clients, case, target, sqlite_path)
            assert_outcome(case["unknown"], "UNKNOWN", "BLOCKED", 5, "RESERVED", 1)
            require(
                case["unknown"]["outbox"]["last_error_code"] == "WAREHOUSE_TRANSPORT_UNKNOWN"
                and case["drop_marker"]["client_response_bytes"] == 0
                and case["drop_marker"]["upstream_status"] == 200,
                "NOT_TRUE_SOCKET_LOSS",
            )
            case["proxy"] = proxy_count(output, case["effect_key"])
            require(
                case["proxy"]["post_count"] == 1 and case["proxy"]["response_bytes_forwarded"] == 0,
                "SOCKET_LOSS_REPOSTED",
            )
            case["query"] = smoke.reconcile("socket-loss-reconcile-original-key")
            recovered = final(smoke, sessions, clients, case, target, sqlite_path)
            assert_outcome(recovered, "SUCCEEDED", "DELIVERED", 6, "CONSUMED", 1)
            original_external = lookup(smoke, provider, case["effect_key"])
            events = immutable_events(provider_events(sqlite_path, case["effect_key"]))
            require(len(events) == 2, "INITIAL_PROVIDER_EVENTS_MISSING")
            with sessions() as session:
                request = session.get(ShipmentRequest, case["request_id"])
                wire = {**request.payload, "request_hash": request.request_hash}
            first_replays = protocol_batch(smoke, wire, 4)
            old_pid = provider_process.pid
            smoke.stop(provider_process)
            provider_process = provider_start(
                smoke, provider, sqlite_path, "provider-replay-restarted"
            )
            require(old_pid != provider_process.pid, "PROVIDER_NOT_RESTARTED")
            later_replays = protocol_batch(smoke, wire, 6)
            require(
                all(item == original_external for item in first_replays + later_replays),
                "REPLAY_CHANGED_PROVIDER_RESULT",
            )
            changed = {key: value for key, value in wire.items() if key != "request_hash"}
            changed["quantity"] = 2
            conflict = smoke.request(
                provider,
                "POST",
                "/mock-warehouse/shipments",
                status=409,
                json={**changed, "request_hash": shipment_request_hash(**changed)},
            )
            require(conflict.get("code") == "IDEMPOTENCY_CONFLICT", "PROTOCOL_CONFLICT_NOT_REFUSED")
            require(
                immutable_events(provider_events(sqlite_path, case["effect_key"])) == events,
                "REPLAYS_CHANGED_IMMUTABLE_EVENTS",
            )
            require(
                state(smoke, sessions, clients, case, target, sqlite_path) == recovered,
                "PROTOCOL_REPLAY_CHANGED_BUSINESS",
            )
            case["protocol_replays"] = {
                "scope": "controlled exact approved wire replays, not UNKNOWN worker retries",
                "before_restart": 4,
                "after_restart": 6,
                "total": 10,
                "first_provider_pid": old_pid,
                "restarted_provider_pid": provider_process.pid,
                "external_shipment_id": original_external["external_shipment_id"],
                "request_hash": wire["request_hash"],
                "events": events,
                "different_payload_status": 409,
                "provider_facts": 1,
            }
            case["dispatch"] = dispatch(sessions, case)
            report["completed_scenarios"].append(case["name"])

            for name, mode in ((REQUIRED[1], "claim"), (REQUIRED[2], "stale")):
                case = enqueue(
                    smoke, sessions, prefix, name, target, clients, provider, sqlite_path
                )
                child, go, resumed = start_delivery_probe(smoke, output, case, mode)
                claimed = assert_claim(sessions, case, "NOT_STARTED")
                require(
                    proxy_count(output, case["effect_key"])["post_count"] == 0,
                    "NOT_STARTED_ALREADY_SENT_HTTP",
                )
                if mode == "claim":
                    smoke.stop(child, kill=True)
                    require(child.returncode == -signal.SIGKILL, "CLAIM_PROBE_NOT_SIGKILLED")
                wait_due(smoke, claimed["lease_expires_at"])
                with child_environment(smoke, WAREHOUSE_BASE_URL=PROXY_URL):
                    case["replacement_consumer"] = smoke.consumer(name + "-replacement")
                recovered = final(smoke, sessions, clients, case, target, sqlite_path)
                assert_outcome(recovered, "SUCCEEDED", "DELIVERED", 6, "CONSUMED", 1)
                case["dispatch"] = dispatch(sessions, case)
                require(
                    case["dispatch"]["attempts"] == 2
                    and [item["send_state"] for item in case["dispatch"]["attempt_records"]]
                    == ["NOT_STARTED", "STARTED"]
                    and case["dispatch"]["attempt_records"][0]["error_code"] == "HTTP_NOT_STARTED",
                    "NOT_STARTED_WAS_NOT_SAFELY_REQUEUED",
                )
                if mode == "stale":
                    with go.open("x") as file:
                        file.write("resume old sender before begin_delivery\n")
                    child.wait(timeout=smoke.remaining(10))
                    smoke.stop(child)
                    require(child.returncode == 0 and resumed.exists(), "STALE_SENDER_PROBE_FAILED")
                    case["old_begin_delivery"] = json.loads(resumed.read_text())
                    require(
                        case["old_begin_delivery"]["error_code"] == "OUTBOX_LEASE_LOST"
                        and case["old_begin_delivery"]["http_calls"] == 0,
                        "STALE_SENDER_WAS_NOT_FENCED",
                    )
                    require(
                        state(smoke, sessions, clients, case, target, sqlite_path) == recovered,
                        "STALE_SENDER_CHANGED_RESULT",
                    )
                case["proxy"] = proxy_count(output, case["effect_key"])
                require(case["proxy"]["post_count"] == 1, "SAFE_TAKEOVER_DUPLICATED_HTTP")
                report["completed_scenarios"].append(case["name"])

            case = enqueue(
                smoke, sessions, prefix, REQUIRED[3], target, clients, provider, sqlite_path
            )
            child, _, _ = start_delivery_probe(smoke, output, case, "armed")
            claimed = assert_claim(sessions, case, "STARTED")
            require(
                proxy_count(output, case["effect_key"])["post_count"] == 0,
                "ARMED_PROBE_ALREADY_SENT_HTTP",
            )
            smoke.stop(child, kill=True)
            require(child.returncode == -signal.SIGKILL, "ARMED_PROBE_NOT_SIGKILLED")
            wait_due(smoke, claimed["lease_expires_at"])
            case["queries"] = []
            for attempt in range(3):
                observed = state(smoke, sessions, clients, case, target, sqlite_path)
                task = observed["reconciliation"]
                if task.get("next_attempt_at"):
                    wait_due(smoke, task["next_attempt_at"])
                result = smoke.reconcile(f"armed-unknown-real-404-{attempt + 1}")
                require(
                    result["reconciliation"]["last_error_code"] == "WAREHOUSE_QUERY_NOT_FOUND",
                    "QUERY_WAS_NOT_REAL_NOT_FOUND",
                )
                case["queries"].append(result)
            exhausted = final(smoke, sessions, clients, case, target, sqlite_path)
            assert_outcome(exhausted, "MANUAL_REVIEW", "DEAD", 5, "RESERVED", 0)
            require(
                exhausted["reconciliation"]["status"] == "EXHAUSTED"
                and exhausted["reconciliation"]["attempts"] == 3,
                "NOT_FOUND_QUERY_BUDGET_NOT_EXHAUSTED",
            )
            generation = support_state(smoke, support, case)["management"]["outbox_generation"]
            case["redrive_denied"] = manual(
                smoke,
                support,
                exhausted["outbox"]["id"],
                "redrive",
                generation,
                key="armed-404-does-not-authorize-post",
                status=409,
            )
            require(
                case["redrive_denied"]["error_code"] == "REDRIVE_NOT_ALLOWED",
                "NOT_FOUND_AUTHORIZED_UNSAFE_POST",
            )
            case["budget_probe"] = smoke.reconcile("404-budget-must-remain-closed")
            require(
                case["budget_probe"]["status"] == "IDLE"
                and state(smoke, sessions, clients, case, target, sqlite_path) == exhausted,
                "NOT_FOUND_REOPENED_QUERY_BUDGET",
            )
            case["proxy"] = proxy_count(output, case["effect_key"])
            require(case["proxy"]["post_count"] == 0, "ARMED_AMBIGUITY_WAS_REPOSTED")
            case["dispatch"] = dispatch(sessions, case)
            report["completed_scenarios"].append(case["name"])

            case = enqueue(
                smoke, sessions, prefix, REQUIRED[4], target, clients, provider, sqlite_path
            )
            fault(smoke, provider, case, "error_after_accept_once")
            case["delivery"] = smoke.consumer("callback-case-accepted-http-500")
            case["unknown"] = state(smoke, sessions, clients, case, target, sqlite_path)
            assert_outcome(case["unknown"], "UNKNOWN", "BLOCKED", 5, "RESERVED", 1)
            events_before = provider_events(sqlite_path, case["effect_key"])
            require(len(events_before) == 2, "CALLBACK_EVENTS_NOT_CREATED")
            case["sequence_one_sender"] = callback_cli(
                smoke, sqlite_path, case["effect_key"], "callback-real-sequence-one"
            )
            require(
                case["sequence_one_sender"]["sequence"] == 1
                and case["sequence_one_sender"]["status"] == "DELIVERED",
                "SEQUENCE_ONE_NOT_ACKNOWLEDGED",
            )
            marker = output / "callback-ack-before-finish.json"
            with child_environment(
                smoke,
                MOCK_WAREHOUSE_CALLBACK_BASE_URL=PRODUCT_URL,
                MOCK_WAREHOUSE_CALLBACK_SECRET=callback_secret,
            ):
                child = smoke.spawn(
                    [
                        sys.executable,
                        "-c",
                        CALLBACK_CHILD,
                        str(marker),
                        str(sqlite_path),
                        case["effect_key"],
                        events_before[1]["event_id"],
                    ],
                    "callback-killed-after-real-ack",
                    cli=False,
                )
            case["ack_marker"] = wait_marker(smoke, child, marker)
            accepted = state(smoke, sessions, clients, case, target, sqlite_path)
            assert_outcome(accepted, "SUCCEEDED", "DELIVERED", 6, "CONSUMED", 1)
            require(
                len(accepted["inbox"]) == 2 and accepted["history_count"] == 6,
                "ACK_DID_NOT_COMMIT_EXACTLY_ONE_BUSINESS_EFFECT",
            )
            claimed_events = provider_events(sqlite_path, case["effect_key"])
            second = claimed_events[1]
            require(
                second["status"] == "CLAIMED"
                and second["attempts"] == 1
                and second["payload_hash"] == case["ack_marker"]["event_hash"],
                "CALLBACK_FINISHED_BEFORE_KILL",
            )
            case["events_before_kill"] = claimed_events
            smoke.stop(child, kill=True)
            require(child.returncode == -signal.SIGKILL, "CALLBACK_NOT_SIGKILLED")
            wait_epoch(smoke, second["lease_expires_at"])
            old_pid = provider_process.pid
            smoke.stop(provider_process)
            provider_process = provider_start(
                smoke, provider, sqlite_path, "provider-after-callback-kill"
            )
            case["replacement_sender"] = callback_cli(
                smoke, sqlite_path, case["effect_key"], "callback-lease-replacement"
            )
            require(
                case["replacement_sender"]["event_id"] == second["event_id"]
                and case["replacement_sender"]["status"] == "DELIVERED"
                and case["replacement_sender"]["attempts"] == 2,
                "CALLBACK_LEASE_NOT_RECOVERED",
            )
            events_after = provider_events(sqlite_path, case["effect_key"])
            require(
                immutable_events(events_after) == immutable_events(events_before)
                and all(item["status"] == "DELIVERED" for item in events_after),
                "CALLBACK_RESTART_CHANGED_EVENT_FACTS",
            )
            case["events_after_replacement"] = events_after
            case["provider_restart"] = {"first_pid": old_pid, "restarted_pid": provider_process.pid}
            require(old_pid != provider_process.pid, "CALLBACK_PROVIDER_NOT_RESTARTED")
            require(
                final(smoke, sessions, clients, case, target, sqlite_path) == accepted,
                "REPLAYED_CALLBACK_DUPLICATED_BUSINESS_EFFECT",
            )
            case["dispatch"] = dispatch(sessions, case)
            report["completed_scenarios"].append(case["name"])

            with sessions() as session:
                require(
                    columns(session.get(Inventory, "tee-white-M")) == original_stock,
                    "ORIGINAL_SKU_STOCK_CHANGED",
                )
                stock = columns(session.get(Inventory, target))
                consumed = (
                    session.scalar(
                        select(func.sum(Reservation.quantity)).where(
                            Reservation.application_id.in_(
                                [item["application_id"] for item in smoke.scenarios]
                            ),
                            Reservation.status == "CONSUMED",
                        )
                    )
                    or 0
                )
            require(
                stock["available"] == 15 and stock["reserved"] == 1 and consumed == 4,
                "FINAL_SYNTHETIC_INVENTORY_MISMATCH",
            )
            require(len(report["completed_scenarios"]) == 5, "MATRIX_INCOMPLETE")
            report["shipment_authority"] = authority_proof(sessions, smoke.scenarios, accounts[1])
            report.update(
                status="PASSED",
                synthetic_inventory_final=stock,
                consumed_quantity=consumed,
                business_outcomes={"SUCCEEDED_v6": 4, "MANUAL_REVIEW_v5": 1},
            )
    except BaseException as error:
        failure = error
        candidate = str(error) if isinstance(error, RuntimeError) else type(error).__name__
        report.update(
            status="FAILED",
            error_code=candidate if re.fullmatch(r"[A-Z_]+", candidate) else type(error).__name__,
        )
    finally:
        cleanup = clean_up(smoke, sessions, usernames, accounts)
        sessions.kw["bind"].dispose()
        report.update(
            elapsed_seconds=round(time.monotonic() - smoke.started, 3),
            cleanup=cleanup,
            not_completed=[name for name in REQUIRED if name not in report["completed_scenarios"]],
            evidence_retained={"provider_sqlite": str(sqlite_path), "business_audit": True},
        )
        if (
            cleanup["errors"]
            or not cleanup["children_stopped"]
            or not cleanup["temporary_staff_disabled"]
        ):
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
                "report": str(output / "http-first.json"),
                "completed": report["completed_scenarios"],
                "elapsed_seconds": report["elapsed_seconds"],
            }
        )
    )
    if failure is not None or report["status"] != "PASSED":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
