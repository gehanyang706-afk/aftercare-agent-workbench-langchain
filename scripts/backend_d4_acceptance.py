"""Stepwise D4 HTTP/persistence acceptance on the dedicated Docker test stack.

The caller controls container stop/restart and migrations between steps. The
provider-only Docker exec transport reaches its existing container loopback; it
cannot control container lifecycle. No migration, truncate, drop or deletion is
performed here. Each step needs a new runtime directory; secrets stay off argv.
"""

import argparse
import base64
import importlib.util
import json
import os
import re
import secrets
import subprocess
import time
from contextlib import ExitStack, contextmanager
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import quote

import httpx
from sqlalchemy import MetaData, select, text
from sqlalchemy.engine import make_url

from aftercare.config import PROJECT_ROOT, Settings
from aftercare.offline import install_offline_guard
from aftercare.repositories.database import database_health, session_factory
from aftercare.repositories.models import (
    Application,
    Inventory,
    Operation,
    OrderLine,
    Reservation,
    ShipmentRequest,
    StaffAccount,
)
from aftercare.services.business import columns, digest
from aftercare.services.demo import seed
from aftercare.services.staff_auth import StaffAuthService
from scripts.backend_c1_http_smoke import command, new_case, new_variant, require

DATABASE = "postgresql+psycopg://aftercare:aftercare_local_only@127.0.0.1:55436/aftercare_test"
PRODUCT_URL = "http://127.0.0.1:8138"
PROVIDER_URL = "http://127.0.0.1:8152"
PROVIDER_CONTAINER = "aftercare-d4-test-warehouse-1"
REVISION = "0011_dispatch_intent"
FORMAT = "backend-d4-acceptance-v1"
CASE_NAME = "docker-recovery"

PROVIDER_EXEC_CODE = r"""
import base64, json, sys, urllib.error, urllib.request
class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None
try:
    packet = json.load(sys.stdin)
    method, path = packet['method'], packet['path']
    assert (method == 'POST' and path == '/mock-warehouse/shipments' or
        method == 'GET' and path.startswith('/mock-warehouse/shipments/by-key/')
        and len(path) > len('/mock-warehouse/shipments/by-key/'))
    assert len(path) <= 1024 and not any(c in path for c in '\r\n?#')
    authorization = packet['authorization']
    assert authorization.startswith('Bearer ') and not any(c in authorization for c in '\r\n')
    body = base64.b64decode(packet['body_b64'], validate=True)
    assert len(body) <= 16384
    request = urllib.request.Request('http://127.0.0.1:8152' + path,
        data=body if method == 'POST' else None, method=method,
        headers={'Authorization': authorization, 'Content-Type': 'application/json'})
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    try:
        response = opener.open(request, timeout=min(5, packet['http_timeout']))
    except urllib.error.HTTPError as error:
        response = error
    with response:
        body = response.read(65537)
        assert len(body) <= 65536
        headers = {key:value for key,value in response.headers.items()
            if key.lower() in ('content-type','x-warehouse-sequence')}
        print(json.dumps({'status':response.code, 'headers':headers,
            'body_b64':base64.b64encode(body).decode('ascii')}), flush=True)
except Exception:
    print(json.dumps({'error_code':'D4_PROVIDER_HTTP_UNAVAILABLE'}), flush=True)
    sys.exit(1)
"""


class DockerLoopbackTransport(httpx.BaseTransport):
    """Acceptance-only provider I/O, without widening the product network guard."""

    def __init__(self, *, run=None):
        self._run = run or subprocess.run

    def handle_request(self, request):
        raw_path = request.url.raw_path.decode("ascii")
        allowed_operation = (
            request.method == "POST"
            and raw_path == "/mock-warehouse/shipments"
            or request.method == "GET"
            and raw_path.startswith("/mock-warehouse/shipments/by-key/")
            and len(raw_path) > len("/mock-warehouse/shipments/by-key/")
        )
        authorization = request.headers.get("authorization", "")
        valid = (
            request.url.scheme == "http"
            and request.url.host == "127.0.0.1"
            and request.url.port == 8152
            and not request.url.username
            and not request.url.password
            and not request.url.query
            and not request.url.fragment
            and len(raw_path) <= 1024
            and not any(character in raw_path for character in "\r\n?#")
            and authorization.startswith("Bearer ")
            and 39 <= len(authorization) <= 263
            and not any(character in authorization for character in "\r\n")
            and allowed_operation
        )
        if not valid:
            raise httpx.TransportError("D4_PROVIDER_REQUEST_DENIED", request=request)
        body = request.read()
        if len(body) > 16384:
            raise httpx.TransportError("D4_PROVIDER_REQUEST_DENIED", request=request)
        requested_timeout = request.extensions.get("timeout", {}).get("read")
        http_timeout = (
            min(5.0, max(0.1, requested_timeout))
            if type(requested_timeout) in {int, float}
            else 5.0
        )
        packet = {
            "method": request.method,
            "path": raw_path,
            "authorization": authorization,
            "body_b64": base64.b64encode(body).decode("ascii"),
            "http_timeout": http_timeout,
        }
        # Preserve Docker's existing local context while excluding provider/model
        # credentials from the host subprocess environment. Request secrets exist
        # only on the exec stdin channel, never in argv or new container env flags.
        env = {
            key: value
            for key, value in os.environ.items()
            if key
            in {
                "PATH",
                "HOME",
                "DOCKER_HOST",
                "DOCKER_CONTEXT",
                "DOCKER_CONFIG",
                "XDG_RUNTIME_DIR",
                "LANG",
                "LC_ALL",
                "TMPDIR",
                "USER",
                "LOGNAME",
            }
        }
        argv = ["docker", "exec", "-i", PROVIDER_CONTAINER, "python", "-c", PROVIDER_EXEC_CODE]
        try:
            completed = self._run(
                argv,
                input=json.dumps(packet).encode(),
                capture_output=True,
                env=env,
                timeout=http_timeout + 2,
                check=False,
            )
        except subprocess.TimeoutExpired:
            raise httpx.ReadTimeout("D4_PROVIDER_DOCKER_TIMEOUT", request=request) from None
        except OSError:
            raise httpx.TransportError("D4_PROVIDER_DOCKER_EXEC_FAILED", request=request) from None
        if completed.returncode != 0:
            raise httpx.TransportError("D4_PROVIDER_DOCKER_EXEC_FAILED", request=request)
        try:
            result = json.loads(completed.stdout)
            status = result["status"]
            content = base64.b64decode(result["body_b64"], validate=True)
            if type(status) is not int or not 100 <= status <= 599 or len(content) > 65536:
                raise ValueError("INVALID_PROVIDER_EXEC_RESPONSE")
            return httpx.Response(status, headers=result["headers"], content=content)
        except (KeyError, TypeError, ValueError):
            raise httpx.TransportError(
                "D4_PROVIDER_DOCKER_RESPONSE_INVALID", request=request
            ) from None


def validate_environment(environ):
    require(environ.get("ALLOW_REAL_MODEL_CALLS") == "false", "EXPLICIT_OFFLINE_REQUIRED")
    require(environ.get("DATABASE_URL") == DATABASE, "EXACT_D4_TEST_DATABASE_REQUIRED")
    url = make_url(environ["DATABASE_URL"])
    require(
        (url.host, url.port, url.database, url.username)
        == ("127.0.0.1", 55436, "aftercare_test", "aftercare")
        and not url.query,
        "EXACT_D4_TEST_DATABASE_REQUIRED",
    )


def runtime_path(value):
    path = Path(value).resolve()
    require(path.is_relative_to((PROJECT_ROOT / "runtime").resolve()), "OUTPUT_MUST_BE_RUNTIME")
    return path


def encode(value):
    def convert(item):
        if isinstance(item, (datetime, date)):
            return item.isoformat()
        raise TypeError("UNSUPPORTED_EVIDENCE_VALUE")

    return json.dumps(value, ensure_ascii=False, indent=2, default=convert)


def write_new(path, value):
    with path.open("x", encoding="utf-8") as file:
        os.chmod(path, 0o600)
        file.write(encode(value) + "\n")
        file.flush()
        os.fsync(file.fileno())


def load_state(path, *, kind="normal"):
    value = json.loads(runtime_path(path).read_text())
    require(
        value.get("format") == FORMAT
        and value.get("kind") == kind
        and value.get("database") == "127.0.0.1:55436/aftercare_test",
        "STATE_TARGET_OR_FORMAT_MISMATCH",
    )
    require(re.fullmatch(r"d4-[a-f0-9]{12}", value.get("prefix", "")), "STATE_PREFIX_INVALID")
    if kind == "normal":
        require(
            value["target"] == value["prefix"] + "-target"
            and value["case"]["order_id"] == value["prefix"] + "-" + CASE_NAME
            and value["case"]["name"] == CASE_NAME,
            "STATE_OWNERSHIP_INVALID",
        )
    return value


class Probe:
    def __init__(self, token):
        self.started, self.token = time.monotonic(), token
        self.events, self.accounts, self.secrets = [], [], [token]

    def remaining(self, cap=5):
        require(time.monotonic() - self.started < 90, "STEP_TIME_BUDGET_EXHAUSTED")
        return min(cap, max(0.1, 90 - (time.monotonic() - self.started)))

    def request(self, client, method, path, *, status=200, **kwargs):
        response = client.request(method, path, timeout=self.remaining(), **kwargs)
        require(response.status_code == status, "HTTP_UNEXPECTED_STATUS")
        return response.json()

    @contextmanager
    def clients(self, sessions, prefix, *, warehouse_id=None, prepare=False):
        auth = StaffAuthService(sessions)
        created, credentials = [], {}
        try:
            roles = ("support", "warehouse") if prepare else ("warehouse",) if warehouse_id else ()
            for role in roles:
                password = secrets.token_urlsafe(32)
                self.secrets.append(password)
                if warehouse_id:
                    with sessions() as session:
                        account = session.get(StaffAccount, warehouse_id)
                        require(
                            account is not None
                            and account.username == prefix + "-warehouse"
                            and account.role == "warehouse"
                            and not account.active,
                            "ORIGINAL_TEMPORARY_ACTOR_MISMATCH",
                        )
                    result = auth.update_account(warehouse_id, active=True, password=password)
                else:
                    result = auth.create_account(prefix + "-" + role, password, role)
                created.append(result["id"])
                self.accounts.append({"id": result["id"], "role": role})
                credentials[role] = {"username": result["username"], "password": password}
            with ExitStack() as stack:
                customer = stack.enter_context(httpx.Client(base_url=PRODUCT_URL, trust_env=False))
                signed = self.request(
                    customer, "POST", "/api/demo-sessions", json={"customer_id": "alice"}
                )
                self.secrets.append(signed["csrf_token"])
                customer.headers["X-CSRF-Token"] = signed["csrf_token"]
                clients = {"customer": customer}
                for role, credential in credentials.items():
                    client = stack.enter_context(
                        httpx.Client(base_url=PRODUCT_URL, trust_env=False)
                    )
                    signed = self.request(client, "POST", "/api/staff/sessions", json=credential)
                    self.secrets.append(signed["csrf_token"])
                    client.headers["X-CSRF-Token"] = signed["csrf_token"]
                    clients[role] = client
                provider = stack.enter_context(
                    httpx.Client(
                        base_url=PROVIDER_URL,
                        trust_env=False,
                        headers={"Authorization": "Bearer " + self.token},
                        transport=DockerLoopbackTransport(),
                    )
                )
                yield clients, provider
        finally:
            cleanup_errors = []
            for staff_id in created:
                try:
                    account = auth.update_account(staff_id, active=False)
                    require(not account["active"], "TEMPORARY_ACCOUNT_NOT_DISABLED")
                    next(row for row in self.accounts if row["id"] == staff_id)[
                        "disabled_after_step"
                    ] = True
                except Exception:
                    cleanup_errors.append(staff_id)
            require(not cleanup_errors, "TEMPORARY_ACCOUNT_CLEANUP_FAILED")


def provider_lookup(probe, client, effect_key, *, status=200):
    return probe.request(
        client,
        "GET",
        "/mock-warehouse/shipments/by-key/" + quote(effect_key, safe=""),
        status=status,
    )


def normal_snapshot(probe, sessions, clients, provider, state, *, expected):
    case = state["case"]
    detail = probe.request(
        clients["customer"], "GET", f"/api/applications/{case['application_id']}"
    )
    delivery = probe.request(
        clients["customer"], "GET", f"/api/fulfillment-requests/{case['request_id']}"
    )
    require(detail["receipt"] == case["original_receipt"], "ORIGINAL_RECEIPT_CHANGED")
    with sessions() as session:
        app = session.get(Application, case["application_id"])
        require(
            app is not None and app.line_id == case["order_id"] + "-1", "APPLICATION_SCOPE_CHANGED"
        )
        reservation = columns(session.get(Reservation, app.id))
        stock = columns(session.get(Inventory, state["target"]))
        occupied = session.get(OrderLine, app.line_id).occupied
        require(
            session.get(Operation, app.operation_id).receipt == case["original_receipt"],
            "DATABASE_RECEIPT_CHANGED",
        )
    fact = provider_lookup(
        probe, provider, case["effect_key"], status=404 if expected == "pending" else 200
    )
    snapshot = {
        "application": {"id": app.id, "status": detail["status"], "version": detail["version"]},
        "delivery": delivery,
        "reservation": reservation,
        "inventory": stock,
        "line_occupied": occupied,
        "history_count": len(detail["history"]),
        "original_receipt": detail["receipt"],
        "provider_lookup": fact,
        "provider_fact_present_via_API": expected != "pending",
    }
    assert_normal(snapshot, expected)
    return snapshot


def assert_normal(snapshot, expected):
    delivery, stock = snapshot["delivery"], snapshot["inventory"]
    pending = expected == "pending"
    require(
        snapshot["application"]["version"] == (5 if pending else 6), "APPLICATION_VERSION_MISMATCH"
    )
    require(
        delivery["request"]["status"] == ("PENDING" if pending else "SUCCEEDED")
        and delivery["outbox"]["status"] == ("PENDING" if pending else "DELIVERED")
        and snapshot["reservation"]["status"] == ("RESERVED" if pending else "CONSUMED")
        and snapshot["line_occupied"] == 1
        and snapshot["history_count"] == (5 if pending else 6)
        and stock["available"] == 19
        and stock["reserved"] == int(pending),
        "SHIPMENT_OR_INVENTORY_MISMATCH",
    )
    if pending:
        require(not delivery["attempts"], "CONSUMER_ALREADY_RAN")


def prepare(probe, sessions):
    with sessions() as session:
        for table in ("warehouse_outbox", "reconciliation_tasks"):
            require(
                session.scalar(
                    text(f"SELECT count(*) FROM {table} WHERE status IN ('PENDING','CLAIMED')")
                )
                == 0,
                "EXISTING_ACTIVE_WORK_REFUSED",
            )
    seed(sessions)
    prefix = "d4-" + secrets.token_hex(6)
    target = new_variant(sessions, prefix)
    with probe.clients(sessions, prefix, prepare=True) as (clients, provider):
        case = new_case(sessions, prefix, CASE_NAME, target)
        probe.events.append(
            {"event": "case_created", "prefix": prefix, "target": target, "case": case}
        )
        command(probe, clients["support"], case, "review", 1, "AWAITING_RETURN", decision="approve")
        command(
            probe,
            clients["customer"],
            case,
            "return-shipment",
            2,
            "RETURN_IN_TRANSIT",
            carrier="local-simulator",
            tracking_number=prefix,
        )
        command(
            probe,
            clients["warehouse"],
            case,
            "receive",
            3,
            "RETURN_RECEIVED",
            note="D4 container restart fixture",
        )
        command(
            probe, clients["warehouse"], case, "replacement-shipments", 4, "REPLACEMENT_PENDING"
        )
        detail = probe.request(
            clients["customer"], "GET", f"/api/applications/{case['application_id']}"
        )
        request = detail["shipment_request"]
        case.update(request_id=request["id"], effect_key=request["effect_key"])
        state = {
            "format": FORMAT,
            "kind": "normal",
            "database": "127.0.0.1:55436/aftercare_test",
            "prefix": prefix,
            "target": target,
            "case": case,
            "accounts": list(probe.accounts),
        }
        state["prepared"] = normal_snapshot(
            probe, sessions, clients, provider, state, expected="pending"
        )
        return state


def verify(probe, sessions, state, *, expected, callbacks=False, replay=False):
    warehouse_id = (
        next(row["id"] for row in state["accounts"] if row["role"] == "warehouse")
        if replay
        else None
    )
    with probe.clients(sessions, state["prefix"], warehouse_id=warehouse_id) as (clients, provider):
        before = normal_snapshot(probe, sessions, clients, provider, state, expected=expected)
        result = {"before": before}
        if replay:
            original = state["case"]["http_commands"][-1]["receipt"]
            repeated = command(
                probe,
                clients["warehouse"],
                {**state["case"], "http_commands": []},
                "replacement-shipments",
                4,
                "REPLACEMENT_PENDING",
            )
            require(original == repeated, "ORIGINAL_ACTOR_COMMAND_REPLAY_CHANGED")
            result["command_replay"] = repeated
        if callbacks:
            rows = before["delivery"]["inbox"]
            require(
                len(rows) == 2
                and {row["sequence"] for row in rows} == {1, 2}
                and all(row["status"] in {"PROCESSED", "STALE"} for row in rows),
                "CALLBACK_INBOX_NOT_COMPLETE",
            )
            result["callback_sequences"] = sorted(row["sequence"] for row in rows)
        result["after"] = normal_snapshot(
            probe, sessions, clients, provider, state, expected=expected
        )
        require(result["before"] == result["after"], "READ_OR_REPLAY_CHANGED_BUSINESS_STATE")
        return result


def replay_provider(probe, sessions, state):
    with probe.clients(sessions, state["prefix"]) as (clients, provider):
        before = normal_snapshot(probe, sessions, clients, provider, state, expected="success")
        with sessions() as session:
            request = session.get(ShipmentRequest, state["case"]["request_id"])
            require(
                request.application_id == state["case"]["application_id"], "REQUEST_SCOPE_CHANGED"
            )
            wire = {**request.payload, "request_hash": request.request_hash}
        repeated = probe.request(provider, "POST", "/mock-warehouse/shipments", json=wire)
        require(repeated == before["provider_lookup"], "PROVIDER_RESTART_LOST_IDEMPOTENCY")
        after = normal_snapshot(probe, sessions, clients, provider, state, expected="success")
        require(before == after, "PROVIDER_REPLAY_CHANGED_BUSINESS_STATE")
        return {
            "before": before,
            "provider_replay": repeated,
            "after": after,
            "physical_provider_row_count": (
                "root separately verifies the named SQLite volume; API alone is not a row count"
            ),
        }


def check_schema(value):
    require(
        re.fullmatch(r"d4_migration_[a-f0-9]{8,32}", value or ""),
        "PRIVATE_MIGRATION_SCHEMA_REQUIRED",
    )
    return value


def migration_helpers():
    # Reuse frozen structural history data; importing defines tests but runs none.
    path = PROJECT_ROOT / "tests/integration/test_fulfillment_migration.py"
    spec = importlib.util.spec_from_file_location("d4_migration_fixture_helpers", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def legacy_rows(connection, tables, old_columns=None):
    result = {}
    for name, table in sorted(tables.items()):
        if name == "alembic_version" or old_columns is not None and name not in old_columns:
            continue
        names = old_columns[name] if old_columns else [col.name for col in table.columns]
        result[name] = [
            dict(row)
            for row in connection.execute(
                select(*(table.c[col] for col in names)).order_by(*table.primary_key)
            ).mappings()
        ]
    return json.loads(encode(result))


def private_connection(session, schema, revision):
    check_schema(schema)
    connection = session.connection()
    require(
        connection.scalar(
            text("SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname=:schema)"),
            {"schema": schema},
        ),
        "MIGRATION_SCHEMA_MISSING",
    )
    connection.execute(text(f'SET LOCAL search_path TO "{schema}", public'))
    require(
        connection.scalar(text("SELECT current_schema()")) == schema,
        "MIGRATION_SCHEMA_SCOPE_MISMATCH",
    )
    require(
        connection.scalar(text(f'SELECT version_num FROM "{schema}".alembic_version')) == revision,
        "MIGRATION_REVISION_MISMATCH",
    )
    metadata = MetaData()
    metadata.reflect(bind=connection, schema=schema)
    return connection, {
        table.name: table for table in metadata.tables.values() if table.schema == schema
    }


def legacy_seed(sessions, schema):
    helpers = migration_helpers()
    prefix = "d4-" + secrets.token_hex(6)
    with sessions.begin() as session:
        connection, tables = private_connection(session, schema, "0006_experiments")
        require(
            not any(
                connection.scalar(select(table).limit(1)) is not None
                for name, table in tables.items()
                if name != "alembic_version"
            ),
            "LEGACY_SCHEMA_NOT_EMPTY",
        )
        helpers.seed_history(connection, tables)
        return {
            "format": FORMAT,
            "kind": "legacy-seed",
            "database": "127.0.0.1:55436/aftercare_test",
            "prefix": prefix,
            "schema": schema,
            "old_columns": {
                name: [col.name for col in table.columns]
                for name, table in tables.items()
                if name != "alembic_version"
            },
            "old_rows": legacy_rows(connection, tables),
            "scope": "structural 0006 SQL history fixture; no model or HTTP execution",
        }


def legacy_prepare(sessions, state):
    schema, prefix = check_schema(state["schema"]), state["prefix"]
    with sessions.begin() as session:
        connection, tables = private_connection(session, schema, "0010_warehouse_recovery")
        require(
            legacy_rows(connection, tables, state["old_columns"]) == state["old_rows"],
            "PRE_0010_MIGRATION_REWROTE_OLD_COLUMNS",
        )
        require("send_state" not in tables["outbox_attempts"].c, "LEGACY_COLUMNS_ALREADY_UPGRADED")
        appid = connection.scalar(
            select(tables["aftercare_requests"].c.id).where(
                tables["aftercare_requests"].c.status == "EXCHANGE_REQUESTED"
            )
        )
        payload = {
            "effect_key": "exchange:" + appid,
            "application_id": appid,
            "variant_id": "legacy-new",
            "quantity": 1,
        }
        now = migration_helpers().NOW
        connection.execute(
            tables["shipment_requests"].insert(),
            {
                "id": prefix + "-legacy-request",
                "application_id": appid,
                "effect_key": payload["effect_key"],
                "payload": payload,
                "request_hash": digest(payload),
                "status": "PENDING",
                "triggered_by": {"actor_id": "historical-migration-fixture"},
                "created_at": now,
                "updated_at": now,
            },
        )
        connection.execute(
            tables["warehouse_outbox"].insert(),
            {
                "id": prefix + "-legacy-outbox",
                "request_id": prefix + "-legacy-request",
                "payload": payload,
                "status": "CLAIMED",
                "lease_owner": "legacy-owner",
                "generation": 1,
                "lease_expires_at": now + timedelta(seconds=15),
                "attempts": 1,
                "cycle_attempts": 1,
                "next_attempt_at": now,
                "created_at": now,
                "updated_at": now,
            },
        )
        connection.execute(
            tables["outbox_attempts"].insert(),
            {
                "id": prefix + "-legacy-attempt",
                "outbox_id": prefix + "-legacy-outbox",
                "generation": 1,
                "owner": "legacy-owner",
                "started_at": now,
            },
        )
        old_columns = {
            name: [col.name for col in table.columns]
            for name, table in tables.items()
            if name != "alembic_version"
        }
        return {
            "format": FORMAT,
            "kind": "legacy",
            "database": "127.0.0.1:55436/aftercare_test",
            "prefix": prefix,
            "schema": schema,
            "old_columns": old_columns,
            "old_rows": legacy_rows(connection, tables),
            "attempt_id": prefix + "-legacy-attempt",
            "scope": (
                "structural SQL historical fixture, including synthetic UNKNOWN ledger; "
                "no HTTP/model execution or proof of approved shipment"
            ),
        }


def assert_legacy_preserved(state, current, attempt):
    require(current == state["old_rows"], "MIGRATION_REWROTE_PRIOR_FACTS")
    require(
        attempt["send_state"] == "LEGACY_UNKNOWN" and attempt["send_started_at"] is None,
        "MIGRATION_INFERRED_LEGACY_SEND_PERMISSION",
    )


def legacy_verify(sessions, state):
    schema = check_schema(state["schema"])
    with sessions() as session:
        connection = session.connection()
        connection.execute(text("SET TRANSACTION READ ONLY"))
        connection, tables = private_connection(session, schema, REVISION)
        current = legacy_rows(connection, tables, state["old_columns"])
        attempts = tables["outbox_attempts"]
        attempt = dict(
            connection.execute(select(attempts).where(attempts.c.id == state["attempt_id"]))
            .mappings()
            .one()
        )
        assert_legacy_preserved(state, current, attempt)
        return {
            "schema": schema,
            "old_columns": state["old_columns"],
            "old_rows": current,
            "old_facts_unchanged": True,
            "new_attempt_columns": {
                "send_state": attempt["send_state"],
                "send_started_at": attempt["send_started_at"],
            },
        }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "action",
        choices=[
            "prepare",
            "verify-pending",
            "verify-success",
            "verify-callbacks",
            "replay-provider",
            "cleanup-accounts",
            "legacy-seed",
            "legacy-prepare",
            "legacy-verify",
        ],
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--state", type=Path)
    parser.add_argument("--schema")
    args = parser.parse_args()
    validate_environment(os.environ)
    if args.action not in {"prepare", "legacy-seed"}:
        require(args.state is not None, "STATE_FILE_REQUIRED")
    if args.action == "legacy-seed":
        check_schema(args.schema)
    token = os.environ.get("WAREHOUSE_TOKEN", "")
    require(args.action.startswith("legacy-") or len(token) >= 32, "WAREHOUSE_TOKEN_REQUIRED")
    output = runtime_path(args.output_dir)
    require(not output.exists(), "EVIDENCE_DIRECTORY_EXISTS")
    output.mkdir(parents=True, mode=0o700)
    os.umask(0o077)
    install_offline_guard()
    sessions = session_factory(
        Settings(_env_file=None, database_url=DATABASE, allow_real_model_calls=False)
    )
    probe = Probe(token)
    report = {
        "status": "RUNNING",
        "action": args.action,
        "database": "127.0.0.1:55436/aftercare_test",
        "real_model_requests": 0,
        "accounts": probe.accounts,
        "events": probe.events,
    }
    failure = None
    try:
        if not args.action.startswith("legacy-"):
            require(
                database_health(Settings(_env_file=None, database_url=DATABASE)).status == "ready",
                "DATABASE_NOT_READY",
            )
        state_kind = (
            "legacy-seed"
            if args.action == "legacy-prepare"
            else "legacy"
            if args.action == "legacy-verify"
            else "normal"
        )
        state = load_state(args.state, kind=state_kind) if args.state else None
        if args.action == "prepare":
            state = prepare(probe, sessions)
            write_new(output / "state.json", state)
            report["result"] = state["prepared"]
        elif args.action in {"legacy-seed", "legacy-prepare"}:
            state = (
                legacy_seed(sessions, args.schema)
                if args.action == "legacy-seed"
                else legacy_prepare(sessions, state)
            )
            write_new(output / "state.json", state)
            report["result"] = {
                "schema": state["schema"],
                "tables": len(state["old_rows"]),
                "scope": state["scope"],
            }
        elif args.action == "legacy-verify":
            report["result"] = legacy_verify(sessions, state)
        elif args.action in {"verify-pending", "verify-success", "verify-callbacks"}:
            report["result"] = verify(
                probe,
                sessions,
                state,
                expected="pending" if args.action == "verify-pending" else "success",
                callbacks=args.action == "verify-callbacks",
                replay=args.action == "verify-success",
            )
        elif args.action == "replay-provider":
            report["result"] = replay_provider(probe, sessions, state)
        else:
            auth = StaffAuthService(sessions)
            report["result"] = [
                auth.update_account(row["id"], active=False) for row in state["accounts"]
            ]
        report["status"] = "PASSED"
    except Exception as error:
        failure = error
        code = str(error) if isinstance(error, RuntimeError) else type(error).__name__
        report.update(
            status="FAILED",
            error_code=code if re.fullmatch(r"[A-Z][A-Z0-9_]+", code) else type(error).__name__,
        )
    finally:
        sessions.kw["bind"].dispose()
        report["elapsed_seconds"] = round(time.monotonic() - probe.started, 3)
        encoded = encode(report)
        require(
            not any(secret and secret in encoded for secret in probe.secrets), "SECRET_IN_REPORT"
        )
        write_new(output / "result.json", report)
    print(
        json.dumps(
            {
                "status": report["status"],
                "action": args.action,
                "result": str(output / "result.json"),
            }
        )
    )
    return 1 if failure is not None else 0


if __name__ == "__main__":
    raise SystemExit(main())
