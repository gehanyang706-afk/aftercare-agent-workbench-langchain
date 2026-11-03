"""Recover the two preserved C1 UNKNOWNs using real, query-only C2 CLI workers.

Never resets PostgreSQL or opens the frozen provider snapshot with SQLite. Run
before any test fixture can clear the isolated 55434/aftercare_test database.
"""

import argparse
import hashlib
import http.client
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
from pathlib import Path

import psycopg
from psycopg.rows import dict_row

from aftercare.config import PROJECT_ROOT
from aftercare.offline import install_offline_guard

TEST_URL = "postgresql+psycopg://aftercare:aftercare_local_only@127.0.0.1:55434/aftercare_test"
OLD_REVISION = "0009_warehouse_outbox"
NEW_REVISION = "0010_warehouse_recovery"
PORT = 8141
FACT_COLUMNS = "effect_key,application_id,request_hash,payload_json,external_shipment_id,created_at"


def require(condition, code):
    if not condition:
        raise RuntimeError(code)


def digest_file(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def encode(value):
    return json.dumps(value, ensure_ascii=False, indent=2, default=lambda item: item.isoformat())


def fixed_error(error):
    value = str(error) if isinstance(error, RuntimeError) else type(error).__name__
    return value if re.fullmatch(r"[A-Z_]+", value) else type(error).__name__


def sqlite_facts(path, *, immutable=False):
    uri = path.as_uri() + "?mode=ro" + ("&immutable=1" if immutable else "")
    with sqlite3.connect(uri, uri=True, timeout=2) as connection:
        connection.row_factory = sqlite3.Row
        return [
            dict(row)
            for row in connection.execute(
                f"SELECT {FACT_COLUMNS} FROM shipments ORDER BY effect_key"
            )
        ]


def read_pg(cases, *, include_recovery=False):
    # These fixed C1 columns work before migration; no current ORM SELECT can
    # accidentally reference a 0010 column while PostgreSQL is still on 0009.
    with psycopg.connect(
        TEST_URL.replace("postgresql+psycopg://", "postgresql://", 1),
        connect_timeout=2,
        row_factory=dict_row,
        options=(
            "-c statement_timeout=3000 -c lock_timeout=1000 -c default_transaction_read_only=on"
        ),
    ) as connection:
        connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        revisions = connection.execute("SELECT version_num FROM alembic_version").fetchall()
        require(len(revisions) == 1, "DATABASE_REVISION_AMBIGUOUS")
        state = {"revision": revisions[0]["version_num"], "cases": []}
        require(state["revision"] in {OLD_REVISION, NEW_REVISION}, "UNEXPECTED_DATABASE_REVISION")
        for case in cases:
            application = connection.execute(
                "SELECT id,operation_id,line_id,quantity,target_variant,status,version "
                "FROM aftercare_requests WHERE id=%s",
                (case["application_id"],),
            ).fetchone()
            require(application is not None, "PRESERVED_APPLICATION_MISSING")
            request = connection.execute(
                "SELECT id,application_id,effect_key,payload,request_hash,status,"
                "external_shipment_id "
                "FROM shipment_requests WHERE id=%s",
                (case["request_id"],),
            ).fetchone()
            outbox = connection.execute(
                "SELECT id,request_id,payload,status,attempts,generation,lease_owner,"
                "lease_expires_at,"
                "last_error_code FROM warehouse_outbox WHERE request_id=%s",
                (case["request_id"],),
            ).fetchone()
            receipt = connection.execute(
                "SELECT receipt FROM operations WHERE id=%s",
                (application["operation_id"],),
            ).fetchone()
            reservation = connection.execute(
                "SELECT application_id,variant_id,quantity,status FROM inventory_reservations "
                "WHERE application_id=%s",
                (case["application_id"],),
            ).fetchone()
            fulfillment = connection.execute(
                "SELECT application_id,effect_key,shipment_status,external_shipment_id "
                "FROM exchange_fulfillments WHERE application_id=%s",
                (case["application_id"],),
            ).fetchone()
            attempts = connection.execute(
                "SELECT generation,outcome,error_code FROM outbox_attempts WHERE outbox_id=%s "
                "ORDER BY generation",
                ((outbox or {}).get("id"),),
            ).fetchall()
            audits = connection.execute(
                "SELECT command,from_version,to_version FROM fulfillment_audits "
                "WHERE application_id=%s ORDER BY to_version",
                (case["application_id"],),
            ).fetchall()
            line = connection.execute(
                "SELECT id,quantity,occupied FROM order_lines WHERE id=%s",
                (application["line_id"],),
            ).fetchone()
            require(
                all(
                    item is not None
                    for item in (request, outbox, receipt, reservation, fulfillment, line)
                ),
                "PRESERVED_DEPENDENT_FACT_MISSING",
            )
            state["cases"].append(
                {
                    "name": case["name"],
                    "application": application,
                    "request": request,
                    "outbox": outbox,
                    "original_receipt": receipt["receipt"],
                    "reservation": reservation,
                    "fulfillment": fulfillment,
                    "outbox_attempts": attempts,
                    "audits": audits,
                    "line": line,
                }
            )
        state["inventory"] = connection.execute(
            "SELECT variant_id,available,reserved,version FROM inventory ORDER BY variant_id"
        ).fetchall()
        state["unknown_request_ids"] = [
            row["id"]
            for row in connection.execute(
                "SELECT id FROM shipment_requests WHERE status='UNKNOWN' ORDER BY id"
            )
        ]
        state["active_outbox_ids"] = [
            row["id"]
            for row in connection.execute(
                "SELECT id FROM warehouse_outbox WHERE status IN ('PENDING','CLAIMED') ORDER BY id"
            )
        ]
        if include_recovery:
            require(state["revision"] == NEW_REVISION, "RECOVERY_MIGRATION_REQUIRED")
            state["tasks"] = connection.execute(
                "SELECT id,request_id,status,owner,generation,lease_expires_at,attempts,"
                "cycle_attempts,"
                "cycle,next_attempt_at,deadline_at,last_error_code FROM reconciliation_tasks "
                "ORDER BY request_id"
            ).fetchall()
            state["sequences"] = connection.execute(
                "SELECT id,last_sequence,provider_external_id FROM shipment_requests "
                "WHERE id=ANY(%s) ORDER BY id",
                ([case["request_id"] for case in cases],),
            ).fetchall()
        return state


def validate_before(state, cases, stock):
    require(
        state["unknown_request_ids"] == sorted(case["request_id"] for case in cases),
        "PRESERVED_UNKNOWN_SET_CHANGED",
    )
    require(not state["active_outbox_ids"], "ACTIVE_OUTBOX_REFUSED")
    inventory = next(
        (row for row in state["inventory"] if row["variant_id"] == stock["variant_id"]), None
    )
    require(inventory == stock and stock["reserved"] == 3, "PRESERVED_INVENTORY_CHANGED")
    for actual, case in zip(state["cases"], cases, strict=True):
        expected = case["final"]
        require(
            actual["application"]["status"] == "REPLACEMENT_PENDING"
            and actual["application"]["version"] == 5,
            "PRESERVED_APPLICATION_CHANGED",
        )
        require(
            actual["request"]["status"] == "UNKNOWN"
            and actual["request"]["application_id"] == case["application_id"]
            and actual["request"]["effect_key"] == case["effect_key"]
            and actual["request"]["request_hash"] == expected["request"]["request_hash"]
            and actual["request"]["external_shipment_id"] is None,
            "PRESERVED_REQUEST_CHANGED",
        )
        require(
            actual["outbox"]["status"] == "BLOCKED"
            and actual["outbox"]["attempts"] == expected["outbox"]["attempts"] == 1
            and actual["outbox"]["lease_owner"] is None
            and actual["outbox"]["lease_expires_at"] is None,
            "PRESERVED_OUTBOX_CHANGED",
        )
        require(
            actual["reservation"] == expected["reservation"]
            and actual["reservation"]["status"] == "RESERVED",
            "PRESERVED_RESERVATION_CHANGED",
        )
        require(actual["original_receipt"] == case["original_receipt"], "ORIGINAL_RECEIPT_CHANGED")
        require(
            actual["line"]["occupied"] == expected["line_occupied"] == 1,
            "PRESERVED_LINE_OCCUPANCY_CHANGED",
        )


def validate_after(before, after, cases, stock):
    require(
        not after["unknown_request_ids"] and not after["active_outbox_ids"],
        "RECOVERY_LEFT_UNKNOWN_OR_ACTIVE_OUTBOX",
    )
    for old, actual, case in zip(before["cases"], after["cases"], cases, strict=True):
        external = case["manual_provider_lookup"]["external_shipment_id"]
        require(
            actual["request"]["status"] == "SUCCEEDED"
            and actual["request"]["external_shipment_id"] == external,
            "RECOVERY_REQUEST_NOT_ORIGINAL_SUCCESS",
        )
        require(
            actual["outbox"]["status"] == "DELIVERED"
            and actual["outbox"]["attempts"] == old["outbox"]["attempts"]
            and actual["outbox_attempts"] == old["outbox_attempts"],
            "RECOVERY_REDELIVERED_OUTBOX",
        )
        require(
            actual["application"]["status"] == "REPLACEMENT_SHIPPED"
            and actual["application"]["version"] == 6,
            "RECOVERY_APPLICATION_NOT_ADVANCED_ONCE",
        )
        require(
            actual["reservation"] == {**old["reservation"], "status": "CONSUMED"},
            "RECOVERY_RESERVATION_NOT_CONSUMED_ONCE",
        )
        require(
            actual["fulfillment"]["external_shipment_id"] == external
            and actual["fulfillment"]["shipment_status"] == "SUCCEEDED",
            "RECOVERY_FULFILLMENT_NOT_ORIGINAL_SUCCESS",
        )
        require(
            actual["original_receipt"] == old["original_receipt"] == case["original_receipt"],
            "RECOVERY_CHANGED_ORIGINAL_RECEIPT",
        )
        require(actual["line"] == old["line"], "RECOVERY_CHANGED_LINE_OCCUPANCY")
        require(
            actual["audits"][:-1] == old["audits"] and actual["audits"][-1]["to_version"] == 6,
            "RECOVERY_NOT_EXACTLY_ONE_DOMAIN_TRANSITION",
        )
    expected_inventory = [
        {**row, "reserved": 1, "version": row["version"] + 2}
        if row["variant_id"] == stock["variant_id"]
        else row
        for row in before["inventory"]
    ]
    require(
        after["inventory"] == expected_inventory, "RECOVERY_INVENTORY_NOT_CONSUMED_EXACTLY_TWICE"
    )
    require(
        len(after["tasks"]) == 2
        and all(
            row["status"] == "DONE"
            and row["attempts"] == row["cycle_attempts"] == 1
            and row["owner"] is None
            and row["lease_expires_at"] is None
            for row in after["tasks"]
        ),
        "RECOVERY_TASKS_NOT_TWO_SINGLE_ATTEMPT_SUCCESSES",
    )
    require(
        {row["request_id"] for row in after["tasks"]} == {case["request_id"] for case in cases},
        "RECOVERY_TASK_REQUEST_BINDING_CHANGED",
    )
    external_by_request = {
        case["request_id"]: case["manual_provider_lookup"]["external_shipment_id"] for case in cases
    }
    require(
        all(
            row["last_sequence"] == 2
            and row["provider_external_id"] == external_by_request[row["id"]]
            for row in after["sequences"]
        ),
        "RECOVERY_PROVIDER_SEQUENCE_NOT_PERSISTED",
    )


class RecoveryRun:
    def __init__(self, output, env, token, started):
        self.output, self.env, self.token, self.started = output, env, token, started
        self.children, self.processes = [], []
        self.journal = (output / "processes.jsonl").open("x")

    def remaining(self, cap=10):
        remaining = self.started + 76 - time.monotonic()
        require(remaining > 0, "RECOVERY_TIME_BUDGET_EXHAUSTED")
        return min(cap, remaining)

    def safe_text(self, value):
        return value.replace(self.token, "[REDACTED]").replace(
            "aftercare_local_only", "[LOCAL_DATABASE_PASSWORD]"
        )

    def record(self, value):
        encoded = encode(value)
        require(self.token not in encoded, "SECRET_IN_EVIDENCE")
        self.journal.write(json.dumps(value, ensure_ascii=False) + "\n")
        self.journal.flush()

    def spawn(self, args, label):
        self.remaining()
        child = subprocess.Popen(
            [sys.executable, "-m", "aftercare", *args],
            cwd=PROJECT_ROOT,
            env=self.env,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
        self.children.append((child, label))
        item = {"label": label, "pid": child.pid, "returncode": None}
        self.processes.append(item)
        self.record({"event": "started", **item})
        return child

    def stop(self, child):
        # Each CLI owns a new group, including its bounded HTTP attempt child.
        try:
            os.killpg(child.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            child.wait(timeout=1)
        except subprocess.TimeoutExpired:
            pass
        # Kill the whole group even if its leader exited first: a timed-out
        # worker must not leave a bounded HTTP subprocess orphaned behind it.
        try:
            os.killpg(child.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        child.wait(timeout=1)
        require(child.poll() is not None, "CHILD_CLEANUP_FAILED")

    def collect(self, child, label, *, timeout):
        try:
            stdout, stderr = child.communicate(timeout=self.remaining(timeout))
        except BaseException:
            self.stop(child)
            stdout, stderr = child.communicate(timeout=1)
            self.save_logs(child, label, stdout, stderr)
            raise
        self.save_logs(child, label, stdout, stderr)
        return stdout

    def save_logs(self, child, label, stdout, stderr):
        for suffix, value in (("stdout", stdout), ("stderr", stderr)):
            with (self.output / f"{label}.{suffix}.log").open("x") as log:
                log.write(self.safe_text(value))
        for item in self.processes:
            if item["pid"] == child.pid:
                item["returncode"] = child.returncode
        self.record(
            {"event": "finished", "label": label, "pid": child.pid, "returncode": child.returncode}
        )

    def cli(self, args, label, *, structured=True, timeout=15):
        child = self.spawn(args, label)
        stdout = self.collect(child, label, timeout=timeout)
        require(child.returncode == 0, "CLI_COMMAND_FAILED")
        if not structured:
            return {"returncode": child.returncode}
        result = json.loads(stdout)
        require(isinstance(result, dict) and result.get("ok") is not False, "CLI_RESULT_INVALID")
        require(self.token not in encode(result), "SECRET_IN_CLI_RESULT")
        return result

    def wait_ready(self, child):
        deadline = time.monotonic() + self.remaining(15)
        while time.monotonic() < deadline:
            require(child.poll() is None, "PROVIDER_EXITED_BEFORE_READY")
            connection = http.client.HTTPConnection("127.0.0.1", PORT, timeout=self.remaining(1))
            try:
                connection.request("GET", "/health/ready")
                response = connection.getresponse()
                body = response.read(1024)
                if response.status == 200 and json.loads(body) == {"status": "ready"}:
                    return
            except (OSError, http.client.HTTPException, ValueError):
                pass
            finally:
                connection.close()
            time.sleep(min(0.1, self.remaining()))
        raise RuntimeError("PROVIDER_READINESS_TIMEOUT")


def main():
    started = time.monotonic()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir", type=Path, default=PROJECT_ROOT / "runtime/backend-C2/c1-recovery-first"
    )
    args = parser.parse_args()
    require(
        os.environ.get("DATABASE_URL") == TEST_URL
        and os.environ.get("ALLOW_REAL_MODEL_CALLS") == "false",
        "EXPLICIT_ISOLATED_OFFLINE_ENVIRONMENT_REQUIRED",
    )
    output = args.output_dir.resolve()
    require(output.is_relative_to((PROJECT_ROOT / "runtime").resolve()), "OUTPUT_MUST_BE_RUNTIME")
    require(not output.exists(), "FIRST_EVIDENCE_EXISTS_USE_NEW_DIRECTORY")
    os.umask(0o077)
    output.mkdir(parents=True, mode=0o700)
    install_offline_guard()
    token = secrets.token_urlsafe(40)
    # Do not inherit cloud keys, PYTHONPATH, proxy configuration or callback
    # destinations. Secrets are supplied solely through this in-memory environment.
    env = {
        key: value
        for key, value in os.environ.items()
        if key in {"PATH", "HOME", "LANG", "LC_ALL", "TMPDIR", "SYSTEMROOT"}
    }
    env.update(
        {
            "PYTHONPATH": str(PROJECT_ROOT / "src"),
            "PYTHONIOENCODING": "utf-8",
            "DATABASE_URL": TEST_URL,
            "ALLOW_REAL_MODEL_CALLS": "false",
            "WAREHOUSE_DELIVERY_ENABLED": "true",
            "WAREHOUSE_SIMULATION_ENABLED": "false",
            "WAREHOUSE_CALLBACKS_ENABLED": "false",
            "DEMO_LOGIN_ENABLED": "false",
            "WAREHOUSE_BASE_URL": f"http://127.0.0.1:{PORT}",
            "WAREHOUSE_TOKEN": token,
            "MOCK_WAREHOUSE_TOKEN": token,
            "MOCK_WAREHOUSE_CALLBACK_BASE_URL": "",
            "MOCK_WAREHOUSE_CALLBACK_SECRET": "",
            "WAREHOUSE_CALLBACK_SECRET": "",
            "WAREHOUSE_TIMEOUT_SECONDS": "2",
            "OUTBOX_LEASE_SECONDS": "15",
            "OUTBOX_MAX_ATTEMPTS": "3",
            "RECONCILIATION_MAX_ATTEMPTS": "3",
            "RECONCILIATION_DEADLINE_SECONDS": "60",
            "RECONCILIATION_MAX_CYCLES": "3",
        }
    )
    run = RecoveryRun(output, env, token, started)
    report = {
        "status": "RUNNING",
        "database": "127.0.0.1:55434/aftercare_test",
        "real_model_requests": 0,
        "time_budget_seconds": 90,
        "transport": "real CLI migration + provider TCP GET + reconciliation CLI subprocesses",
        "provider_post_requests_by_script": 0,
        "temporary_staff_created": 0,
        "processes": run.processes,
        "callbacks_enabled": False,
    }
    original, expected_sha, provider, before_facts = None, None, None, None
    state_path = output / "warehouse.sqlite3"
    errors = []

    def alarm(signum, frame):
        raise RuntimeError("RECOVERY_TOTAL_DEADLINE_EXCEEDED")

    previous_handler = signal.signal(signal.SIGALRM, alarm)
    # Interrupt blocking work at 76s, reserving 14s of the 90s total for group
    # termination/reaping and the small local consistent snapshot.
    signal.setitimer(signal.ITIMER_REAL, max(0.1, 76 - (time.monotonic() - started)))
    try:
        manifest = json.loads(
            (PROJECT_ROOT / "docs/benchmarks/reports/backend-C1-validation.json").read_text()
        )
        descriptor = manifest["consistent_provider_snapshot"]
        original = PROJECT_ROOT / descriptor["path"]
        require(
            original == PROJECT_ROOT / "runtime/backend-C1/provider-final.sqlite3"
            and not original.is_symlink(),
            "UNEXPECTED_FROZEN_SNAPSHOT_PATH",
        )
        expected_sha = descriptor["sha256"]
        require(digest_file(original) == expected_sha, "FROZEN_PROVIDER_SNAPSHOT_HASH_MISMATCH")
        raw_path = PROJECT_ROOT / "runtime/backend-C1/http-first/http-first.json"
        require(
            digest_file(raw_path)
            == manifest["evidence_sha256"][str(raw_path.relative_to(PROJECT_ROOT))],
            "FROZEN_C1_RAW_RESULT_HASH_MISMATCH",
        )
        raw = json.loads(raw_path.read_text())
        require(raw["status"] == "PASSED", "C1_SOURCE_ACCEPTANCE_NOT_PASSED")
        by_name = {case["name"]: case for case in raw["scenarios"]}
        cases = [by_name[name] for name in ("accepted-lost", "killed")]
        stock = raw["synthetic_inventory_final"]
        with original.open("rb") as source, state_path.open("xb") as destination:
            destination.write(source.read())
        require(digest_file(state_path) == expected_sha, "PROVIDER_COPY_HASH_MISMATCH")
        before_facts = sqlite_facts(state_path, immutable=True)
        require(
            len(before_facts) == descriptor["total_rows"] == 5, "FROZEN_PROVIDER_FACT_COUNT_CHANGED"
        )
        facts_by_key = {row["effect_key"]: row for row in before_facts}
        for case in cases:
            lookup = case["manual_provider_lookup"]
            fact = facts_by_key[case["effect_key"]]
            require(
                all(
                    fact[key] == lookup[key]
                    for key in (
                        "effect_key",
                        "application_id",
                        "request_hash",
                        "external_shipment_id",
                    )
                ),
                "FROZEN_LOOKUP_NOT_ORIGINAL_PROVIDER_FACT",
            )
        report["source"] = {"snapshot": descriptor, "raw_result_sha256": digest_file(raw_path)}
        report["provider_facts_before"] = before_facts
        before = read_pg(cases)
        report["before"] = before
        validate_before(before, cases, stock)
        for actual, case in zip(before["cases"], cases, strict=True):
            expected_payload = json.loads(facts_by_key[case["effect_key"]]["payload_json"])
            require(
                actual["request"]["payload"] == actual["outbox"]["payload"] == expected_payload,
                "PRESERVED_PAYLOAD_BINDING_CHANGED",
            )
        if before["revision"] == NEW_REVISION:
            already_migrated = read_pg(cases, include_recovery=True)
            report["already_migrated_before_recovery"] = already_migrated
            require(
                all(
                    row["attempts"] == 0 and row["status"] == "PENDING"
                    for row in already_migrated["tasks"]
                ),
                "PREEXISTING_RECOVERY_ATTEMPTS_REFUSED",
            )
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", PORT))
        report["migration"] = run.cli(["db", "migrate"], "db-migrate", structured=False, timeout=25)
        migrated = read_pg(cases, include_recovery=True)
        report["after_migration"] = migrated
        require(migrated["revision"] == NEW_REVISION, "MIGRATION_NOT_AT_C2")
        require(
            migrated["cases"] == before["cases"] and migrated["inventory"] == before["inventory"],
            "MIGRATION_CHANGED_C1_BUSINESS_FACTS",
        )
        provider = run.spawn(
            ["mock-warehouse", "--port", str(PORT), "--state-path", str(state_path)], "provider"
        )
        run.wait_ready(provider)
        report["workers"] = [
            run.cli(["reconciliation-worker", "--once"], f"reconcile-{index}") for index in (1, 2)
        ]
        after = read_pg(cases, include_recovery=True)
        report["after_recovery"] = after
        validate_after(before, after, cases, stock)
        report["third_worker"] = run.cli(["reconciliation-worker", "--once"], "reconcile-idle")
        require(report["third_worker"]["status"] == "IDLE", "THIRD_WORKER_NOT_IDLE")
        unchanged = read_pg(cases, include_recovery=True)
        require(unchanged == after, "IDLE_WORKER_CHANGED_RECOVERED_FACTS")
        report["third_worker_preserved_state"] = True
        report["status"] = "PASSED"
    except BaseException as error:
        report.update(status="FAILED", error_code=fixed_error(error))
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        for child, label in reversed(run.children):
            try:
                run.stop(child)
                if label == "provider":
                    stdout, stderr = child.communicate(timeout=1)
                    run.save_logs(child, label, stdout, stderr)
            except BaseException:
                errors.append("CHILD_CLEANUP_FAILED")
        try:
            if state_path.exists() and before_facts is not None:
                final_facts = sqlite_facts(state_path)
                require(final_facts == before_facts, "PROVIDER_FACTS_CHANGED_DURING_QUERY_RECOVERY")
                final_path = output / "provider-final.sqlite3"
                with final_path.open("xb"):
                    pass
                with sqlite3.connect(
                    state_path.as_uri() + "?mode=ro", uri=True, timeout=2
                ) as source:
                    with sqlite3.connect(final_path, timeout=2) as destination:
                        source.backup(destination)
                with sqlite3.connect(
                    final_path.as_uri() + "?mode=ro&immutable=1", uri=True
                ) as final:
                    version = final.execute("PRAGMA user_version").fetchone()[0]
                    event_count = (
                        final.execute("SELECT count(*) FROM callback_events").fetchone()[0]
                        if version == 2
                        else 0
                    )
                report["provider_final"] = {
                    "snapshot": str(final_path),
                    "sha256": digest_file(final_path),
                    "schema_version": version,
                    "facts": final_facts,
                    "event_count": event_count,
                    "original_facts_unchanged": True,
                }
                if report["status"] == "PASSED":
                    require(
                        version == 2 and event_count == 10, "PROVIDER_V2_EVENT_BACKFILL_MISMATCH"
                    )
            if original is not None and expected_sha is not None:
                report["frozen_snapshot_sha256_after"] = digest_file(original)
                require(
                    report["frozen_snapshot_sha256_after"] == expected_sha,
                    "FROZEN_PROVIDER_SNAPSHOT_CHANGED",
                )
        except BaseException as error:
            errors.append(fixed_error(error))
        with socket.socket() as probe:
            probe.settimeout(0.2)
            port_closed = probe.connect_ex(("127.0.0.1", PORT)) != 0
        stopped = all(child.poll() is not None for child, _ in run.children)
        if not port_closed:
            errors.append("PROVIDER_PORT_NOT_CLOSED")
        if not stopped:
            errors.append("CHILD_STILL_RUNNING")
        report["cleanup"] = {
            "children_stopped": stopped,
            "provider_port_closed": port_closed,
            "errors": errors,
        }
        report["elapsed_seconds"] = round(time.monotonic() - started, 3)
        if errors:
            report["status"] = "FAILED"
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)
        encoded = encode(report)
        require(token not in encoded, "SECRET_IN_REPORT")
        with (output / "recovery-first.json").open("x") as evidence:
            evidence.write(encoded + "\n")
        run.journal.close()
    print(
        json.dumps(
            {
                "status": report["status"],
                "result": str(output / "recovery-first.json"),
                "real_model_requests": 0,
            }
        ),
        flush=True,
    )
    return 0 if report["status"] == "PASSED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
