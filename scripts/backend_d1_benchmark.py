"""Paired D1 read-API benchmark on one persistent synthetic Stage-A fixture.

prepare -> measure before -> change product source -> measure after -> compare
-> explicit cleanup. No migrations, resets, models, workers, SSE or lock probes.
Requests use TestClient/in-process ASGI, not TCP, concurrent load or HTTP QPS.
"""

import argparse
import hashlib
import json
import os
import platform
import re
import secrets
import statistics
import subprocess
import time
from collections import Counter
from contextlib import contextmanager
from copy import deepcopy
from datetime import timedelta
from pathlib import Path

from fastapi.testclient import TestClient
from sqlalchemy import delete, select, text

from aftercare.api.app import create_app
from aftercare.config import PROJECT_ROOT, Settings
from aftercare.domain.contracts import utc_now
from aftercare.offline import install_offline_guard
from aftercare.repositories.database import session_factory
from aftercare.repositories.models import DemoSession
from aftercare.services.business import BusinessService, digest
from scripts import backend_baseline as baseline

REVISION = "0011_dispatch_intent"
FORMAT = "backend-d1-paired-v1"
PREFIX_PATTERN = re.compile(r"benchmark_d1_[0-9a-f]{16}\Z")


def require(condition, code):
    if not condition:
        raise RuntimeError(code)


def canonical(value):
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=lambda item: item.isoformat(),
    )


def sha(value):
    return hashlib.sha256(value).hexdigest()


def fingerprint(value):
    return sha(canonical(value).encode())


def new_json(path, value):
    with path.open("x") as handle:
        handle.write(json.dumps(json.loads(canonical(value)), ensure_ascii=False, indent=2) + "\n")


def runtime_path(path):
    resolved = path.expanduser().resolve()
    require(resolved.is_relative_to((PROJECT_ROOT / "runtime").resolve()), "OUTPUT_MUST_BE_RUNTIME")
    return resolved


def new_directory(path):
    resolved = runtime_path(path)
    require(not resolved.exists(), "EVIDENCE_DIRECTORY_EXISTS")
    resolved.mkdir(parents=True, mode=0o700)
    return resolved


def source_info():
    entries = {
        str(path.relative_to(PROJECT_ROOT)): sha(path.read_bytes())
        for path in sorted((PROJECT_ROOT / "src/aftercare").rglob("*.py"))
    }
    return {
        "commit": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=PROJECT_ROOT, text=True, timeout=5
        ).strip(),
        "product_entries": entries,
        "product_fingerprint": fingerprint(entries),
        "harness_sha256": sha(Path(__file__).read_bytes()),
        "stage_A_harness_sha256": sha(Path(baseline.__file__).read_bytes()),
        "requirements_sha256": sha((PROJECT_ROOT / "requirements.lock").read_bytes()),
    }


@contextmanager
def database(raw):
    baseline.validate_target(raw)
    require(raw == baseline.TEST_URL, "EXACT_ISOLATED_TEST_URL_REQUIRED")
    sessions = session_factory(Settings(_env_file=None, database_url=raw))
    engine = sessions.kw["bind"]
    try:
        with engine.connect() as gate:
            acquired = gate.scalar(text("SELECT pg_try_advisory_lock(27102026, 1)"))
            gate.commit()
            require(acquired, "ANOTHER_BASELINE_HARNESS_IS_RUNNING")
            try:
                with sessions() as session:
                    require(
                        session.scalar(text("SELECT current_user")) == "aftercare",
                        "DEDICATED_ROLE_REQUIRED",
                    )
                    require(
                        list(session.scalars(text("SELECT version_num FROM alembic_version")))
                        == [REVISION],
                        "SCHEMA_REVISION_MISMATCH",
                    )
                    require(
                        session.get(baseline.Customer, "alice") is not None,
                        "EXISTING_DEMO_SEED_REQUIRED",
                    )
                    busy = session.scalar(
                        text(
                            "SELECT count(*) FROM pg_stat_activity "
                            "WHERE datname=current_database() "
                            "AND pid<>pg_backend_pid() AND pid<>:gate AND state<>'idle'"
                        ),
                        {"gate": gate.scalar(text("SELECT pg_backend_pid()"))},
                    )
                    gate.commit()
                    require(busy == 0, "TEST_DATABASE_BUSY")
                yield sessions
            finally:
                gate.execute(text("SELECT pg_advisory_unlock(27102026, 1)"))
                gate.commit()
    finally:
        engine.dispose()


def fixture_projection(sessions, prefix):
    rows = {}
    with sessions() as session:
        session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
        conv_ids = select(baseline.Conversation.id).where(
            baseline.Conversation.id.startswith(prefix + "-", autoescape=True)
        )
        for model in baseline.TABLES:
            if model is baseline.Customer or model is DemoSession:
                continue
            table = model.__table__
            if "conversation_id" in table.c:
                predicate = table.c.conversation_id.in_(conv_ids)
            elif model is baseline.Policy:
                predicate = table.c.id == prefix
            elif model is baseline.Inventory:
                predicate = table.c.variant_id.startswith(prefix + "-", autoescape=True)
            elif model is baseline.OrderLine:
                predicate = table.c.order_id.startswith(prefix + "-", autoescape=True)
            else:
                predicate = table.c.id.startswith(prefix + "-", autoescape=True)
            values = [
                dict(row) for row in session.execute(select(table).where(predicate)).mappings()
            ]
            rows[table.name] = sorted(values, key=canonical)
    return json.loads(canonical(rows))


def workload(prefix, cases):
    result = [
        {
            "name": f"order-lines-{number}",
            "path": f"/api/orders/{prefix}-order-{number}",
            "dimension": "order_line_count",
            "size": number,
        }
        for number in (1, 10, 100)
    ]
    for number in (0, 10, 100):
        result.append(
            {
                "name": f"snapshot-previews-{number}",
                "path": f"/api/conversations/{cases[f'previews-{number}']['conversation']}",
                "dimension": "preview_count",
                "size": number,
            }
        )
    for number in (10, 100, 1000):
        case = cases["previews-0" if number == 10 else f"messages-{number}"]
        result.append(
            {
                "name": f"snapshot-messages-{number}",
                "path": f"/api/conversations/{case['conversation']}",
                "dimension": "message_count",
                "size": number,
            }
        )
    return result


def load_pair(path):
    directory = runtime_path(path)
    require(not (directory / "cleanup.json").exists(), "PAIR_ALREADY_CLEANED")
    pair = json.loads((directory / "pair.json").read_text())
    require(
        pair["format"] == FORMAT and PREFIX_PATTERN.fullmatch(pair["prefix"]),
        "PAIR_MANIFEST_INVALID",
    )
    require(pair["harness_sha256"] == sha(Path(__file__).read_bytes()), "PAIR_HARNESS_CHANGED")
    return directory, pair


def prepare(args, output):
    # Seed determines all measured entity IDs. A's Event IDs are generated once
    # and then retained/fingerprinted with the pair; no regeneration between runs.
    prefix = "benchmark_d1_" + sha(args.seed.encode())[:16]
    intent = {
        "format": FORMAT,
        "prefix": prefix,
        "seed": args.seed,
        "harness_sha256": sha(Path(__file__).read_bytes()),
    }
    with database(args.database_url) as sessions:
        require(
            not any(fixture_projection(sessions, prefix).values()), "FIXTURE_PREFIX_ALREADY_EXISTS"
        )
        # Only an absent prefix grants cleanup ownership to this pair.
        new_json(output / "intent.json", intent)
        before = baseline.counts(sessions)
        cases = baseline.fixture(sessions, prefix)
        projection = fixture_projection(sessions, prefix)
        new_json(output / "fixture-projection.json", projection)
        pair = {
            **intent,
            "status": "PREPARED",
            "fixed_business_clock": baseline.FIXED.isoformat(),
            "event_id_policy": "Stage-A high IDs generated once; retained unchanged for both runs",
            "cases": cases,
            "workload": workload(prefix, cases),
            "duplicate_control": "snapshot-messages-10 and snapshot-previews-0 use the same path",
            "fixture_fingerprint": fingerprint(projection),
            "fixture_rows": {name: len(values) for name, values in projection.items()},
            "table_counts_before_prepare": before,
            "table_counts_with_fixture": baseline.counts(sessions),
            "source_at_prepare": source_info(),
        }
        new_json(output / "pair.json", pair)
    return {"status": "PREPARED", "pair": str(output / "pair.json"), "prefix": prefix}


class Recorder(baseline.SQLRecorder):
    def __init__(self, engine):
        self.executions = []
        super().__init__(engine)

    def before(self, conn, cursor, statement, parameters, context, executemany):
        super().before(conn, cursor, statement, parameters, context, executemany)
        if self.enabled and not executemany:
            self.executions.append((statement, deepcopy(parameters)))


def request_sample(client, recorder, path, index):
    recorder.calls.clear()
    recorder.executions.clear()
    recorder.enabled = True
    started = time.perf_counter()
    response, error = None, None
    try:
        response = client.get(path)
    except Exception as failure:
        error = type(failure).__name__
    finally:
        elapsed = (time.perf_counter() - started) * 1000
        recorder.enabled = False
    body, semantic_hash = None, None
    if response is not None:
        try:
            body = response.json()
            semantic_hash = fingerprint(body)
        except (ValueError, UnicodeError):
            error = "NON_JSON_RESPONSE"
    successful = response is not None and response.status_code == 200 and error is None
    return {
        "index": index,
        "ok": successful,
        "http_status": response.status_code if response is not None else None,
        "duration_ms": elapsed,
        "application_sql_count": len(recorder.calls),
        "response_bytes": len(response.content) if response is not None else 0,
        "response_semantic_sha256": semantic_hash,
        "error_code": error if error is not None else None if successful else "HTTP_STATUS_NOT_200",
    }, body


def measure_case(client, recorder, spec, samples, warmup, output):
    warmups, values, body = [], [], None
    for index in range(warmup):
        value, _ = request_sample(client, recorder, spec["path"], index)
        warmups.append(value)
    statements, executions = [], []
    for index in range(samples):
        value, payload = request_sample(client, recorder, spec["path"], index)
        values.append(value)
        if index == 0:
            body = payload
            statements = list(recorder.calls)
            executions = list(recorder.executions)
    successes = [value for value in values if value["ok"]]
    hashes = {value["response_semantic_sha256"] for value in [*warmups, *values]}
    stable = len(hashes) == 1 and None not in hashes
    new_json(output / "responses" / (spec["name"] + ".json"), body)
    metrics = {
        **spec,
        "samples_requested": samples,
        "samples_completed": len(values),
        "samples_failed": samples - len(successes),
        "warmup_requested": warmup,
        "warmup_failed": sum(not value["ok"] for value in warmups),
        "response_semantics_stable": stable,
        "response_semantic_sha256": next(iter(hashes)) if stable else None,
        "warmup_samples": warmups,
        "samples": values,
        "summary_includes_successful_samples_only": True,
        "sql_fingerprints_one_request": [
            {"statement": statement, "count": count}
            for statement, count in Counter(statements).items()
        ],
    }
    if successes:
        durations = [value["duration_ms"] for value in successes]
        metrics["asgi_latency_ms"] = {
            "p50": round(statistics.median(durations), 3),
            "p95": round(baseline.percentile(durations, 0.95), 3),
        }
        for name in ("application_sql_count", "response_bytes"):
            amounts = [value[name] for value in successes]
            metrics[name] = {"min": min(amounts), "max": max(amounts)}
    metrics["passed"] = stable and not metrics["samples_failed"] and not metrics["warmup_failed"]
    return metrics, executions


def database_conditions(sessions):
    settings = (
        "server_version",
        "shared_buffers",
        "work_mem",
        "effective_cache_size",
        "random_page_cost",
        "seq_page_cost",
        "jit",
        "max_parallel_workers_per_gather",
        "track_io_timing",
        "TimeZone",
        "default_transaction_isolation",
        "statement_timeout",
        "lock_timeout",
        "client_encoding",
    )
    names = [model.__tablename__ for model in baseline.TABLES]
    with sessions() as session:
        result = {
            name: session.scalar(text("SELECT current_setting(:name)"), {"name": name})
            for name in settings
        }
        stats = (
            session.execute(
                text(
                    "SELECT relname,n_live_tup,n_dead_tup,last_analyze,last_autoanalyze,"
                    "analyze_count,"
                    "autoanalyze_count FROM pg_stat_user_tables WHERE schemaname='public' "
                    "AND relname=ANY(:names) ORDER BY relname"
                ),
                {"names": names},
            )
            .mappings()
            .all()
        )
        indexes = (
            session.execute(
                text(
                    "SELECT tablename,indexname,indexdef FROM pg_indexes WHERE schemaname='public' "
                    "AND tablename=ANY(:names) ORDER BY tablename,indexname"
                ),
                {"names": names},
            )
            .mappings()
            .all()
        )
    return {
        "settings": result,
        "table_statistics": [dict(row) for row in stats],
        "indexes": [dict(row) for row in indexes],
        "analyze_performed_by_harness": False,
        "cache_policy": "3 warmups by default; no cache flush; plans collected after measurements",
    }


def explain(sessions, examples):
    results = []
    with sessions.kw["bind"].connect() as connection:
        connection.execute(text("SET TRANSACTION READ ONLY"))
        for case, executions in examples.items():
            seen = set()
            for statement, parameters in executions:
                normalized = " ".join(statement.split())
                if (
                    normalized in seen
                    or not normalized.upper().startswith("SELECT ")
                    or "FOR UPDATE" in normalized.upper()
                    or not re.search(
                        r"\b(?:order_lines|variants|messages|action_previews|events)\b", normalized
                    )
                ):
                    continue
                seen.add(normalized)
                plan = connection.exec_driver_sql(
                    "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + statement, parameters
                ).scalar()
                results.append({"case": case, "actual_statement": normalized, "plan": plan})
    return results


def measure(args, output):
    pair_dir, pair = load_pair(args.pair_dir)
    report = {
        "format": FORMAT,
        "stage": "D1",
        "label": args.label,
        "status": "RUNNING",
        "pair_manifest": str(pair_dir / "pair.json"),
        "pair_manifest_sha256": sha((pair_dir / "pair.json").read_bytes()),
        "fixture_fingerprint": pair["fixture_fingerprint"],
        "seed": pair["seed"],
        "fixed_business_clock": pair["fixed_business_clock"],
        "fixture_rows": pair["fixture_rows"],
        "duplicate_control": pair["duplicate_control"],
        "workload": pair["workload"],
        "samples_per_case": args.samples,
        "warmup_per_case": args.warmup,
        "concurrency": 1,
        "percentile_method": "p50=median; p95=nearest rank ceil(n*0.95)",
        "measurement_scope": "sequential in-process TestClient/ASGI; not TCP/QPS",
        "sql_count_scope": (
            "SQLAlchemy cursor executions including auth; excludes driver ping/transaction IO"
        ),
        "database_target": "127.0.0.1:55434/aftercare_test",
        "schema_revision": REVISION,
        "real_model_requests": 0,
        "source": source_info(),
        "host": {
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
            "python": platform.python_version(),
        },
        "measurements": [],
    }
    token = secrets.token_urlsafe(32)
    session_hash = digest(token)
    recorder = None
    try:
        with database(args.database_url) as sessions:
            require(
                fingerprint(fixture_projection(sessions, pair["prefix"]))
                == pair["fixture_fingerprint"],
                "FIXTURE_CHANGED_OR_MISSING",
            )
            initial_counts = baseline.counts(sessions)
            require(
                initial_counts == pair["table_counts_with_fixture"], "DATABASE_POPULATION_CHANGED"
            )
            report["table_counts"] = initial_counts
            report["database_conditions"] = database_conditions(sessions)
            try:
                with sessions.begin() as session:
                    session.add(
                        DemoSession(
                            token_hash=session_hash,
                            customer_id="alice",
                            csrf_token=secrets.token_urlsafe(32),
                            expires_at=utc_now() + timedelta(hours=24),
                        )
                    )
                app = create_app(
                    Settings(
                        _env_file=None,
                        app_env="test",
                        database_url=args.database_url,
                        demo_login_enabled=True,
                        warehouse_delivery_enabled=False,
                        warehouse_simulation_enabled=False,
                        warehouse_callbacks_enabled=False,
                    )
                )
                app.state.business = BusinessService(sessions, clock=lambda: baseline.FIXED)
                recorder = Recorder(sessions.kw["bind"])
                (output / "responses").mkdir()
                examples = {}
                with TestClient(app) as client:
                    client.cookies.set("aftercare_session", token)
                    for spec in pair["workload"]:
                        metrics, executions = measure_case(
                            client, recorder, spec, args.samples, args.warmup, output
                        )
                        report["measurements"].append(metrics)
                        if spec["name"] in {
                            "order-lines-100",
                            "snapshot-previews-100",
                            "snapshot-messages-1000",
                        }:
                            examples[spec["name"]] = executions
                recorder.close()
                recorder = None
                report["execution_plans"] = explain(sessions, examples)
            finally:
                if recorder is not None:
                    recorder.close()
                with sessions.begin() as session:
                    session.execute(
                        delete(DemoSession).where(DemoSession.token_hash == session_hash)
                    )
                final_counts = baseline.counts(sessions)
                report["session_cleanup"] = {
                    "removed": True,
                    "table_counts_match": final_counts == initial_counts,
                }
                report["fixture_unchanged"] = (
                    fingerprint(fixture_projection(sessions, pair["prefix"]))
                    == pair["fixture_fingerprint"]
                )
            require(
                report["session_cleanup"]["table_counts_match"] and report["fixture_unchanged"],
                "BENCHMARK_MUTATED_PERSISTENT_DATA",
            )
            require(source_info() == report["source"], "SOURCE_CHANGED_DURING_MEASUREMENT")
            report["status"] = (
                "PASSED" if all(item["passed"] for item in report["measurements"]) else "FAILED"
            )
    except Exception as failure:
        report["status"] = "FAILED"
        report["error_code"] = (
            str(failure)
            if isinstance(failure, RuntimeError) and re.fullmatch(r"[A-Z_]+", str(failure))
            else type(failure).__name__
        )
    require(token not in canonical(report), "SECRET_IN_REPORT")
    new_json(output / "result.json", report)
    return {"status": report["status"], "result": str(output / "result.json")}


def compare(args, output):
    before = json.loads(runtime_path(args.before).read_text())
    after = json.loads(runtime_path(args.after).read_text())
    require(before["label"] == "before" and after["label"] == "after", "PAIR_LABELS_INVALID")
    require(before["status"] == after["status"] == "PASSED", "PAIR_HAS_FAILED_MEASUREMENTS")
    equal_fields = (
        "format",
        "pair_manifest_sha256",
        "fixture_fingerprint",
        "seed",
        "fixed_business_clock",
        "fixture_rows",
        "workload",
        "samples_per_case",
        "warmup_per_case",
        "concurrency",
        "host",
        "database_target",
        "schema_revision",
        "table_counts",
        "sql_count_scope",
        "measurement_scope",
    )
    for name in equal_fields:
        require(before[name] == after[name], "PAIR_CONDITIONS_DIFFER")
    for name in ("harness_sha256", "stage_A_harness_sha256", "requirements_sha256"):
        require(
            before["source"][name] == after["source"][name], "PAIR_HARNESS_OR_DEPENDENCIES_CHANGED"
        )
    require(
        before["database_conditions"]["settings"] == after["database_conditions"]["settings"],
        "PAIR_POSTGRESQL_SETTINGS_CHANGED",
    )
    comparisons = []
    require(len(before["measurements"]) == len(after["measurements"]), "PAIR_CASES_DIFFER")
    for old, new in zip(before["measurements"], after["measurements"], strict=True):
        require(old["name"] == new["name"], "PAIR_CASE_ORDER_CHANGED")
        require(
            old["response_semantic_sha256"] == new["response_semantic_sha256"],
            "PAIR_RESPONSE_SEMANTICS_CHANGED",
        )
        comparisons.append(
            {
                "name": old["name"],
                "response_semantics_equal": True,
                "before": {
                    key: old[key]
                    for key in ("asgi_latency_ms", "application_sql_count", "response_bytes")
                },
                "after": {
                    key: new[key]
                    for key in ("asgi_latency_ms", "application_sql_count", "response_bytes")
                },
                "p50_reduction_percent": round(
                    100 * (1 - new["asgi_latency_ms"]["p50"] / old["asgi_latency_ms"]["p50"]), 2
                )
                if old["asgi_latency_ms"]["p50"]
                else None,
            }
        )
    result = {
        "format": FORMAT,
        "status": "COMPARABLE",
        "comparisons": comparisons,
        "before_sha256": sha(runtime_path(args.before).read_bytes()),
        "after_sha256": sha(runtime_path(args.after).read_bytes()),
        "statistics_changed": before["database_conditions"]["table_statistics"]
        != after["database_conditions"]["table_statistics"],
        "indexes_changed": before["database_conditions"]["indexes"]
        != after["database_conditions"]["indexes"],
        "limits": "single sequential ASGI pair; no QPS, lock or SSE conclusions",
    }
    new_json(output / "comparison.json", result)
    return {"status": result["status"], "comparison": str(output / "comparison.json")}


def cleanup(args, output):
    directory = runtime_path(args.pair_dir)
    require(not (directory / "cleanup.json").exists(), "PAIR_ALREADY_CLEANED")
    path = directory / "pair.json"
    pair = json.loads((path if path.exists() else directory / "intent.json").read_text())
    require(
        pair["format"] == FORMAT and PREFIX_PATTERN.fullmatch(pair["prefix"]),
        "PAIR_MANIFEST_INVALID",
    )
    with database(args.database_url) as sessions:
        before = baseline.counts(sessions)
        baseline.cleanup(sessions, pair["prefix"], "no-session-for-" + pair["prefix"])
        after = baseline.counts(sessions)
        require(
            not any(fixture_projection(sessions, pair["prefix"]).values()),
            "FIXTURE_CLEANUP_INCOMPLETE",
        )
    result = {
        "status": "CLEANED",
        "prefix": pair["prefix"],
        "table_counts_before": before,
        "table_counts_after": after,
        "restored_prepare_counts": after == pair.get("table_counts_before_prepare")
        if path.exists()
        else None,
    }
    new_json(output / "result.json", result)
    new_json(directory / "cleanup.json", result)
    return {"status": "CLEANED", "result": str(output / "result.json")}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", default=baseline.TEST_URL)
    sub = parser.add_subparsers(dest="action", required=True)
    prepare_args = sub.add_parser("prepare")
    prepare_args.add_argument("--pair-dir", type=Path, required=True)
    prepare_args.add_argument("--seed", default="backend-D1-20261029-first")
    measure_args = sub.add_parser("measure")
    measure_args.add_argument("--pair-dir", type=Path, required=True)
    measure_args.add_argument("--output-dir", type=Path, required=True)
    measure_args.add_argument("--label", choices=("before", "after"), required=True)
    measure_args.add_argument("--samples", type=int, choices=range(3, 101), default=30)
    measure_args.add_argument("--warmup", type=int, choices=range(1, 11), default=3)
    compare_args = sub.add_parser("compare")
    compare_args.add_argument("--before", type=Path, required=True)
    compare_args.add_argument("--after", type=Path, required=True)
    compare_args.add_argument("--output-dir", type=Path, required=True)
    cleanup_args = sub.add_parser("cleanup")
    cleanup_args.add_argument("--pair-dir", type=Path, required=True)
    cleanup_args.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    require(
        os.environ.get("ALLOW_REAL_MODEL_CALLS") == "false", "EXPLICIT_OFFLINE_ENVIRONMENT_REQUIRED"
    )
    baseline.validate_target(args.database_url)
    require(args.database_url == baseline.TEST_URL, "EXACT_ISOLATED_TEST_URL_REQUIRED")
    require(
        os.environ.get("DATABASE_URL", baseline.TEST_URL) == baseline.TEST_URL,
        "DATABASE_ENVIRONMENT_MISMATCH",
    )
    os.umask(0o077)
    install_offline_guard()
    output = new_directory(args.pair_dir if args.action == "prepare" else args.output_dir)
    try:
        result = {"prepare": prepare, "measure": measure, "compare": compare, "cleanup": cleanup}[
            args.action
        ](args, output)
    except Exception as failure:
        value = str(failure) if isinstance(failure, RuntimeError) else type(failure).__name__
        result = {
            "status": "FAILED",
            "action": args.action,
            "error_code": value if re.fullmatch(r"[A-Z_]+", value) else type(failure).__name__,
        }
        new_json(output / "failure.json", result)
    print(json.dumps(result, ensure_ascii=False), flush=True)
    return 1 if result["status"] == "FAILED" else 0


if __name__ == "__main__":
    raise SystemExit(main())
