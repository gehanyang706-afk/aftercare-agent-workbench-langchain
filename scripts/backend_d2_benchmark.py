"""D2 paired snapshot/SSE measurements on isolated 55434/test, without product edits.

prepare -> measure before -> measure after -> compare -> cleanup. Retains one
fixed Stage-A fixture. Normal reads use ASGI; long streams use real loopback TCP.
Row-lock probes use actual PG transactions and independently observed waiters.
No migrations, truncation, models, external services, or arbitrary load claims.
"""

import argparse
import asyncio
import hashlib
import json
import logging
import os
import platform
import re
import secrets
import socket
import statistics
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path

import httpx
import uvicorn
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, delete, event, text
from sqlalchemy.orm import sessionmaker

from aftercare.api.app import create_app
from aftercare.config import Settings
from aftercare.domain.contracts import utc_now
from aftercare.offline import install_offline_guard
from aftercare.repositories.models import DemoSession
from aftercare.services.business import BusinessService, digest
from scripts import backend_d1_benchmark as core

FORMAT = "backend-d2-paired-v1"
PREFIX = re.compile(r"benchmark_d2_[0-9a-f]{16}\Z")
PORT = 8136
LOCK_MS = 300
STATEMENT_MS = 2000


def source_info():
    info = core.source_info()
    info["d1_harness_sha256"] = info.pop("harness_sha256")
    info["harness_sha256"] = core.sha(Path(__file__).read_bytes())
    assets = [
        *sorted((core.PROJECT_ROOT / "src/aftercare/web/static").glob("*.js")),
        core.PROJECT_ROOT / "src/aftercare/web/templates/index.html",
    ]
    info["web_entries"] = {
        str(path.relative_to(core.PROJECT_ROOT)): core.sha(path.read_bytes()) for path in assets
    }
    return info


def load_pair(path):
    directory = core.runtime_path(path)
    core.require(not (directory / "cleanup.json").exists(), "PAIR_ALREADY_CLEANED")
    pair = json.loads((directory / "pair.json").read_text())
    core.require(pair["format"] == FORMAT and PREFIX.fullmatch(pair["prefix"]), "PAIR_INVALID")
    core.require(pair["harness_sha256"] == source_info()["harness_sha256"], "HARNESS_CHANGED")
    return directory, pair


def prepare(args, output):
    prefix = "benchmark_d2_" + hashlib.sha256(args.seed.encode()).hexdigest()[:16]
    with core.database(args.database_url) as sessions:
        core.require(not any(core.fixture_projection(sessions, prefix).values()), "PREFIX_EXISTS")
        counts = core.baseline.counts(sessions)
        cases = core.baseline.fixture(sessions, prefix)
        projection = core.fixture_projection(sessions, prefix)
        core.new_json(output / "fixture-projection.json", projection)
        pair = {
            "format": FORMAT,
            "prefix": prefix,
            "seed": args.seed,
            "cases": cases,
            "fixture_fingerprint": core.fingerprint(projection),
            "fixture_rows": {name: len(rows) for name, rows in projection.items()},
            "table_counts_before_prepare": counts,
            "table_counts_with_fixture": core.baseline.counts(sessions),
            "fixed_business_clock": core.baseline.FIXED.isoformat(),
            "harness_sha256": source_info()["harness_sha256"],
            "source_at_prepare": source_info(),
        }
        core.new_json(output / "pair.json", pair)
    return {"status": "PREPARED", "pair": str(output / "pair.json")}


class Recorder(core.Recorder):
    """Group SQL counter shared by server/client threads; never per-connection attribution."""

    def __init__(self, engine):
        self.metric_lock = threading.Lock()
        self.peak_checked_out = 0
        self.checked_out = 0
        super().__init__(engine)
        event.listen(engine, "checkout", self.checkout)
        event.listen(engine, "checkin", self.checkin)

    def before(self, *args):
        with self.metric_lock:
            super().before(*args)

    def checkout(self, *args):
        with self.metric_lock:
            self.checked_out += 1
            if self.enabled:
                self.peak_checked_out = max(self.peak_checked_out, self.checked_out)

    def checkin(self, *args):
        with self.metric_lock:
            self.checked_out -= 1

    def start(self):
        with self.metric_lock:
            self.calls.clear()
            self.executions.clear()
            self.peak_checked_out = 0
            self.enabled = True

    def snapshot(self):
        with self.metric_lock:
            return {
                "application_sql_count": len(self.calls),
                "locking_sql_count": sum("FOR UPDATE" in sql.upper() for sql in self.calls),
                "peak_checked_out": self.peak_checked_out,
                "checked_out_at_observation": self.checked_out,
                "sql_fingerprints": [
                    {"statement": sql, "count": count} for sql, count in Counter(self.calls).items()
                ],
            }

    def close(self):
        event.remove(self.engine, "checkout", self.checkout)
        event.remove(self.engine, "checkin", self.checkin)
        super().close()


@contextmanager
def application(sessions, token):
    app_sessions = sessionmaker(
        create_engine(
            core.baseline.TEST_URL,
            connect_args={
                "connect_timeout": 2,
                "options": f"-c lock_timeout={LOCK_MS}ms -c statement_timeout={STATEMENT_MS}ms",
            },
            pool_pre_ping=True,
            pool_size=8,
            max_overflow=2,
        ),
        expire_on_commit=False,
    )
    app = create_app(
        Settings(
            _env_file=None,
            app_env="test",
            database_url=core.baseline.TEST_URL,
            demo_login_enabled=True,
            warehouse_delivery_enabled=False,
            warehouse_simulation_enabled=False,
            warehouse_callbacks_enabled=False,
        )
    )
    app.state.business = BusinessService(app_sessions, clock=lambda: core.baseline.FIXED)
    token_hash = digest(token)
    with sessions.begin() as session:
        session.add(
            DemoSession(
                token_hash=token_hash,
                customer_id="alice",
                csrf_token=secrets.token_urlsafe(32),
                expires_at=utc_now() + timedelta(hours=24),
            )
        )
    try:
        yield app, app_sessions
    finally:
        app_sessions.kw["bind"].dispose()
        with sessions.begin() as session:
            session.execute(delete(DemoSession).where(DemoSession.token_hash == token_hash))


def parse_sse(raw):
    result = []
    for block in raw.replace("\r\n", "\n").split("\n\n"):
        fields = {}
        for line in block.splitlines():
            if ":" in line and not line.startswith(":"):
                key, value = line.split(":", 1)
                fields[key] = value.lstrip()
        if fields.get("event") == "trace" and "data" in fields:
            result.append({"id": int(fields["id"]), "data": json.loads(fields["data"])})
    return result


def finite_sse(client, recorder, name, path, headers, samples, output):
    values, bodies = [], []
    for index in range(samples + 3):
        recorder.start()
        started = time.perf_counter()
        error, response = None, None
        try:
            response = client.get(path, headers=headers)
            traces = parse_sse(response.text)
        except Exception as failure:
            error, traces = type(failure).__name__, []
        recorder.enabled = False
        measurement = {
            "index": index - 3,
            "ok": response is not None and response.status_code == 200 and error is None,
            "http_status": response.status_code if response is not None else None,
            "error_code": error,
            "duration_ms": (time.perf_counter() - started) * 1000,
            "response_bytes": len(response.content) if response is not None else 0,
            "trace_semantic_sha256": core.fingerprint(traces),
            "trace_count": len(traces),
            **recorder.snapshot(),
        }
        values.append(measurement)
        if index == 3:
            bodies = traces
    core.new_json(output / "responses" / (name + ".json"), bodies)
    measured = values[3:]
    durations = [item["duration_ms"] for item in measured if item["ok"]]
    summary = {
        "name": name,
        "path": path,
        "headers": headers,
        "samples_requested": samples,
        "samples_completed": len(measured),
        "samples_failed": sum(not item["ok"] for item in measured),
        "warmup_failed": sum(not item["ok"] for item in values[:3]),
        "trace_semantic_sha256": core.fingerprint(bodies),
        "trace_count": len(bodies),
        "samples": measured,
        "warmup_samples": values[:3],
        "trace_semantics_stable": len({x["trace_semantic_sha256"] for x in values}) == 1,
    }
    if durations:
        summary["latency_ms"] = {
            "p50": round(statistics.median(durations), 3),
            "p95": round(core.baseline.percentile(durations, 0.95), 3),
        }
    summary["passed"] = (
        not summary["samples_failed"]
        and not summary["warmup_failed"]
        and summary["trace_semantics_stable"]
    )
    return summary


@contextmanager
def tcp_server(app):
    with socket.socket() as probe:
        # Match the actual server's reuse policy: allow prior TIME_WAIT sockets,
        # but do not enable REUSEPORT or share another live listener.
        probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        probe.bind(("127.0.0.1", PORT))
    # Exception logs can contain SQL parameters. This private measurement server
    # suppresses logging; only safe exception class/status and SQLSTATE are saved.
    names = ("uvicorn", "uvicorn.error", "uvicorn.access", "sqlalchemy.engine")
    previous = {name: logging.getLogger(name).disabled for name in names}
    for name in names:
        logging.getLogger(name).disabled = True
    server = uvicorn.Server(
        uvicorn.Config(
            app,
            host="127.0.0.1",
            port=PORT,
            log_level="critical",
            access_log=False,
            timeout_graceful_shutdown=2,
        )
    )
    thread = threading.Thread(target=server.run, name="d2-measurement-http", daemon=True)
    thread.start()
    deadline = time.monotonic() + 5
    try:
        while not server.started:
            core.require(thread.is_alive() and time.monotonic() < deadline, "SERVER_START_FAILED")
            time.sleep(0.01)
        yield f"http://127.0.0.1:{PORT}"
    finally:
        server.should_exit = True
        thread.join(timeout=5)
        if thread.is_alive():
            server.force_exit = True
            thread.join(timeout=2)
        for name, disabled in previous.items():
            logging.getLogger(name).disabled = disabled
        core.require(not thread.is_alive(), "SERVER_CLEANUP_FAILED")


async def stream_case(base_url, token, path, seconds, connections, stop_at=None):
    async def consume(client, number):
        value = {"connection": number, "http_status": None, "trace_ids": [], "response_bytes": 0}

        async def read():
            raw = ""
            async with client.stream("GET", path) as response:
                value["http_status"] = response.status_code
                async for chunk in response.aiter_text():
                    value["response_bytes"] += len(chunk.encode())
                    raw += chunk
                    traces = parse_sse(raw)
                    value["trace_ids"] = [item["id"] for item in traces]
                    value["trace_semantic_sha256"] = core.fingerprint(traces)
                    if stop_at is not None and len(traces) >= stop_at:
                        return

        started = time.perf_counter()
        try:
            await asyncio.wait_for(read(), timeout=seconds)
            value["stop_reason"] = "EXPECTED_TRACE_COUNT" if stop_at is not None else "EOF"
        except TimeoutError:
            value["stop_reason"] = "WINDOW_END"
        except Exception as error:
            value["stop_reason"] = type(error).__name__
        value["elapsed_ms"] = (time.perf_counter() - started) * 1000
        return value

    async with httpx.AsyncClient(
        base_url=base_url,
        cookies={"aftercare_session": token},
        timeout=4,
        trust_env=False,
        limits=httpx.Limits(max_connections=8),
    ) as client:
        return await asyncio.gather(*(consume(client, i) for i in range(connections)))


def stream_windows(base_url, token, pair, recorder, seconds):
    data = pair["cases"]["previews-0"]
    path = f"/api/conversations/{data['conversation']}/events?after={data['last_event']}"
    results = []
    for connections in (1, 4):
        recorder.start()
        values = asyncio.run(stream_case(base_url, token, path, seconds, connections))
        at_close = recorder.snapshot()
        time.sleep(0.7)
        settled = recorder.snapshot()
        time.sleep(0.7)
        final = recorder.snapshot()
        recorder.enabled = False
        results.append(
            {
                "connections": connections,
                "window_seconds": seconds,
                "sql_count_scope": "group total across all connections, not per connection",
                "clients": values,
                "at_client_close": at_close,
                "after_disconnect_settled": settled,
                "after_disconnect_recheck": final,
                "query_count_stopped": settled["application_sql_count"]
                == final["application_sql_count"],
                "pool_returned": final["checked_out_at_observation"] == 0,
            }
        )
    return results


def lock_probes(raw, base_url, token, conversation, app_sessions):
    observer = create_engine(raw, connect_args={"connect_timeout": 2})
    results = []
    try:
        for name in ("external_snapshot", "sse_once", "internal_snapshot"):
            with observer.connect() as holder:
                holder.execute(
                    text("UPDATE conversations SET revision=revision WHERE id=:id"),
                    {"id": conversation},
                )
                holder_pid = holder.scalar(text("SELECT pg_backend_pid()"))

                def request(name=name):
                    started = time.perf_counter()
                    if name == "internal_snapshot":
                        try:
                            BusinessService(
                                app_sessions, clock=lambda: core.baseline.FIXED
                            ).snapshot("alice", conversation)
                            outcome = {"outcome": "ok", "sqlstate": None}
                        except Exception as failure:
                            outcome = {
                                "outcome": "error",
                                "error_code": type(failure).__name__,
                                "sqlstate": getattr(
                                    getattr(failure, "orig", None), "sqlstate", None
                                ),
                            }
                    else:
                        path = f"/api/conversations/{conversation}"
                        if name == "sse_once":
                            path += "/events?once=true"
                        try:
                            with httpx.Client(
                                base_url=base_url,
                                cookies={"aftercare_session": token},
                                timeout=4,
                                trust_env=False,
                            ) as client:
                                response = client.get(path)
                            outcome = {
                                "outcome": "ok" if response.status_code == 200 else "http_error",
                                "http_status": response.status_code,
                            }
                        except Exception as failure:
                            outcome = {"outcome": "error", "error_code": type(failure).__name__}
                    outcome["elapsed_ms"] = (time.perf_counter() - started) * 1000
                    return outcome

                waits = []
                with ThreadPoolExecutor(max_workers=1) as pool:
                    future = pool.submit(request)
                    deadline = time.monotonic() + 4
                    try:
                        with observer.connect() as check:
                            while not future.done() and time.monotonic() < deadline:
                                observed = check.execute(
                                    text(
                                        "SELECT pid,wait_event_type,wait_event,"
                                        "pg_blocking_pids(pid) AS blockers FROM pg_stat_activity "
                                        "WHERE datname=current_database() AND :holder="
                                        "ANY(pg_blocking_pids(pid))"
                                    ),
                                    {"holder": holder_pid},
                                ).mappings()
                                waits.extend(dict(row) for row in observed)
                                check.commit()
                                time.sleep(0.01)
                        outcome = future.result(timeout=5)
                    finally:
                        holder.rollback()
                results.append(
                    {
                        "name": name,
                        "holder_pid": holder_pid,
                        "observed_waits": list({core.canonical(x): x for x in waits}.values()),
                        **outcome,
                    }
                )
    finally:
        observer.dispose()
    return results


def measure(args, output):
    _, pair = load_pair(args.pair_dir)
    token = secrets.token_urlsafe(32)
    report = {
        "format": FORMAT,
        "status": "RUNNING",
        "label": args.label,
        "source": source_info(),
        "fixture_fingerprint": pair["fixture_fingerprint"],
        "pair_sha256": core.sha((core.runtime_path(args.pair_dir) / "pair.json").read_bytes()),
        "samples_per_case": args.samples,
        "warmup_per_case": 3,
        "stream_window_seconds": args.stream_seconds,
        "database_target": "127.0.0.1:55434/aftercare_test",
        "schema_revision": core.REVISION,
        "host": {
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
            "python": platform.python_version(),
        },
        "lock_timeout_ms": LOCK_MS,
        "statement_timeout_ms": STATEMENT_MS,
        "real_model_requests": 0,
        "snapshots": [],
        "finite_sse": [],
        "limits": [
            "Sequential ASGI latency is not TCP throughput, QPS, SLA or arbitrary timing coverage",
            "Idle stream SQL is group total; startup/authentication and polling are included",
            "Real row-lock failures are retained baseline outcomes, not successful-read latency",
        ],
    }
    try:
        with core.database(args.database_url) as sessions:
            core.require(
                core.fingerprint(core.fixture_projection(sessions, pair["prefix"]))
                == pair["fixture_fingerprint"],
                "FIXTURE_CHANGED",
            )
            core.require(
                core.baseline.counts(sessions) == pair["table_counts_with_fixture"],
                "DATABASE_POPULATION_CHANGED",
            )
            with application(sessions, token) as (app, app_sessions):
                report["database_conditions"] = core.database_conditions(app_sessions)
                recorder = Recorder(app_sessions.kw["bind"])
                try:
                    (output / "responses").mkdir()
                    with TestClient(app, raise_server_exceptions=False) as client:
                        client.cookies.set("aftercare_session", token)
                        for name in (
                            "previews-0",
                            "previews-10",
                            "previews-100",
                            "messages-100",
                            "messages-1000",
                        ):
                            spec = {
                                "name": "snapshot-" + name,
                                "path": f"/api/conversations/{pair['cases'][name]['conversation']}",
                            }
                            metric, _ = core.measure_case(
                                client, recorder, spec, args.samples, 3, output
                            )
                            report["snapshots"].append(metric)
                        small, many = pair["cases"]["previews-0"], pair["cases"]["events-1000"]
                        work = [
                            ("empty-after-last", small, small["last_event"], {}),
                            ("ten-events", small, 0, {}),
                            ("hundred-events", pair["cases"]["events-100"], 0, {}),
                            ("first-hundred-of-thousand", many, 0, {}),
                            (
                                "last-event-id-reconnect",
                                many,
                                0,
                                {"Last-Event-ID": str(many["last_event"] - 900)},
                            ),
                        ]
                        for name, case, after, headers in work:
                            path = (
                                f"/api/conversations/{case['conversation']}/events"
                                f"?once=true&after={after}"
                            )
                            report["finite_sse"].append(
                                finite_sse(
                                    client, recorder, name, path, headers, args.samples, output
                                )
                            )
                    with tcp_server(app) as base_url:
                        report["idle_streams"] = stream_windows(
                            base_url, token, pair, recorder, args.stream_seconds
                        )
                        many = pair["cases"]["events-1000"]
                        recorder.start()
                        report["backlog_stream"] = {
                            "expected_events": 1000,
                            "clients": asyncio.run(
                                stream_case(
                                    base_url,
                                    token,
                                    f"/api/conversations/{many['conversation']}/events",
                                    8,
                                    1,
                                    1000,
                                )
                            ),
                        }
                        report["backlog_stream"].update(recorder.snapshot())
                        recorder.enabled = False
                        time.sleep(0.7)
                        report["lock_probes"] = lock_probes(
                            args.database_url,
                            base_url,
                            token,
                            pair["cases"]["previews-0"]["conversation"],
                            app_sessions,
                        )
                finally:
                    recorder.close()
            report["fixture_unchanged"] = (
                core.fingerprint(core.fixture_projection(sessions, pair["prefix"]))
                == pair["fixture_fingerprint"]
            )
            report["temporary_session_removed"] = (
                core.baseline.counts(sessions) == pair["table_counts_with_fixture"]
            )
        streams_ok = all(
            item["query_count_stopped"]
            and item["pool_returned"]
            and all(
                client["http_status"] == 200
                and client["trace_ids"] == []
                and client["stop_reason"] == "WINDOW_END"
                for client in item["clients"]
            )
            for item in report["idle_streams"]
        )
        last = pair["cases"]["events-1000"]["last_event"]
        backlog_ok = report["backlog_stream"]["clients"][0]["trace_ids"] == list(
            range(last - 999, last + 1)
        )
        internal = next(x for x in report["lock_probes"] if x["name"] == "internal_snapshot")
        internal_ok = internal.get("sqlstate") == "55P03" and bool(internal["observed_waits"])
        report["stream_windows_passed"] = streams_ok
        report["complete_backlog_passed"] = backlog_ok
        report["internal_lock_retained"] = internal_ok
        core.require(source_info() == report["source"], "SOURCE_CHANGED_DURING_MEASUREMENT")
        report["status"] = (
            "PASSED"
            if all(x["passed"] for x in [*report["snapshots"], *report["finite_sse"]])
            and report["fixture_unchanged"]
            and report["temporary_session_removed"]
            and streams_ok
            and backlog_ok
            and internal_ok
            else "FAILED"
        )
    except Exception as failure:
        report["status"] = "FAILED"
        report["error_code"] = (
            str(failure)
            if isinstance(failure, RuntimeError) and re.fullmatch(r"[A-Z_]+", str(failure))
            else type(failure).__name__
        )
    core.require(token not in core.canonical(report), "SECRET_IN_REPORT")
    core.new_json(output / "result.json", report)
    return {"status": report["status"], "result": str(output / "result.json")}


def compare(args, output):
    before = json.loads(core.runtime_path(args.before).read_text())
    after = json.loads(core.runtime_path(args.after).read_text())
    core.require(before["status"] == after["status"] == "PASSED", "FAILED_PAIR")
    for key in (
        "format",
        "fixture_fingerprint",
        "pair_sha256",
        "samples_per_case",
        "warmup_per_case",
        "stream_window_seconds",
        "schema_revision",
        "lock_timeout_ms",
        "statement_timeout_ms",
        "host",
    ):
        core.require(before[key] == after[key], "PAIR_CONDITIONS_DIFFER")
    for key in (
        "harness_sha256",
        "d1_harness_sha256",
        "stage_A_harness_sha256",
        "requirements_sha256",
    ):
        core.require(
            before["source"][key] == after["source"][key], "HARNESS_OR_DEPENDENCIES_CHANGED"
        )
    core.require(
        before["database_conditions"]["settings"] == after["database_conditions"]["settings"],
        "PG_SETTINGS_CHANGED",
    )
    comparisons = []
    for key, semantic in (
        ("snapshots", "response_semantic_sha256"),
        ("finite_sse", "trace_semantic_sha256"),
    ):
        for old, new in zip(before[key], after[key], strict=True):
            core.require(
                old["name"] == new["name"] and old[semantic] == new[semantic],
                "READ_SEMANTICS_CHANGED",
            )
            comparisons.append({"kind": key, "name": old["name"], "semantic_hash_equal": True})
    core.require(
        all(x["query_count_stopped"] and x["pool_returned"] for x in after["idle_streams"]),
        "STREAM_DISCONNECT_NOT_CLEANED",
    )
    internal = next(x for x in after["lock_probes"] if x["name"] == "internal_snapshot")
    core.require(
        internal.get("sqlstate") == "55P03" and internal["observed_waits"],
        "INTERNAL_LOCK_GUARD_LOST",
    )
    core.require(
        all(
            x["outcome"] == "ok" and not x["observed_waits"]
            for x in after["lock_probes"]
            if x["name"] != "internal_snapshot"
        ),
        "EXTERNAL_READ_STILL_BLOCKED",
    )
    result = {
        "status": "COMPARABLE",
        "semantic_checks": comparisons,
        "before": before,
        "after": after,
    }
    core.new_json(output / "comparison.json", result)
    return {"status": "COMPARABLE", "comparison": str(output / "comparison.json")}


def cleanup(args, output):
    directory, pair = load_pair(args.pair_dir)
    with core.database(args.database_url) as sessions:
        core.baseline.cleanup(sessions, pair["prefix"], "no-session-for-" + pair["prefix"])
        core.require(
            not any(core.fixture_projection(sessions, pair["prefix"]).values()),
            "CLEANUP_INCOMPLETE",
        )
        restored = core.baseline.counts(sessions) == pair["table_counts_before_prepare"]
    result = {"status": "CLEANED", "original_counts_restored": restored, "prefix": pair["prefix"]}
    core.new_json(output / "result.json", result)
    core.new_json(directory / "cleanup.json", result)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", default=core.baseline.TEST_URL)
    sub = parser.add_subparsers(dest="action", required=True)
    p = sub.add_parser("prepare")
    p.add_argument("--pair-dir", type=Path, required=True)
    p.add_argument("--seed", default="backend-D2-20261029-first")
    p = sub.add_parser("measure")
    p.add_argument("--pair-dir", type=Path, required=True)
    p.add_argument("--label", choices=("before", "after"), required=True)
    p.add_argument("--output-dir", type=Path, required=True)
    p.add_argument("--samples", type=int, choices=range(3, 101), default=30)
    p.add_argument("--stream-seconds", type=float, default=3.2)
    p = sub.add_parser("compare")
    p.add_argument("--before", type=Path, required=True)
    p.add_argument("--after", type=Path, required=True)
    p.add_argument("--output-dir", type=Path, required=True)
    p = sub.add_parser("cleanup")
    p.add_argument("--pair-dir", type=Path, required=True)
    p.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    core.require(os.environ.get("ALLOW_REAL_MODEL_CALLS") == "false", "OFFLINE_REQUIRED")
    core.require(args.database_url == core.baseline.TEST_URL, "EXACT_TEST_URL_REQUIRED")
    core.require(
        os.environ.get("DATABASE_URL", args.database_url) == args.database_url,
        "DATABASE_ENV_MISMATCH",
    )
    if args.action == "measure":
        core.require(1 <= args.stream_seconds <= 10, "STREAM_WINDOW_INVALID")
    os.umask(0o077)
    install_offline_guard()
    output = core.new_directory(args.pair_dir if args.action == "prepare" else args.output_dir)
    result = {"prepare": prepare, "measure": measure, "compare": compare, "cleanup": cleanup}[
        args.action
    ](args, output)
    print(json.dumps(result))
    return 1 if result["status"] == "FAILED" else 0


if __name__ == "__main__":
    raise SystemExit(main())
