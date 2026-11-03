"""Stage A: bounded, synthetic read/lock probes in the isolated migration test DB.

No migrations, truncation, model/worker execution, or product behavior changes.
Only this run's prefixed rows and session are removed. Results are not HTTP QPS.
"""

import argparse
import hashlib
import json
import math
import platform
import secrets
import statistics
import subprocess
import threading
import time
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path

import yaml
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, delete, event, func, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import sessionmaker

from aftercare.api.app import create_app
from aftercare.config import PROJECT_ROOT, Settings
from aftercare.domain.contracts import utc_now
from aftercare.offline import install_offline_guard
from aftercare.repositories.database import BASELINE_REVISION, session_factory
from aftercare.repositories.models import (
    Conversation,
    Customer,
    DemoSession,
    Event,
    Inventory,
    Job,
    Message,
    Order,
    OrderLine,
    Policy,
    Preview,
    Variant,
)
from aftercare.services.business import BusinessService, digest

TEST_URL = "postgresql+psycopg://aftercare:aftercare_local_only@127.0.0.1:55434/aftercare_test"
FIXED = datetime(2026, 10, 27, 4, tzinfo=UTC)
TABLES = [
    Customer,
    Policy,
    Variant,
    Inventory,
    Order,
    OrderLine,
    Conversation,
    Message,
    Preview,
    Job,
    Event,
    DemoSession,
]


def validate_target(raw):
    url = make_url(raw)
    if not (
        url.drivername == "postgresql+psycopg"
        and url.host == "127.0.0.1"
        and url.port == 55434
        and url.database == "aftercare_test"
        and url.username == "aftercare"
        and not url.query
    ):
        raise ValueError("ONLY_127_0_0_1_55434_AFTERCARE_TEST_ALLOWED")
    return url


def counts(sessions):
    with sessions() as s:
        return {m.__tablename__: s.scalar(select(func.count()).select_from(m)) for m in TABLES}


def percentile(values, quantile):
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * quantile) - 1)]


class SQLRecorder:
    def __init__(self, engine):
        self.engine = engine
        self.calls = []
        self.enabled = False
        event.listen(engine, "before_cursor_execute", self.before)

    def before(self, conn, cursor, statement, parameters, context, executemany):
        if self.enabled:
            self.calls.append(" ".join(statement.split()))

    def close(self):
        event.remove(self.engine, "before_cursor_execute", self.before)


def fixture(sessions, prefix):
    """Independent dimensions; invalidated previews are read-history fixtures only."""
    rules = yaml.safe_load((PROJECT_ROOT / "configs/business-rules.v1.yaml").read_text())
    cases = {}
    definitions = [
        ("previews-0", 10, 0, 10),
        ("previews-10", 10, 10, 10),
        ("previews-100", 10, 100, 10),
        ("messages-100", 100, 0, 10),
        ("messages-1000", 1000, 0, 10),
        ("events-100", 10, 0, 100),
        ("events-1000", 10, 0, 1000),
    ]
    # Explicit IDs do not advance the test database's public events sequence.
    event_base = 8_000_000_000_000_000 + int(uuid.uuid4().hex[:8], 16) * 10_000
    with sessions.begin() as s:
        s.add(Policy(id=prefix, rules=rules, content_hash=digest(rules), current=False))
        s.flush()
        for index in range(100):
            vid = f"{prefix}-v{index}"
            s.add(
                Variant(
                    id=vid,
                    product_id=prefix,
                    title="Baseline",
                    color="white",
                    size=str(index),
                    price_fen=100,
                    returnable=True,
                    exchangeable=True,
                    category="ordinary",
                )
            )
        s.flush()
        s.add_all(
            Inventory(variant_id=f"{prefix}-v{i}", available=10, reserved=0, version=1)
            for i in range(100)
        )
        for line_count in (1, 10, 100):
            oid = f"{prefix}-order-{line_count}"
            s.add(
                Order(
                    id=oid,
                    customer_id="alice",
                    status="DELIVERED",
                    paid_fen=100 * line_count,
                    delivered_at=FIXED,
                    version=1,
                    policy_version=prefix,
                )
            )
            s.flush()
            s.add_all(
                OrderLine(
                    id=f"{oid}-{i}",
                    order_id=oid,
                    variant_id=f"{prefix}-v{i}",
                    quantity=1,
                    occupied=0,
                    unused=True,
                )
                for i in range(line_count)
            )
        next_event = event_base
        for name, messages, previews, events in definitions:
            cid = f"{prefix}-{name}"
            s.add(
                Conversation(
                    id=cid, customer_id="alice", revision=messages, status="WAITING_USER", case={}
                )
            )
            s.flush()
            s.add_all(
                Message(
                    id=f"{cid}-m{i}",
                    conversation_id=cid,
                    client_message_id=f"m{i}",
                    role="user",
                    text="Synthetic baseline message",
                    revision=i + 1,
                    created_at=FIXED + timedelta(seconds=i),
                )
                for i in range(messages)
            )
            s.add_all(
                Preview(
                    id=f"{cid}-p{i}",
                    conversation_id=cid,
                    customer_id="alice",
                    order_id=f"{prefix}-order-100",
                    payload={"target_variant": f"{prefix}-v{i}"},
                    parameter_hash=f"synthetic-{i}",
                    revision=1,
                    order_version=1,
                    eligibility_policy=prefix,
                    flow_policy=prefix,
                    policy_hash=digest(rules),
                    status="INVALIDATED",
                    expires_at=FIXED,
                )
                for i in range(previews)
            )
            ids = []
            for i in range(events):
                next_event += 1
                ids.append(next_event)
                s.add(
                    Event(
                        id=next_event,
                        conversation_id=cid,
                        event_type="baseline_trace",
                        payload={"sequence": i},
                        created_at=FIXED + timedelta(seconds=i),
                    )
                )
            cases[name] = {
                "conversation": cid,
                "messages": messages,
                "previews": previews,
                "events": events,
                "last_event": ids[-1],
            }
    return cases


def response_shape(response):
    if "text/event-stream" in response.headers.get("content-type", ""):
        return {"events": sum(line.startswith("data:") for line in response.text.splitlines())}
    body = response.json()
    if isinstance(body, list):
        return {"records": len(body)}
    return {key: len(value) for key, value in body.items() if isinstance(value, list)}


def measure(client, recorder, name, path, samples):
    for _ in range(3):
        response = client.get(path)
        response.raise_for_status()
    durations, sql_counts, sizes, shapes = [], [], [], []
    fingerprints = Counter()
    for _ in range(samples):
        recorder.calls.clear()
        recorder.enabled = True
        started = time.perf_counter()
        try:
            response = client.get(path)
        finally:
            elapsed = (time.perf_counter() - started) * 1000
            recorder.enabled = False
        response.raise_for_status()
        durations.append(elapsed)
        sql_counts.append(len(recorder.calls))
        sizes.append(len(response.content))
        shapes.append(response_shape(response))
        if not fingerprints:
            fingerprints.update(recorder.calls)
    if any(shape != shapes[0] for shape in shapes):
        raise RuntimeError("UNSTABLE_RESPONSE_SHAPE")
    return {
        "name": name,
        "samples": samples,
        "warmup": 3,
        "asgi_latency_ms": {
            "p50": round(statistics.median(durations), 3),
            "p95": round(percentile(durations, 0.95), 3),
        },
        "application_sql_count": {"min": min(sql_counts), "max": max(sql_counts)},
        "response_bytes": {"min": min(sizes), "max": max(sizes)},
        "response_shape": shapes[0],
        "sql_fingerprints_one_request": [
            {"statement": statement, "count": n} for statement, n in fingerprints.items()
        ],
    }


def lock_probes(raw, conversation):
    engine = create_engine(
        raw,
        connect_args={
            "connect_timeout": 2,
            "options": "-c lock_timeout=300ms -c statement_timeout=2000ms",
        },
    )
    sessions = sessionmaker(engine, expire_on_commit=False)
    service = BusinessService(sessions)
    statements = {
        "plain_select": "SELECT revision FROM conversations WHERE id=:id",
        "locking_select": "SELECT revision FROM conversations WHERE id=:id FOR UPDATE",
        "update": "UPDATE conversations SET revision=revision WHERE id=:id",
        "product_snapshot": None,
    }
    results = []
    try:
        for name, statement in statements.items():
            with engine.connect() as holder:
                holder.execute(
                    text("SELECT id FROM conversations WHERE id=:id FOR UPDATE"),
                    {"id": conversation},
                ).one()
                ready = threading.Event()
                pid_box = []

                def contender(statement=statement, pid_box=pid_box, ready=ready):
                    started = time.perf_counter()
                    try:
                        # Bind a single independent connection so its backend PID is observable.
                        with engine.connect() as connection:
                            pid_box.append(connection.scalar(text("SELECT pg_backend_pid()")))
                            connection.commit()
                            ready.set()
                            if statement is None:
                                bound = sessionmaker(connection, expire_on_commit=False)
                                BusinessService(bound).snapshot("alice", conversation)
                            else:
                                result = connection.execute(text(statement), {"id": conversation})
                                if statement.startswith("SELECT"):
                                    result.all()
                            connection.rollback()
                        return {
                            "outcome": "ok",
                            "sqlstate": None,
                            "elapsed_ms": round((time.perf_counter() - started) * 1000, 3),
                        }
                    except DBAPIError as error:
                        return {
                            "outcome": "database_error",
                            "sqlstate": getattr(error.orig, "sqlstate", None),
                            "elapsed_ms": round((time.perf_counter() - started) * 1000, 3),
                        }

                pool = ThreadPoolExecutor(max_workers=1)
                future = pool.submit(contender)
                waits = []
                try:
                    if not ready.wait(timeout=5):
                        raise RuntimeError("LOCK_CONTENDER_NOT_READY")
                    deadline = time.monotonic() + 4
                    with engine.connect() as observer:
                        while not future.done() and time.monotonic() < deadline:
                            row = (
                                observer.execute(
                                    text(
                                        "SELECT wait_event_type, wait_event, "
                                        "cardinality(pg_blocking_pids(pid)) AS blockers "
                                        "FROM pg_stat_activity WHERE pid=:pid"
                                    ),
                                    {"pid": pid_box[0]},
                                )
                                .mappings()
                                .one_or_none()
                            )
                            observer.commit()
                            if row is not None and row["wait_event_type"] == "Lock":
                                waits.append(dict(row))
                            time.sleep(0.01)
                    result = future.result(timeout=5)
                finally:
                    holder.rollback()
                    pool.shutdown(wait=True, cancel_futures=True)
                expected = "ok" if name == "plain_select" else "database_error"
                if result["outcome"] != expected or (
                    name != "plain_select" and result["sqlstate"] != "55P03"
                ):
                    raise RuntimeError(f"UNEXPECTED_LOCK_RESULT:{name}")
                if name != "plain_select" and not waits:
                    raise RuntimeError(f"LOCK_WAIT_NOT_OBSERVED:{name}")
                results.append(
                    {
                        "name": name,
                        **result,
                        "observed_waits": list(
                            {json.dumps(w, sort_keys=True): w for w in waits}.values()
                        ),
                    }
                )
        service.snapshot("alice", conversation)
        return {
            "lock_timeout_ms": 300,
            "statement_timeout_ms": 2000,
            "results": results,
            "snapshot_after_release": "ok",
        }
    finally:
        engine.dispose()


def explain_queries(sessions, prefix, cases):
    queries = {
        "order_lines_100": (
            "SELECT * FROM order_lines WHERE order_id=:value",
            f"{prefix}-order-100",
        ),
        "previews_100": (
            "SELECT * FROM action_previews WHERE conversation_id=:value",
            cases["previews-100"]["conversation"],
        ),
        "sse_first_100_of_1000": (
            "SELECT * FROM events WHERE conversation_id=:value AND id>0 ORDER BY id LIMIT 100",
            cases["events-1000"]["conversation"],
        ),
        "runtime_state_among_1000": (
            "SELECT * FROM events WHERE conversation_id=:value "
            "AND event_type='runtime_state' ORDER BY id DESC",
            cases["events-1000"]["conversation"],
        ),
    }
    with sessions() as s:
        return {
            name: s.scalar(
                text("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + statement), {"value": value}
            )
            for name, (statement, value) in queries.items()
        }


def cleanup(sessions, prefix, session_hash):
    with sessions.begin() as s:
        conv_ids = select(Conversation.id).where(
            Conversation.id.startswith(prefix, autoescape=True)
        )
        for model in (Event, Job, Preview, Message):
            s.execute(delete(model).where(model.conversation_id.in_(conv_ids)))
        s.execute(delete(Conversation).where(Conversation.id.startswith(prefix, autoescape=True)))
        s.execute(delete(OrderLine).where(OrderLine.order_id.startswith(prefix, autoescape=True)))
        s.execute(delete(Order).where(Order.id.startswith(prefix, autoescape=True)))
        s.execute(delete(Inventory).where(Inventory.variant_id.startswith(prefix, autoescape=True)))
        s.execute(delete(Variant).where(Variant.id.startswith(prefix, autoescape=True)))
        s.execute(delete(Policy).where(Policy.id == prefix))
        s.execute(delete(DemoSession).where(DemoSession.token_hash == session_hash))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database-url", default=TEST_URL)
    parser.add_argument("--samples", type=int, default=30, choices=range(3, 101), metavar="3..100")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    validate_target(args.database_url)
    install_offline_guard()
    sessions = session_factory(Settings(_env_file=None, database_url=args.database_url))
    engine = sessions.kw["bind"]
    prefix = "baseline_a_" + uuid.uuid4().hex[:12]
    token = secrets.token_urlsafe(32)
    session_hash = digest(token)
    report = {
        "stage": "A",
        "schema_version": "backend-baseline-v1",
        "real_model_requests": 0,
        "product_behavior_changed": False,
        "source_commit": subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=PROJECT_ROOT, text=True
        ).strip(),
        "baseline_script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "product_source_sha256": hashlib.sha256(
            "\n".join(
                str(path.relative_to(PROJECT_ROOT))
                + ":"
                + hashlib.sha256(path.read_bytes()).hexdigest()
                for path in sorted((PROJECT_ROOT / "src/aftercare").rglob("*.py"))
            ).encode()
        ).hexdigest(),
        "requirements_sha256": hashlib.sha256(
            (PROJECT_ROOT / "requirements.lock").read_bytes()
        ).hexdigest(),
        "host": {
            "system": platform.system(),
            "release": platform.release(),
            "machine": platform.machine(),
            "python": platform.python_version(),
        },
        "database_target": "127.0.0.1:55434/aftercare_test",
        "measurement_scope": "sequential in-process ASGI requests; not network QPS",
        "sql_count_scope": "SQLAlchemy cursor executions; excludes driver ping/transaction IO",
        "analyze_performed": False,
    }
    recorder = None
    before = None
    try:
        with engine.connect() as gate:
            gate.execute(text("SELECT pg_advisory_lock(27102026, 1)"))
            gate.commit()
            try:
                with sessions() as s:
                    if s.scalar(text("SELECT current_user")) != "aftercare":
                        raise RuntimeError("DEDICATED_ROLE_REQUIRED")
                    if list(s.scalars(text("SELECT version_num FROM alembic_version"))) != [
                        BASELINE_REVISION
                    ]:
                        raise RuntimeError("SCHEMA_REVISION_MISMATCH")
                    if s.get(Customer, "alice") is None:
                        raise RuntimeError("EXISTING_DEMO_SEED_REQUIRED")
                    report["postgresql_version"] = s.scalar(text("SHOW server_version"))
                    report["other_active_connections"] = s.scalar(
                        text(
                            "SELECT count(*) FROM pg_stat_activity "
                            "WHERE datname=current_database() "
                            "AND pid<>pg_backend_pid() AND pid<>:gate AND state<>'idle'"
                        ),
                        {"gate": gate.scalar(text("SELECT pg_backend_pid()"))},
                    )
                    gate.commit()
                    if report["other_active_connections"]:
                        raise RuntimeError("TEST_DATABASE_BUSY")
                before = counts(sessions)
                cases = fixture(sessions, prefix)
                report["fixtures"] = {
                    name: {k: v for k, v in data.items() if k in {"messages", "previews", "events"}}
                    for name, data in cases.items()
                }
                with sessions.begin() as s:
                    s.add(
                        DemoSession(
                            token_hash=session_hash,
                            customer_id="alice",
                            csrf_token=secrets.token_urlsafe(32),
                            expires_at=utc_now() + timedelta(hours=1),
                        )
                    )
                app = create_app(
                    Settings(
                        _env_file=None,
                        app_env="test",
                        database_url=args.database_url,
                        demo_login_enabled=True,
                    )
                )
                app.state.business = BusinessService(sessions)
                recorder = SQLRecorder(engine)
                measurements = []
                with TestClient(app) as client:
                    client.cookies.set("aftercare_session", token)
                    for line_count in (1, 10, 100):
                        measurements.append(
                            measure(
                                client,
                                recorder,
                                f"order-lines-{line_count}",
                                f"/api/orders/{prefix}-order-{line_count}",
                                args.samples,
                            )
                        )
                    for name, data in cases.items():
                        measurements.append(
                            measure(
                                client,
                                recorder,
                                f"snapshot-{name}",
                                f"/api/conversations/{data['conversation']}",
                                args.samples,
                            )
                        )
                    for name in ("previews-0", "events-100", "events-1000"):
                        data = cases[name]
                        measurements.append(
                            measure(
                                client,
                                recorder,
                                f"sse-once-{data['events']}",
                                f"/api/conversations/{data['conversation']}/events?once=true",
                                args.samples,
                            )
                        )
                    small = cases["previews-0"]
                    measurements.append(
                        measure(
                            client,
                            recorder,
                            "sse-replay-after-last",
                            f"/api/conversations/{small['conversation']}/events?once=true"
                            f"&after={small['last_event']}",
                            args.samples,
                        )
                    )
                report["measurements"] = measurements
                report["execution_plans"] = explain_queries(sessions, prefix, cases)
                report["lock_probes"] = lock_probes(
                    args.database_url, cases["previews-0"]["conversation"]
                )
            finally:
                if recorder:
                    recorder.close()
                if before is not None:
                    cleanup(sessions, prefix, session_hash)
                    after = counts(sessions)
                    report["cleanup"] = {
                        "table_counts_before": before,
                        "table_counts_after": after,
                        "match": before == after,
                    }
                    if before != after:
                        raise RuntimeError("BASELINE_CLEANUP_COUNTS_MISMATCH")
                gate.execute(text("SELECT pg_advisory_unlock(27102026, 1)"))
                gate.commit()
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
        print(
            json.dumps(
                {
                    "status": "complete",
                    "output": str(args.output.resolve()),
                    "cases": len(report["measurements"]),
                    "cleanup": report["cleanup"]["match"],
                }
            )
        )
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
