"""D1 pagination supplement; freeze before once, then compare the same live scopes.

No product writes beyond one temporary customer session, no synthetic applications
or audit rows. TestClient ASGI measurements are not TCP throughput or QPS.
"""

import argparse
import json
import os
import re
import secrets
import statistics
import time
from collections import Counter
from datetime import timedelta
from pathlib import Path
from urllib.parse import urlencode

from fastapi.testclient import TestClient
from sqlalchemy import delete, select

from aftercare.api.app import create_app
from aftercare.config import PROJECT_ROOT, Settings
from aftercare.domain.contracts import utc_now
from aftercare.offline import install_offline_guard
from aftercare.repositories import models
from aftercare.services.business import BusinessService, digest
from scripts import backend_d1_benchmark as core

FORMAT = "backend-d1-pages-v1"
BUSINESS_TABLES = (
    models.ShipmentRequest,
    models.WarehouseOutbox,
    models.OutboxAttempt,
    models.ReconciliationTask,
    models.WarehouseInbox,
    models.Inventory,
    models.Reservation,
    models.FulfillmentAudit,
)


def script_sha():
    return core.sha(Path(__file__).read_bytes())


def business_projection(sessions, prefix):
    result = {}
    with sessions() as session:
        for model in BUSINESS_TABLES:
            query = select(model.__table__)
            if model is models.Inventory:
                query = query.where(
                    ~models.Inventory.variant_id.startswith(prefix + "-", autoescape=True)
                )
            rows = [dict(row) for row in session.execute(query).mappings()]
            result[model.__tablename__] = sorted(rows, key=core.canonical)
    return json.loads(core.canonical(result))


def scope_for_before(sessions, pair, before_path):
    raw = json.loads(before_path.read_text())["read_only_business_rows"]
    audits = Counter(row["application_id"] for row in raw["fulfillment_audits"])
    candidates = sorted(
        row["application_id"]
        for row in raw["shipment_requests"]
        if row["status"] == "SUCCEEDED" and audits[row["application_id"]] == 6
    )
    with sessions() as session:
        owned = set(
            session.scalars(
                select(models.Application.id)
                .join(models.OrderLine, models.Application.line_id == models.OrderLine.id)
                .join(models.Order, models.OrderLine.order_id == models.Order.id)
                .where(models.Application.id.in_(candidates), models.Order.customer_id == "alice")
            )
        )
    eligible = [identity for identity in candidates if identity in owned]
    core.require(bool(eligible), "NO_REAL_ALICE_SIX_AUDIT_APPLICATION")
    return {
        "principal": "alice",
        "application_id": eligible[0],
        "audit_count": 6,
        "messages_conversation": pair["cases"]["messages-1000"]["conversation"],
        "message_count": 1000,
        "expected_application_ids": sorted(
            row["application_id"] for row in raw["shipment_requests"]
        ),
        "business_source": str(before_path),
        "business_source_sha256": core.sha(before_path.read_bytes()),
    }


def legacy_workload(scope):
    return [
        {"name": "applications-default", "path": "/api/applications"},
        {"name": "applications-limit2", "path": "/api/applications?limit=2"},
        {
            "name": "fulfillment-events-default",
            "path": f"/api/applications/{scope['application_id']}/events",
        },
    ]


def traversal(client, recorder, spec, expected, index):
    started = time.perf_counter()
    records, samples, cursors = [], [], set()
    after = None
    error = None
    while True:
        query = {"limit": spec["limit"]}
        if after is not None:
            query["after"] = after
        sample, body = core.request_sample(
            client, recorder, spec["path"] + "?" + urlencode(query), len(samples)
        )
        samples.append(sample)
        if (
            not sample["ok"]
            or not isinstance(body, dict)
            or not isinstance(body.get("items"), list)
        ):
            error = "PAGE_REQUEST_FAILED"
            break
        items = body["items"]
        if len(items) > spec["limit"] or "next_cursor" not in body:
            error = "PAGE_ENVELOPE_INVALID"
            break
        records.extend(items)
        if records != expected[: len(records)]:
            error = "PAGE_PROJECTION_CHANGED"
            break
        cursor = body["next_cursor"]
        if cursor is None:
            if records != expected:
                error = "PAGE_TRAVERSAL_INCOMPLETE"
            break
        if (
            not items
            or cursor != items[-1][spec["cursor_field"]]
            or str(cursor) in cursors
            or len(records) >= len(expected)
        ):
            error = "PAGE_CURSOR_INVALID_OR_LOOP"
            break
        cursors.add(str(cursor))
        after = cursor
        if len(samples) > 100:
            error = "PAGE_BUDGET_EXHAUSTED"
            break
    result = {
        "index": index,
        "ok": error is None,
        "error_code": error,
        "duration_ms": (time.perf_counter() - started) * 1000,
        "requests": len(samples),
        "application_sql_count": sum(item["application_sql_count"] for item in samples),
        "response_bytes": sum(item["response_bytes"] for item in samples),
        "item_count": len(records),
        "projection_sha256": core.fingerprint(records),
        "pages": samples,
    }
    return result


def measure_traversal(client, recorder, spec, expected, args):
    warmups = [traversal(client, recorder, spec, expected, index) for index in range(args.warmup)]
    values = [traversal(client, recorder, spec, expected, index) for index in range(args.samples)]
    successful = [item for item in values if item["ok"]]
    result = {
        **spec,
        "kind": "complete_pagination_traversal",
        "samples_requested": args.samples,
        "samples_completed": len(values),
        "samples_failed": len(values) - len(successful),
        "warmup_failed": sum(not item["ok"] for item in warmups),
        "expected_projection_sha256": core.fingerprint(expected),
        "expected_items": len(expected),
        "samples": values,
        "warmup_samples": warmups,
        "passed": all(item["ok"] for item in [*warmups, *values]),
        "summary_includes_successful_samples_only": True,
    }
    if successful:
        durations = [item["duration_ms"] for item in successful]
        result["asgi_latency_ms"] = {
            "p50": round(statistics.median(durations), 3),
            "p95": round(core.baseline.percentile(durations, 0.95), 3),
        }
        for name in ("requests", "application_sql_count", "response_bytes"):
            result[name] = {
                "min": min(item[name] for item in successful),
                "max": max(item[name] for item in successful),
            }
    return result


def run(args, output):
    pair_dir, pair = core.load_pair(args.pair_dir)
    reference = None
    if args.action == "after":
        reference_path = core.runtime_path(args.reference)
        reference = json.loads(reference_path.read_text())
        core.require(reference["status"] == "PASSED", "BEFORE_SUPPLEMENT_FAILED")
        core.require(reference["pages_harness_sha256"] == script_sha(), "PAGES_HARNESS_CHANGED")
        core.require(
            reference["samples_per_case"] == args.samples
            and reference["warmup_per_case"] == args.warmup,
            "PAGES_MEASUREMENT_SETTINGS_CHANGED",
        )
    report = {
        "format": FORMAT,
        "status": "RUNNING",
        "label": args.action,
        "pages_harness_sha256": script_sha(),
        "source": core.source_info(),
        "pair_manifest_sha256": core.sha((pair_dir / "pair.json").read_bytes()),
        "fixture_fingerprint": pair["fixture_fingerprint"],
        "samples_per_case": args.samples,
        "warmup_per_case": args.warmup,
        "concurrency": 1,
        "measurement_scope": "sequential TestClient/in-process ASGI; not TCP/QPS",
        "sql_count_scope": (
            "SQLAlchemy cursor calls including auth; excludes driver ping/transaction IO"
        ),
        "real_model_requests": 0,
        "legacy_measurements": [],
        "page_first_screen": [],
        "page_traversals": [],
        "limits": [
            "1000 synthetic messages; five existing approved applications; six real audit events",
            "message projection equality is not equality to the complete legacy snapshot",
            "full traversal totals include all requests; first-page metrics are separate",
        ],
    }
    token = secrets.token_urlsafe(32)
    token_hash = digest(token)
    recorder = None
    with core.database(args.database_url) as sessions:
        core.require(
            core.fingerprint(core.fixture_projection(sessions, pair["prefix"]))
            == pair["fixture_fingerprint"],
            "FIXTURE_CHANGED_OR_MISSING",
        )
        before_counts = core.baseline.counts(sessions)
        core.require(
            before_counts == pair["table_counts_with_fixture"], "DATABASE_POPULATION_CHANGED"
        )
        original = business_projection(sessions, pair["prefix"])
        report["business_scope_fingerprint"] = core.fingerprint(original)
        report["database_conditions"] = core.database_conditions(sessions)
        if reference is None:
            scope = scope_for_before(sessions, pair, core.runtime_path(args.business_before))
        else:
            scope = reference["scope"]
            core.require(
                reference["pair_manifest_sha256"] == report["pair_manifest_sha256"]
                and reference["business_scope_fingerprint"] == report["business_scope_fingerprint"],
                "PAGES_REFERENCE_SCOPE_CHANGED",
            )
            core.require(
                core.sha(Path(scope["business_source"]).read_bytes())
                == scope["business_source_sha256"],
                "BUSINESS_BEFORE_EVIDENCE_CHANGED",
            )
        report["scope"] = scope
        core.new_json(output / "scope.json", scope)
        (output / "responses").mkdir()
        try:
            with sessions.begin() as session:
                session.add(
                    models.DemoSession(
                        token_hash=token_hash,
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
                    warehouse_callbacks_enabled=False,
                    warehouse_simulation_enabled=False,
                )
            )
            app.state.business = BusinessService(sessions, clock=lambda: core.baseline.FIXED)
            recorder = core.Recorder(sessions.kw["bind"])
            with TestClient(app) as client:
                client.cookies.set("aftercare_session", token)
                bodies = {}
                for spec in legacy_workload(scope):
                    metric, _ = core.measure_case(
                        client, recorder, spec, args.samples, args.warmup, output
                    )
                    report["legacy_measurements"].append(metric)
                    bodies[spec["name"]] = json.loads(
                        (output / "responses" / (spec["name"] + ".json")).read_text()
                    )
                core.require(
                    sorted(
                        item["application_id"] for item in bodies["applications-default"]["items"]
                    )
                    == scope["expected_application_ids"],
                    "REAL_APPLICATION_SCOPE_CHANGED",
                )
                core.require(
                    bodies["applications-default"]["next_cursor"] is None,
                    "APPLICATION_DEFAULT_NOT_COMPLETE_REFERENCE",
                )
                core.require(
                    len(bodies["fulfillment-events-default"]["items"]) == 6,
                    "REAL_AUDIT_SCOPE_CHANGED",
                )
                if reference is None:
                    response = client.get(f"/api/conversations/{scope['messages_conversation']}")
                    core.require(response.status_code == 200, "MESSAGE_REFERENCE_HTTP_FAILED")
                    messages = response.json()["messages"]
                    core.require(len(messages) == 1000, "MESSAGE_REFERENCE_SCALE_CHANGED")
                    projections = {
                        "messages": messages,
                        "applications": bodies["applications-default"]["items"],
                        "fulfillment-events": bodies["fulfillment-events-default"]["items"],
                    }
                    core.new_json(output / "reference-projections.json", projections)
                    report["reference_projections"] = str(output / "reference-projections.json")
                    report["reference_projections_sha256"] = core.sha(
                        (output / "reference-projections.json").read_bytes()
                    )
                    report["unmeasured_reference_snapshot_requests"] = 1
                else:
                    for old, new in zip(
                        reference["legacy_measurements"], report["legacy_measurements"], strict=True
                    ):
                        core.require(
                            old["name"] == new["name"]
                            and old["response_semantic_sha256"] == new["response_semantic_sha256"],
                            "LEGACY_DEFAULT_RESPONSE_CHANGED",
                        )
                    path = Path(reference["reference_projections"])
                    core.require(
                        core.sha(path.read_bytes()) == reference["reference_projections_sha256"],
                        "REFERENCE_PROJECTIONS_CHANGED",
                    )
                    projections = json.loads(path.read_text())
                    specs = [
                        {
                            "name": "messages",
                            "path": f"/api/conversations/{scope['messages_conversation']}/messages",
                            "limit": 50,
                            "cursor_field": "id",
                        },
                        {
                            "name": "applications",
                            "path": "/api/applications",
                            "limit": 2,
                            "cursor_field": "application_id",
                        },
                        {
                            "name": "fulfillment-events",
                            "path": f"/api/applications/{scope['application_id']}/events",
                            "limit": 2,
                            "cursor_field": "to_version",
                        },
                    ]
                    for spec in specs:
                        expected = projections[spec["name"]]
                        first_spec = {
                            "name": spec["name"] + "-first-page",
                            "path": spec["path"] + "?" + urlencode({"limit": spec["limit"]}),
                        }
                        metric, _ = core.measure_case(
                            client, recorder, first_spec, args.samples, args.warmup, output
                        )
                        first = json.loads(
                            (output / "responses" / (first_spec["name"] + ".json")).read_text()
                        )
                        core.require(
                            first["items"] == expected[: spec["limit"]],
                            "FIRST_PAGE_PROJECTION_CHANGED",
                        )
                        report["page_first_screen"].append(metric)
                        report["page_traversals"].append(
                            measure_traversal(client, recorder, spec, expected, args)
                        )
                    report["legacy_default_semantics_equal"] = True
        finally:
            if recorder is not None:
                recorder.close()
            with sessions.begin() as session:
                session.execute(
                    delete(models.DemoSession).where(models.DemoSession.token_hash == token_hash)
                )
            report["cleanup"] = {
                "temporary_session_removed": True,
                "table_counts_unchanged": core.baseline.counts(sessions) == before_counts,
                "business_unchanged": business_projection(sessions, pair["prefix"]) == original,
                "fixture_unchanged": core.fingerprint(
                    core.fixture_projection(sessions, pair["prefix"])
                )
                == pair["fixture_fingerprint"],
            }
            core.require(token not in core.canonical(report), "SECRET_IN_REPORT")
            # Preserve completed sample denominators even if later strict
            # projection/cleanup validation fails and the final status is FAILED.
            core.new_json(output / "partial-result.json", report)
            core.require(all(report["cleanup"].values()), "PAGES_BENCHMARK_MUTATED_DATA")
    core.require(core.source_info() == report["source"], "SOURCE_CHANGED_DURING_MEASUREMENT")
    metrics = [
        *report["legacy_measurements"],
        *report["page_first_screen"],
        *report["page_traversals"],
    ]
    report["status"] = "PASSED" if all(item["passed"] for item in metrics) else "FAILED"
    core.require(token not in core.canonical(report), "SECRET_IN_REPORT")
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("before", "after"))
    parser.add_argument("--pair-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--reference", type=Path)
    parser.add_argument(
        "--business-before",
        type=Path,
        default=PROJECT_ROOT / "runtime/backend-D1/database-before.json",
    )
    parser.add_argument("--database-url", default=core.baseline.TEST_URL)
    parser.add_argument("--samples", type=int, choices=range(3, 101), default=30)
    parser.add_argument("--warmup", type=int, choices=range(1, 11), default=3)
    args = parser.parse_args()
    core.require(
        os.environ.get("ALLOW_REAL_MODEL_CALLS") == "false", "EXPLICIT_OFFLINE_ENVIRONMENT_REQUIRED"
    )
    core.require(
        args.database_url == core.baseline.TEST_URL
        and os.environ.get("DATABASE_URL", core.baseline.TEST_URL) == core.baseline.TEST_URL,
        "EXACT_ISOLATED_TEST_URL_REQUIRED",
    )
    core.baseline.validate_target(args.database_url)
    core.require(args.action != "after" or args.reference is not None, "BEFORE_REFERENCE_REQUIRED")
    os.umask(0o077)
    install_offline_guard()
    output = core.new_directory(args.output_dir)
    try:
        result = run(args, output)
    except Exception as error:
        value = str(error) if isinstance(error, RuntimeError) else type(error).__name__
        result = {
            "format": FORMAT,
            "status": "FAILED",
            "label": args.action,
            "error_code": value if re.fullmatch(r"[A-Z_]+", value) else type(error).__name__,
        }
    core.new_json(output / "result.json", result)
    print(
        json.dumps({"status": result["status"], "result": str(output / "result.json")}), flush=True
    )
    return 0 if result["status"] == "PASSED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
