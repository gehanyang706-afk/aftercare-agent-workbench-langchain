"""Capture real paginated read SQL and PostgreSQL plans on the retained D1 fixture."""

import argparse
import json
import os
import re
import secrets
from datetime import timedelta
from pathlib import Path
from urllib.parse import urlencode

from fastapi.testclient import TestClient
from sqlalchemy import delete, text

from aftercare.api.app import create_app
from aftercare.config import Settings
from aftercare.domain.contracts import utc_now
from aftercare.offline import install_offline_guard
from aftercare.repositories.models import DemoSession
from aftercare.services.business import BusinessService, digest
from scripts import backend_d1_benchmark as core


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pair-dir", type=Path, required=True)
    parser.add_argument("--reference", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    core.require(os.environ.get("ALLOW_REAL_MODEL_CALLS") == "false", "OFFLINE_REQUIRED")
    install_offline_guard()
    output = core.new_directory(args.output_dir)
    _, pair = core.load_pair(args.pair_dir)
    reference = json.loads(core.runtime_path(args.reference).read_text())
    core.require(reference["status"] == "PASSED", "REFERENCE_FAILED")
    scope = reference["scope"]
    projection = json.loads(Path(reference["reference_projections"]).read_text())
    messages = projection["messages"]
    application_id = scope["application_id"]
    message_path = f"/api/conversations/{scope['messages_conversation']}/messages"
    paths = {
        "messages-first-50-of-1000": message_path + "?limit=50",
        "messages-after-950-of-1000": message_path
        + "?"
        + urlencode({"limit": 50, "after": messages[949]["id"]}),
        "application-first-2-of-5": "/api/applications?limit=2",
        "application-after-first-2": "/api/applications?"
        + urlencode({"limit": 2, "after": projection["applications"][1]["application_id"]}),
        "fulfillment-events-default-6": f"/api/applications/{application_id}/events",
        "fulfillment-events-first-2": f"/api/applications/{application_id}/events?limit=2",
        "fulfillment-events-after-4": f"/api/applications/{application_id}/events?limit=2&after=4",
    }
    token = secrets.token_urlsafe(32)
    token_hash = digest(token)
    report = {"status": "RUNNING", "source": core.source_info(), "plans": [], "paths": paths}
    with core.database(core.baseline.TEST_URL) as sessions:
        core.require(
            core.fingerprint(core.fixture_projection(sessions, pair["prefix"]))
            == pair["fixture_fingerprint"],
            "FIXTURE_CHANGED",
        )
        with sessions() as session:
            report["indexes"] = [
                dict(row)
                for row in session.execute(
                    text(
                        "SELECT tablename,indexname,indexdef FROM pg_indexes "
                        "WHERE schemaname='public' ORDER BY tablename,indexname"
                    )
                ).mappings()
            ]
        recorder = core.Recorder(sessions.kw["bind"])
        try:
            with sessions.begin() as session:
                session.add(
                    DemoSession(
                        token_hash=token_hash,
                        customer_id="alice",
                        csrf_token=secrets.token_urlsafe(32),
                        expires_at=utc_now() + timedelta(hours=1),
                    )
                )
            settings = Settings(
                _env_file=None,
                app_env="test",
                database_url=core.baseline.TEST_URL,
                demo_login_enabled=True,
                warehouse_delivery_enabled=False,
                warehouse_callbacks_enabled=False,
                warehouse_simulation_enabled=False,
            )
            app = create_app(settings)
            app.state.business = BusinessService(sessions, clock=lambda: core.baseline.FIXED)
            examples = {}
            with TestClient(app) as client:
                client.cookies.set("aftercare_session", token)
                for name, path in paths.items():
                    value, _ = core.request_sample(client, recorder, path, 0)
                    core.require(value["ok"], "PLAN_PROBE_HTTP_FAILED")
                    examples[name] = list(recorder.executions)
            recorder.close()
            recorder = None
            with sessions.kw["bind"].connect() as connection:
                connection.execute(text("SET TRANSACTION READ ONLY"))
                for name, statements in examples.items():
                    for sql, parameters in statements:
                        normalized = " ".join(sql.split())
                        if (
                            not normalized.upper().startswith("SELECT ")
                            or "FOR UPDATE" in normalized.upper()
                            or not re.search(
                                r"\b(?:messages|aftercare_requests|fulfillment_audits)\b",
                                normalized,
                            )
                        ):
                            continue
                        plan = connection.exec_driver_sql(
                            "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + sql, parameters
                        ).scalar()
                        report["plans"].append(
                            {"case": name, "statement": normalized, "plan": plan}
                        )
            report["status"] = "PASSED"
        finally:
            if recorder is not None:
                recorder.close()
            with sessions.begin() as session:
                session.execute(delete(DemoSession).where(DemoSession.token_hash == token_hash))
    core.require(token not in core.canonical(report), "SECRET_IN_REPORT")
    core.new_json(output / "result.json", report)
    print(json.dumps({"status": report["status"], "plan_count": len(report["plans"])}))


if __name__ == "__main__":
    main()
