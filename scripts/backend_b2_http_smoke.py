"""Bounded real localhost HTTP + CLI Worker check; isolated migration test DB only."""

import argparse
import json
import os
import secrets
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx

from aftercare.config import PROJECT_ROOT, Settings
from aftercare.domain.contracts import ActionInput
from aftercare.offline import install_offline_guard
from aftercare.repositories.database import session_factory
from aftercare.services.business import BusinessService
from aftercare.services.demo import seed
from aftercare.services.staff_auth import StaffAuthService
from scripts.backend_baseline import TEST_URL, validate_target


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8132, choices=range(8132, 8140))
    parser.add_argument("--hold-for-browser", type=int, default=0, choices=range(0, 601))
    parser.add_argument("--output-dir", type=Path, default=PROJECT_ROOT / "runtime/backend-B2")
    args = parser.parse_args()
    raw_url = os.environ.get("DATABASE_URL", "")
    validate_target(raw_url)
    if raw_url != TEST_URL or os.environ.get("ALLOW_REAL_MODEL_CALLS") != "false":
        raise RuntimeError("EXPLICIT_ISOLATED_OFFLINE_ENVIRONMENT_REQUIRED")
    output = args.output_dir.resolve()
    if not output.is_relative_to((PROJECT_ROOT / "runtime").resolve()):
        raise RuntimeError("OUTPUT_MUST_BE_PROJECT_RUNTIME")
    if any((output / name).exists() for name in ("http-first.json", "http-server.log")):
        raise RuntimeError("FIRST_EVIDENCE_EXISTS_USE_A_NEW_OUTPUT_DIR")
    install_offline_guard()
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", args.port))
    output.mkdir(parents=True, exist_ok=True)
    base = f"http://127.0.0.1:{args.port}"
    env = {**os.environ, "PYTHONPATH": "src:.", "ALLOW_REAL_MODEL_CALLS": "false"}
    settings = Settings(_env_file=None, database_url=raw_url)
    sessions = session_factory(settings)
    server = None
    accounts = []
    prefix = "qa-" + secrets.token_hex(4)
    credentials = {
        role: {"username": f"{prefix}-{role}", "password": secrets.token_urlsafe(20)}
        for role in ("support", "warehouse")
    }
    report = {
        "base_url": base,
        "database": "127.0.0.1:55434/aftercare_test",
        "transport": "actual localhost TCP HTTP",
        "real_model_requests": 0,
    }
    marker = output / "finish-browser.flag"
    if marker.exists():
        marker.unlink()
    try:
        seed(sessions)
        for role, value in credentials.items():
            created = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "aftercare",
                    "staff",
                    "create",
                    "--username",
                    value["username"],
                    "--role",
                    role,
                    "--password-stdin",
                ],
                input=value["password"] + "\n",
                capture_output=True,
                text=True,
                timeout=15,
                cwd=PROJECT_ROOT,
                env=env,
            )
            if created.returncode:
                raise RuntimeError("CLI_STAFF_PROVISION_FAILED")
            accounts.append(json.loads(created.stdout)["id"])
        with (output / "http-server.log").open("w") as log:
            server = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "aftercare",
                    "serve",
                    "--offline",
                    "--demo-login",
                    "--simulate-warehouse",
                    "--port",
                    str(args.port),
                ],
                cwd=PROJECT_ROOT,
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
            )
            with httpx.Client(base_url=base, trust_env=False, timeout=10) as customer:
                deadline = time.monotonic() + 30
                while True:
                    if server.poll() is not None:
                        raise RuntimeError("SMOKE_SERVER_EXITED")
                    try:
                        if customer.get("/health/ready").status_code == 200:
                            break
                    except httpx.TransportError:
                        pass
                    if time.monotonic() > deadline:
                        raise RuntimeError("SMOKE_SERVER_NOT_READY")
                    time.sleep(0.2)
                assert customer.get("/fulfillment").status_code == 200
                signed = customer.post("/api/demo-sessions", json={"customer_id": "alice"})
                signed.raise_for_status()
                customer.headers["X-CSRF-Token"] = signed.json()["csrf_token"]
                conv = customer.post("/api/conversations").json()["id"]
                customer.post(
                    f"/api/conversations/{conv}/messages",
                    json={
                        "text": "S1002 中 1 件换大一码",
                        "client_message_id": prefix,
                    },
                ).raise_for_status()

                def worker():
                    result = subprocess.run(
                        [
                            sys.executable,
                            "-m",
                            "aftercare",
                            "worker",
                            "--provider",
                            "scripted",
                            "--once",
                        ],
                        cwd=PROJECT_ROOT,
                        env=env,
                        capture_output=True,
                        text=True,
                        timeout=40,
                    )
                    if result.returncode:
                        raise RuntimeError("SMOKE_CLI_WORKER_FAILED")

                worker()
                snapshot = customer.get(f"/api/conversations/{conv}").json()
                pending = [p for p in snapshot["previews"] if p["status"] == "PENDING"]
                assert len(pending) == 1
                customer.post(
                    f"/api/previews/{pending[0]['id']}/confirm", json={"decision": "accept"}
                ).raise_for_status()
                worker()
                snapshot = customer.get(f"/api/conversations/{conv}").json()
                original = snapshot["operations"][0]
                appid = original["application_id"]
                report.update(application_id=appid, original_receipt=original, cli_worker_runs=2)
                with (
                    httpx.Client(base_url=base, trust_env=False, timeout=10) as support,
                    httpx.Client(base_url=base, trust_env=False, timeout=10) as warehouse,
                ):
                    for client, role in ((support, "support"), (warehouse, "warehouse")):
                        signed = client.post("/api/staff/sessions", json=credentials[role])
                        signed.raise_for_status()
                        client.headers["X-CSRF-Token"] = signed.json()["csrf_token"]
                    flow = [
                        (support, "review", {"decision": "approve"}),
                        (
                            customer,
                            "return-shipment",
                            {"carrier": "本地模拟", "tracking_number": prefix},
                        ),
                        (warehouse, "receive", {"note": "已核对退回商品"}),
                        (warehouse, "replacement-shipments", {}),
                        (warehouse, "simulate-shipment", {}),
                        (customer, "complete", {}),
                    ]
                    version, records = 1, []
                    for client, path, data in flow:
                        payload = {"expected_version": version, **data}
                        headers = {"Idempotency-Key": f"{prefix}:{path}"}
                        response = client.post(
                            f"/api/applications/{appid}/{path}", json=payload, headers=headers
                        )
                        response.raise_for_status()
                        receipt = response.json()
                        repeated = client.post(
                            f"/api/applications/{appid}/{path}", json=payload, headers=headers
                        )
                        assert repeated.json() == receipt
                        version = receipt["version"]
                        records.append(receipt)
                    final = customer.get(f"/api/applications/{appid}").json()
                    assert final["status"] == "COMPLETED" and final["version"] == 7
                    assert final["receipt"] == original and len(final["history"]) == 7
                    report.update(commands=records, final=final, repeated_commands=6)
                # Leave a second, independently approved request for optional browser QA.
                business = BusinessService(sessions)
                second_conv = business.create_conversation("bob")
                preview = business.preview(
                    "bob",
                    second_conv,
                    ActionInput(
                        action="exchange",
                        order_id="S2001",
                        line_id="S2001-1",
                        target_variant="tee-black-M",
                        quantity=1,
                    ),
                )
                business.confirm("bob", preview["id"], True)
                browser_app = business.commit("bob", preview["id"])["application_id"]
                access = output / "local-access.json"
                access.write_text(
                    json.dumps(
                        {
                            "base_url": base,
                            "staff": credentials,
                            "customer": "bob",
                            "application_id": browser_app,
                        },
                        ensure_ascii=False,
                        indent=2,
                    )
                    + "\n"
                )
                access.chmod(0o600)
                report["browser_application_id"] = browser_app
                (output / "http-first.json").write_text(
                    json.dumps(report, ensure_ascii=False, indent=2) + "\n"
                )
                print(
                    json.dumps(
                        {
                            "status": "HTTP_COMPLETE",
                            "commands": len(records),
                            "duplicate_replays": 6,
                            "application_id": appid,
                            "browser_ready": bool(args.hold_for_browser),
                        },
                        ensure_ascii=False,
                    ),
                    flush=True,
                )
                deadline = time.monotonic() + args.hold_for_browser
                while time.monotonic() < deadline and not marker.exists():
                    time.sleep(0.25)
    finally:
        if server is not None:
            server.terminate()
            try:
                server.wait(timeout=10)
            except subprocess.TimeoutExpired:
                server.kill()
                server.wait(timeout=5)
        auth = StaffAuthService(sessions)
        for staff_id in accounts:
            auth.update_account(staff_id, active=False)
        sessions.kw["bind"].dispose()


if __name__ == "__main__":
    main()
