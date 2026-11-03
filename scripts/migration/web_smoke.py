"""Offline HTTP server + actual CLI worker check on the new migration DB only."""

import json
import os
import subprocess
import sys
import time
from pathlib import Path

import httpx
from sqlalchemy.engine import make_url


def main():
    url = make_url(os.environ["DATABASE_URL"])
    if (url.host, url.port, url.database) != ("127.0.0.1", 55434, "aftercare_demo"):
        raise ValueError("MIGRATION_SMOKE_REQUIRES_NEW_55434_DEMO_DATABASE")
    if os.environ.get("ALLOW_REAL_MODEL_CALLS") != "false":
        raise ValueError("MIGRATION_SMOKE_MUST_BE_OFFLINE")
    output = Path("runtime/langchain-migration")
    output.mkdir(parents=True, exist_ok=True)
    subprocess.run([sys.executable, "-m", "aftercare", "db", "migrate"], check=True)
    subprocess.run([sys.executable, "-m", "aftercare", "demo", "seed"], check=True)
    with (output / "http-server.log").open("w") as log:
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "aftercare",
                "serve",
                "--offline",
                "--demo-login",
                "--port",
                "8131",
            ],
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        try:
            with httpx.Client(base_url="http://127.0.0.1:8131", timeout=10, trust_env=False) as c:
                for _ in range(100):
                    if process.poll() is not None:
                        raise RuntimeError("MIGRATION_HTTP_SERVER_EXITED")
                    try:
                        if c.get("/health/ready").status_code == 200:
                            break
                    except httpx.ConnectError:
                        pass
                    time.sleep(0.1)
                else:
                    raise RuntimeError("MIGRATION_HTTP_SERVER_NOT_READY")
                assert c.get("/").status_code == 200
                me = c.post("/api/demo-sessions", json={"customer_id": "alice"}).json()
                c.headers["X-CSRF-Token"] = me["csrf_token"]
                conv = c.post("/api/conversations").json()["id"]
                response = c.post(
                    f"/api/conversations/{conv}/messages",
                    json={"text": "取消订单 S1001", "client_message_id": "migration-smoke"},
                )
                assert response.status_code == 202, response.text

                def worker():
                    subprocess.run(
                        [sys.executable, "-m", "aftercare", "worker", "--once"],
                        check=True,
                        timeout=60,
                    )

                worker()
                first = c.get(f"/api/conversations/{conv}").json()
                pending = next(p for p in first["previews"] if p["status"] == "PENDING")
                assert first["task_state"] == "WAITING_CONFIRMATION"
                assert first["operations"] == []
                endpoint = f"/api/previews/{pending['id']}/confirm"
                assert c.post(endpoint, json={"decision": "accept"}).status_code == 202
                assert c.post(endpoint, json={"decision": "accept"}).status_code == 202
                worker()
                final = c.get(f"/api/conversations/{conv}").json()
                assert final["task_state"] == "COMPLETED"
                assert len(final["operations"]) == 1
                notice = final["messages"][-1]["text"]
                assert "不代表退款到账" in notice
                events = c.get(f"/api/conversations/{conv}/events?once=true").text
                ids = [int(s[4:]) for s in events.splitlines() if s.startswith("id: ")]
                assert ids and ids == sorted(set(ids))
                replay = c.get(
                    f"/api/conversations/{conv}/events?once=true",
                    headers={"Last-Event-ID": str(ids[-1])},
                ).text
                assert "data:" not in replay
                result = {
                    "http_server": "127.0.0.1:8131",
                    "database": "127.0.0.1:55434/aftercare_demo",
                    "worker": "actual CLI + supervised child",
                    "task_state": final["task_state"],
                    "operation_count": len(final["operations"]),
                    "duplicate_confirmation": "same operation",
                    "sse_replay": "passed",
                    "notice": notice,
                    "real_model_requests": 0,
                }
                (output / "http-smoke.json").write_text(
                    json.dumps(result, ensure_ascii=False, indent=2)
                )
                print(json.dumps(result, ensure_ascii=False))
        finally:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


if __name__ == "__main__":
    main()
