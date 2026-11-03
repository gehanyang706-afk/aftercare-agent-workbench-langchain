from datetime import timedelta

from fastapi.testclient import TestClient
from sqlalchemy import func, select

from aftercare.agent.runtime import setup_checkpoints
from aftercare.api.app import create_app
from aftercare.config import Settings
from aftercare.repositories.models import Application, Event, Job, ModelCall
from aftercare.workers.worker import run_once


def web(service, dev=False):
    settings = Settings(
        _env_file=None,
        app_env="test",
        demo_login_enabled=True,
        dev_faults=dev,
        database_url=service.sessions.kw["bind"].url.render_as_string(hide_password=False),
    )
    setup_checkpoints(settings)
    app = create_app(settings)
    app.state.business = service
    client = TestClient(app)
    me = client.post("/api/demo-sessions", json={"customer_id": "alice"}).json()
    client.headers["X-CSRF-Token"] = me["csrf_token"]
    return client, settings


def submit(client, conv, text="S1002 中 1 件换大一码", mid="first"):
    response = client.post(
        f"/api/conversations/{conv}/messages", json={"text": text, "client_message_id": mid}
    )
    assert response.status_code == 202, response.text
    return response.json()


def test_web_real_worker_confirm_receipt_and_sse_replay(service):
    client, settings = web(service)
    assert client.get("/").status_code == 200
    assert client.get("/static/app.js").status_code == 200
    conv = client.post("/api/conversations").json()["id"]
    one = submit(client, conv)
    assert submit(client, conv)["job_id"] == one["job_id"]
    assert run_once(settings)
    snap = client.get(f"/api/conversations/{conv}").json()
    assert snap["status"] == "WAITING_CONFIRMATION", snap
    pid = snap["previews"][0]["id"]
    assert (
        client.post(f"/api/previews/{pid}/confirm", json={"decision": "accept"}).status_code == 202
    )
    assert (
        client.post(f"/api/previews/{pid}/confirm", json={"decision": "accept"}).status_code == 202
    )
    assert run_once(settings)
    snap = client.get(f"/api/conversations/{conv}").json()
    assert len(snap["operations"]) == 1 and snap["operations"][0]["result"] == "EXCHANGE_REQUESTED"
    stream = client.get(f"/api/conversations/{conv}/events?once=true").text
    ids = [int(line[4:]) for line in stream.splitlines() if line.startswith("id: ")]
    assert ids == sorted(set(ids))
    replay = client.get(
        f"/api/conversations/{conv}/events?once=true", headers={"Last-Event-ID": str(ids[-1])}
    )
    assert "data:" not in replay.text
    with service.sessions() as s:
        assert s.scalar(select(func.count()).select_from(Application)) == 1
        assert s.scalar(select(func.count()).select_from(Job)) == 2


def test_revise_refresh_and_cross_principal_csrf(service):
    client, settings = web(service)
    conv = client.post("/api/conversations").json()["id"]
    submit(client, conv)
    run_once(settings)
    pid = client.get(f"/api/conversations/{conv}").json()["previews"][0]["id"]
    submit(client, conv, "改为退货", "second")
    run_once(settings)
    assert (
        client.post(f"/api/previews/{pid}/confirm", json={"decision": "accept"}).status_code == 409
    )
    result = client.post("/api/demo-sessions", json={"customer_id": "bob"}).json()
    client.headers["X-CSRF-Token"] = result["csrf_token"]
    assert client.get(f"/api/conversations/{conv}").status_code == 404
    assert client.post("/api/conversations", headers={"X-CSRF-Token": ""}).status_code == 403
    assert (
        client.post("/api/conversations", headers={"Origin": "https://evil.example"}).status_code
        == 403
    )


def test_lost_response_injection_and_production_fault_route(service):
    client, settings = web(service, dev=True)
    conv = client.post("/api/conversations").json()["id"]
    submit(client, conv)
    run_once(settings)
    assert (
        client.post(
            "/api/dev/faults", json={"conversation_id": conv, "fault": "lost_response"}
        ).status_code
        == 200
    )
    pid = client.get(f"/api/conversations/{conv}").json()["previews"][0]["id"]
    client.post(f"/api/previews/{pid}/confirm", json={"decision": "accept"})
    run_once(settings)
    snap = client.get(f"/api/conversations/{conv}").json()
    assert len(snap["operations"]) == 1
    assert "原回执" in snap["messages"][-1]["text"]
    with service.sessions() as s:
        names = [
            e.payload.get("tool")
            for e in s.scalars(select(Event).where(Event.event_type == "tool_result"))
        ]
        assert "get_operation" in names
    settings.dev_faults = False
    assert (
        client.post("/api/dev/faults", json={"conversation_id": conv, "fault": "slow"}).status_code
        == 404
    )


def test_expired_lease_is_reclaimed_and_blocking_unknown_usage_cleanup(service):
    client, settings = web(service, dev=True)
    conv = client.post("/api/conversations").json()["id"]
    client.post("/api/dev/faults", json={"conversation_id": conv, "fault": "slow"})
    job_id = submit(client, conv)["job_id"]
    from aftercare.domain.contracts import utc_now

    with service.sessions.begin() as s:
        job = s.get(Job, job_id)
        job.status = "RUNNING"
        job.lease_owner = "dead-worker"
        job.lease_expires_at = utc_now() - timedelta(seconds=10)
    assert run_once(settings, seconds=2)
    with service.sessions() as s:
        job = s.get(Job, job_id)
        assert job.status == "FAILED" and job.attempt == 1
        calls = list(s.scalars(select(ModelCall)))
        assert calls and all(c.status == "USAGE_UNKNOWN" for c in calls)


def test_cancel_running_blocking_job_cleans_process_and_preserves_unknown_usage(service):
    import time
    from concurrent.futures import ThreadPoolExecutor

    client, settings = web(service, dev=True)
    conv = client.post("/api/conversations").json()["id"]
    client.post("/api/dev/faults", json={"conversation_id": conv, "fault": "slow"})
    job_id = submit(client, conv)["job_id"]
    with ThreadPoolExecutor(max_workers=1) as pool:
        running = pool.submit(run_once, settings, 30)
        deadline = time.monotonic() + 8
        while time.monotonic() < deadline:
            with service.sessions() as session:
                if session.scalar(select(ModelCall.id).where(ModelCall.run_id == job_id)):
                    break
            time.sleep(0.05)
        else:
            raise AssertionError("blocking reservation never created")
        start = time.monotonic()
        assert client.post(f"/api/jobs/{job_id}/cancel").status_code == 200
        assert running.result(timeout=5)
        assert time.monotonic() - start < 5
    with service.sessions() as session:
        assert session.get(Job, job_id).status == "CANCELLED"
        assert (
            session.scalar(select(ModelCall).where(ModelCall.run_id == job_id)).status
            == "USAGE_UNKNOWN"
        )
