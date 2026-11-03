"""External MVCC reads preserve legacy facts without weakening internal execution fences."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from datetime import timedelta
from threading import Event, Lock

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import event, func, select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.orm import sessionmaker

from aftercare.api.app import create_app
from aftercare.config import Settings
from aftercare.domain.contracts import ActionInput, BusinessError
from aftercare.repositories.models import (
    Conversation,
    Job,
    Operation,
    Preview,
)
from aftercare.services.business import BusinessService, columns
from aftercare.services.read_snapshot import ReadSnapshotService
from aftercare.workers.queue import enqueue

pytestmark = pytest.mark.integration


@pytest.fixture
def client(service):
    settings = Settings(
        _env_file=None,
        app_env="test",
        demo_login_enabled=True,
        database_url=service.sessions.kw["bind"].url.render_as_string(hide_password=False),
    )
    app = create_app(settings)
    app.state.business = service
    with TestClient(app) as browser:
        result = browser.post("/api/demo-sessions", json={"customer_id": "alice"})
        assert result.status_code == 200
        browser.headers["X-CSRF-Token"] = result.json()["csrf_token"]
        yield browser


def prepare(service, *, approved=False):
    conversation = service.create_conversation("alice")
    preview = service.preview(
        "alice",
        conversation,
        ActionInput(
            action="exchange",
            order_id="S1002",
            line_id="S1002-1",
            quantity=1,
            target_variant="tee-white-L",
        ),
    )
    if approved:
        service.confirm("alice", preview["id"], True)
    return conversation, preview["id"]


def legacy_complete(service, conversation):
    """Frozen old API behavior; no new reader used as its own expected result."""
    result = service.snapshot("alice", conversation)
    with service.sessions() as session:
        result["jobs"] = [
            columns(row)
            for row in session.scalars(select(Job).where(Job.conversation_id == conversation))
        ]
        result["operations"] = [
            row.receipt
            for row in session.scalars(
                select(Operation)
                .join(Preview, Operation.preview_id == Preview.id)
                .where(Preview.conversation_id == conversation)
            )
        ]
    return result


def runtime(service, conversation, task_state, *, revision=0, graph_state="END"):
    with service.sessions.begin() as session:
        service.event(
            session,
            conversation,
            "runtime_state",
            {"revision": revision, "task_state": task_state, "graph_state": graph_state},
        )


@pytest.mark.parametrize(
    "scenario,expected_state",
    [
        ("idle", "IDLE"),
        ("pending", "WAITING_CONFIRMATION"),
        ("confirmed", "WAITING_SUBMISSION"),
        ("rejected", "WAITING_USER"),
        ("runtime-failed", "FAILED"),
        ("runtime-unknown", "EXECUTION_UNKNOWN"),
        ("runtime-rejected", "REJECTED"),
        ("runtime-complete-without-receipt", "FAILED"),
        ("runtime-other-revision", "IDLE"),
        ("receipt-without-notice", "SUBMITTED"),
        ("receipt-graph-end-without-notice", "SUBMITTED"),
        ("receipt-final-notice", "COMPLETED"),
        ("receipt-wrong-notice", "SUBMITTED"),
        ("receipt-still-unknown", "EXECUTION_UNKNOWN"),
        ("old-receipt-new-complete-without-receipt", "FAILED"),
    ],
)
def test_external_snapshot_matches_legacy_state_and_all_fields(
    client, service, scenario, expected_state
):
    preview_id, receipt = None, None
    if scenario in {"pending", "confirmed", "rejected"} or scenario.startswith("receipt-"):
        conversation, preview_id = prepare(service, approved=scenario != "pending")
    elif scenario == "old-receipt-new-complete-without-receipt":
        conversation, preview_id = prepare(service, approved=True)
    else:
        conversation = service.create_conversation("alice")
    if scenario == "rejected":
        service.confirm("alice", preview_id, False)
    if scenario.startswith("receipt-") or scenario.startswith("old-receipt-"):
        receipt = service.commit("alice", preview_id)
    if scenario == "runtime-failed":
        runtime(service, conversation, "FAILED")
    elif scenario in {"runtime-unknown", "receipt-still-unknown"}:
        runtime(service, conversation, "EXECUTION_UNKNOWN")
    elif scenario == "runtime-rejected":
        runtime(service, conversation, "REJECTED")
    elif scenario == "runtime-complete-without-receipt":
        runtime(service, conversation, "COMPLETED")
    elif scenario == "receipt-graph-end-without-notice":
        runtime(service, conversation, "COMPLETED")
    elif scenario == "runtime-other-revision":
        runtime(service, conversation, "FAILED", revision=42)
    elif scenario in {"receipt-final-notice", "receipt-wrong-notice"}:
        with service.sessions.begin() as session:
            service.event(
                session,
                conversation,
                "user_notice",
                {
                    "stage": "final",
                    "operation_id": receipt["operation_id"]
                    if scenario == "receipt-final-notice"
                    else "unrelated-operation",
                },
            )
    elif scenario == "old-receipt-new-complete-without-receipt":
        service.append_message("alice", conversation, "新的诉求", "next-intent")
        runtime(service, conversation, "COMPLETED", revision=1)

    expected = legacy_complete(service, conversation)
    response = client.get(f"/api/conversations/{conversation}")
    assert response.status_code == 200, response.text
    assert response.json() == expected
    assert expected["task_state"] == expected_state
    assert ReadSnapshotService(service).get("alice", conversation) == expected
    if receipt is not None:
        assert expected["operations"] == [receipt]
        assert service.get_operation("alice", preview_id=preview_id) == receipt


def test_snapshot_preserves_every_job_field_and_historical_jobs(client, service):
    conversation = service.create_conversation("alice")
    with service.sessions.begin() as session:
        for number, status in enumerate(("PENDING", "RUNNING", "FAILED", "COMPLETED", "CANCELLED")):
            session.add(
                Job(
                    id=f"snapshot-job-{number}",
                    conversation_id=conversation,
                    dedup_key=f"snapshot-dedup-{number}",
                    kind="message",
                    revision=number,
                    status=status,
                    lease_owner="original-owner" if status == "RUNNING" else None,
                    lease_expires_at=service.clock() + timedelta(seconds=30)
                    if status == "RUNNING"
                    else None,
                    attempt=number,
                    cancel_requested=status == "CANCELLED",
                    result={"原字段": [False, 0, "中文"]},
                    created_at=service.clock(),
                )
            )
    expected = legacy_complete(service, conversation)
    assert len(expected["jobs"]) == 5
    assert client.get(f"/api/conversations/{conversation}").json() == expected


@pytest.mark.parametrize("principal,missing", [("bob", False), ("alice", True)])
def test_read_authorizes_before_loading_associations(service, principal, missing):
    conversation, _ = prepare(service)
    calls = []

    def before(conn, cursor, statement, parameters, context, executemany):
        calls.append(" ".join(statement.lower().split()))

    engine = service.sessions.kw["bind"]
    event.listen(engine, "before_cursor_execute", before)
    try:
        with pytest.raises(BusinessError) as error:
            ReadSnapshotService(service).get(principal, "missing" if missing else conversation)
    finally:
        event.remove(engine, "before_cursor_execute", before)
    assert error.value.code == "NOT_FOUND"
    assert len(calls) == 2
    assert calls[0].startswith("set transaction") and "from conversations " in calls[1]


def test_snapshot_http_keeps_session_and_owner_boundaries(client, service):
    conversation, _ = prepare(service)
    client.cookies.clear()
    assert client.get(f"/api/conversations/{conversation}").status_code == 401
    result = client.post("/api/demo-sessions", json={"customer_id": "bob"})
    client.headers["X-CSRF-Token"] = result.json()["csrf_token"]
    response = client.get(
        f"/api/conversations/{conversation}",
        headers={"X-Customer-ID": "alice"},
        params={"customer_id": "alice"},
    )
    assert response.status_code == 404
    assert client.get("/api/conversations/missing").status_code == 404


def test_internal_guard_still_fences_execution_but_browser_read_does_not_call_it(
    client, service, monkeypatch
):
    conversation, _ = prepare(service)
    expected = legacy_complete(service, conversation)
    guards = []

    def guard(session, conv):
        guards.append(conv.id)
        raise BusinessError("WORKER_LEASE_LOST")

    service.run_guard = guard
    assert ReadSnapshotService(service).get("alice", conversation) == expected
    assert client.get(f"/api/conversations/{conversation}").json() == expected
    assert guards == []
    with pytest.raises(BusinessError) as error:
        service.snapshot("alice", conversation)
    assert error.value.code == "WORKER_LEASE_LOST" and guards == [conversation]
    monkeypatch.setattr(service, "_conversation", lambda *args: pytest.fail("LOCKED_ENTRY_USED"))
    assert ReadSnapshotService(service).get("alice", conversation) == expected


def test_read_uses_one_readonly_repeatable_read_transaction_and_no_row_lock(service):
    conversation, _ = prepare(service)
    calls, connections = [], set()

    def before(conn, cursor, statement, parameters, context, executemany):
        calls.append(" ".join(statement.lower().split()))
        connections.add(id(conn))

    engine = service.sessions.kw["bind"]
    event.listen(engine, "before_cursor_execute", before)
    try:
        ReadSnapshotService(service).get("alice", conversation)
    finally:
        event.remove(engine, "before_cursor_execute", before)
    assert calls[0] == "set transaction isolation level repeatable read, read only"
    assert len(connections) == 1
    assert all("for update" not in sql and "for share" not in sql for sql in calls)
    assert all(sql.startswith(("select ", "set transaction ")) for sql in calls)
    assert any("from jobs " in sql for sql in calls)
    assert any("from operations join action_previews" in sql for sql in calls)


def test_readonly_transaction_rejects_accidental_write_in_projection(service, monkeypatch):
    conversation, _ = prepare(service)

    def wrong_projection(session, conv):
        session.execute(
            text("UPDATE conversations SET revision=revision+1 WHERE id=:id"),
            {"id": conv.id},
        )
        return []

    monkeypatch.setattr(service, "_preview_views", wrong_projection)
    with pytest.raises(DBAPIError) as error:
        ReadSnapshotService(service).get("alice", conversation)
    assert getattr(error.value.orig, "sqlstate", None) == "25006"
    with service.sessions() as session:
        assert session.get(Conversation, conversation).revision == 0


@contextmanager
def paused_read_after_conversation(service):
    entered, release = Event(), Event()
    once, mutex = [False], Lock()

    def after(conn, cursor, statement, parameters, context, executemany):
        sql = " ".join(statement.lower().split())
        if not sql.startswith("select ") or "from conversations " not in sql or "for update" in sql:
            return
        with mutex:
            if once[0]:
                return
            once[0] = True
        entered.set()
        assert release.wait(timeout=8), "external reader pause was not released"

    engine = service.sessions.kw["bind"]
    event.listen(engine, "after_cursor_execute", after)
    try:
        yield entered, release
    finally:
        release.set()
        event.remove(engine, "after_cursor_execute", after)


@pytest.mark.parametrize("writer_kind", ["message-and-job", "commit-and-receipt"])
def test_concurrent_write_commits_without_blocking_and_read_has_one_mvcc_version(
    client, service, writer_kind
):
    conversation, preview_id = prepare(service, approved=writer_kind == "commit-and-receipt")
    expected = legacy_complete(service, conversation)
    reader = ReadSnapshotService(service)
    with paused_read_after_conversation(service) as (entered, release):
        with ThreadPoolExecutor(max_workers=2) as pool:
            future = pool.submit(client.get, f"/api/conversations/{conversation}")
            try:
                assert entered.wait(timeout=5), "reader did not reach its plain conversation SELECT"
                if writer_kind == "message-and-job":

                    def write():
                        return service.append_message(
                            "alice",
                            conversation,
                            "改为退货",
                            "while-reading",
                            enqueue=lambda session, msg: enqueue(
                                session,
                                service,
                                conversation,
                                msg.revision,
                                "message",
                                message_id=msg.id,
                            ),
                        )

                else:

                    def write():
                        return service.commit("alice", preview_id)

                written = pool.submit(write).result(timeout=3)
                assert not future.done(), "controlled reader must remain paused while write commits"
            finally:
                release.set()
            response = future.result(timeout=5)
            assert response.status_code == 200, response.text
            snapshot = response.json()
    assert snapshot == expected
    after = reader.get("alice", conversation)
    if writer_kind == "message-and-job":
        assert after["revision"] == 1 and len(after["messages"]) == len(after["jobs"]) == 1
        assert after["messages"][0]["id"] == written["id"]
        assert after["previews"][0]["status"] == "INVALIDATED"
        assert snapshot["previews"][0]["confirmation_available"] is True
    else:
        assert after["operations"] == [written]
        assert after["task_state"] == "SUBMITTED" and after["previews"][0]["status"] == "COMMITTED"
        assert snapshot["operations"] == []


def test_old_read_confirmation_flag_does_not_authorize_a_changed_intent(client, service):
    conversation, preview_id = prepare(service)
    displayed = client.get(f"/api/conversations/{conversation}").json()
    assert displayed["previews"][0]["confirmation_available"] is True
    service.append_message("alice", conversation, "改为退货", "new-intent")
    denied = client.post(f"/api/previews/{preview_id}/confirm", json={"decision": "accept"})
    assert denied.status_code == 409 and denied.json()["error_code"] == "PREVIEW_INVALIDATED"
    with service.sessions() as session:
        assert session.scalar(select(func.count()).select_from(Operation)) == 0


def test_external_read_ignores_writer_row_lock_but_internal_snapshot_still_blocks(service):
    conversation, _ = prepare(service)
    reader = ReadSnapshotService(service)
    expected = legacy_complete(service, conversation)
    engine = service.sessions.kw["bind"]
    with engine.connect() as holder:
        holder.execute(
            text("SELECT id FROM conversations WHERE id=:id FOR UPDATE"), {"id": conversation}
        ).one()
        try:
            with ThreadPoolExecutor(max_workers=1) as pool:
                assert pool.submit(reader.get, "alice", conversation).result(timeout=3) == expected
            with engine.connect() as contender:
                contender.execute(text("SET lock_timeout='150ms'"))
                contender.commit()
                try:
                    bound = sessionmaker(contender, expire_on_commit=False)
                    with pytest.raises(DBAPIError) as error:
                        BusinessService(bound, service.clock).snapshot("alice", conversation)
                    assert getattr(error.value.orig, "sqlstate", None) == "55P03"
                finally:
                    contender.rollback()
                    contender.execute(text("RESET lock_timeout"))
                    contender.commit()
        finally:
            holder.rollback()
