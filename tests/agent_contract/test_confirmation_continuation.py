"""Actual graph ends after preview: trusted confirmation must not become a no-op."""

import inspect
import json
from concurrent.futures import ThreadPoolExecutor

import pytest
from langchain_core.messages import ToolMessage
from sqlalchemy import func, select

from aftercare.agent.runtime import run_turn, setup_checkpoints
from aftercare.config import Settings
from aftercare.domain.contracts import BusinessError
from aftercare.llm.budget import BudgetLedger
from aftercare.llm.scripted import ScriptedModel, offline_plan, response
from aftercare.repositories.models import Application, Inventory, Operation, Order, Reservation


def config(service):
    return Settings(
        _env_file=None,
        demo_login_enabled=True,
        database_url=service.sessions.kw["bind"].url.render_as_string(hide_password=False),
    )


def preview_stop(messages):
    if any(
        isinstance(m, ToolMessage)
        and m.name == "preview_action"
        and json.loads(m.content).get("ok")
        for m in messages
    ):
        return response("预览已生成，请确认是否继续。")
    return offline_plan(messages)


def start(service, stop=True, text="S1002 中 1 件换大一码"):
    settings = config(service)
    setup_checkpoints(settings)
    conv = service.create_conversation("alice")
    msg = service.append_message("alice", conv, text, "first")
    model = ScriptedModel(
        ledger=BudgetLedger(service.sessions),
        conversation_id=conv,
        run_id="initial",
        planner=preview_stop if stop else offline_plan,
    )
    result = run_turn(settings, service, "alice", conv, msg["id"], model=model)
    pid = next(
        p["id"] for p in service.snapshot("alice", conv)["previews"] if p["status"] == "PENDING"
    )
    return settings, conv, pid, result


def resume(settings, service, conv, event, **kw):
    # Before the fix, exercise the exact old API and preserve the behavioral failure.
    args = (
        {"confirmation": event.get("confirmation")}
        if "confirmation" in inspect.signature(run_turn).parameters
        else {}
    )
    return run_turn(settings, service, "alice", conv, resume=True, **args, **kw)


def counts(service):
    with service.sessions() as s:
        return tuple(
            s.scalar(select(func.count()).select_from(t))
            for t in (Operation, Application, Reservation)
        )


def test_ended_graph_confirmation_produces_receipt_not_old_question(service):
    settings, conv, pid, initial = start(service)
    assert not initial.get("__interrupt__")
    event = service.confirm("alice", pid, True)
    result = resume(settings, service, conv, event)
    assert counts(service) == (1, 1, 1)
    assert result["receipt"]["preview_id"] == pid
    assert result["task_state"] == "COMPLETED"
    assert "请确认" not in result["communications"][-1]
    assert service.snapshot("alice", conv)["revision"] == 1


def test_existing_hitl_path_still_submits_once(service):
    settings, conv, pid, initial = start(service, False)
    assert initial.get("__interrupt__")
    event = service.confirm("alice", pid, True)
    result = resume(settings, service, conv, event)
    assert counts(service) == (1, 1, 1)
    assert result["receipt"]["preview_id"] == pid


def test_workbench_confirmation_job_uses_same_ended_graph_runtime(service):
    from fastapi.testclient import TestClient

    from aftercare.api.app import create_app
    from aftercare.domain.contracts import utc_now
    from aftercare.workers.worker import run_once

    service.clock = utc_now
    settings, conv, pid, _ = start(service)
    app = create_app(settings)
    app.state.business = service
    client = TestClient(app)
    who = client.post("/api/demo-sessions", json={"customer_id": "alice"}).json()
    client.headers["X-CSRF-Token"] = who["csrf_token"]
    assert (
        client.post(f"/api/previews/{pid}/confirm", json={"decision": "accept"}).status_code == 202
    )
    assert run_once(settings)
    assert counts(service) == (1, 1, 1)
    snap = client.get(f"/api/conversations/{conv}").json()
    assert snap["task_state"] == "COMPLETED"
    assert "请确认" not in snap["messages"][-1]["text"]


def test_reject_is_terminal_without_submission(service):
    settings, conv, pid, _ = start(service)
    event = service.confirm("alice", pid, False)
    result = resume(settings, service, conv, event, decision="reject")
    assert counts(service) == (0, 0, 0)
    assert result["task_state"] == "REJECTED" and result["receipt"] is None


def test_old_approval_event_after_new_intent_cannot_commit(service):
    settings, conv, pid, _ = start(service)
    event = service.confirm("alice", pid, True)
    service.append_message("alice", conv, "改为退其中 1 件", "second")
    with pytest.raises(BusinessError, match="INTENT_CHANGED"):
        resume(settings, service, conv, event)
    assert counts(service) == (0, 0, 0)
    assert service.snapshot("alice", conv)["revision"] == 2


def test_repeated_confirmation_parallel_resume_and_restart_are_idempotent(service):
    settings, conv, pid, _ = start(service)
    first = service.confirm("alice", pid, True)
    duplicate = service.confirm("alice", pid, True)
    with ThreadPoolExecutor(2) as pool:
        a = pool.submit(resume, settings, service, conv, first)
        b = pool.submit(resume, settings, service, conv, duplicate)
        ra, rb = a.result(), b.result()
    # Each call constructs a fresh graph/checkpointer, equivalent to a restarted runtime.
    rc = resume(settings, service, conv, first)
    assert len({r["receipt"]["operation_id"] for r in (ra, rb, rc)}) == 1
    assert counts(service) == (1, 1, 1)
    with service.sessions() as s:
        stock = s.get(Inventory, "tee-white-L")
        assert (stock.available, stock.reserved) == (0, 1)
        from aftercare.repositories.models import Message

        assert (
            s.scalar(
                select(func.count())
                .select_from(Message)
                .where(
                    Message.conversation_id == conv,
                    Message.client_message_id.like("notice:receipt:%"),
                )
            )
            == 1
        )


@pytest.mark.parametrize("change", ["stock", "order", "policy"])
def test_commit_rechecks_changes_after_trusted_confirmation(service, change):
    settings, conv, pid, _ = start(service)
    event = service.confirm("alice", pid, True)
    with service.sessions.begin() as s:
        if change == "stock":
            row = s.get(Inventory, "tee-white-L")
            row.available = 0
            row.version += 1
        elif change == "order":
            row = s.get(Order, "S1002")
            row.version += 1
            row.status = "SHIPPED"
        else:
            from aftercare.repositories.models import Policy

            row = s.scalar(select(Policy).where(Policy.current.is_(True)))
            row.content_hash = "new-policy-hash"
    result = resume(settings, service, conv, event)
    assert result["task_state"] == "FAILED"
    assert result["error"]["error_code"] in {"STATE_CHANGED", "POLICY_CHANGED"}
    assert counts(service) == (0, 0, 0)
    assert not any(p["confirmation_available"] for p in service.snapshot("alice", conv)["previews"])


def test_post_commit_lost_response_recovers_same_receipt_once(service):
    settings, conv, pid, _ = start(service)
    event = service.confirm("alice", pid, True)
    service.commit_fault = "lost_response"
    result = resume(settings, service, conv, event)
    assert result["task_state"] == "COMPLETED" and result["receipt"]["preview_id"] == pid
    assert "原回执" in result["communications"][0]
    again = resume(settings, service, conv, event)
    assert result["receipt"] == again["receipt"] and counts(service) == (1, 1, 1)
    from aftercare.repositories.models import Event

    with service.sessions() as s:
        events = list(s.scalars(select(Event).where(Event.conversation_id == conv)))
        fault = next(e for e in events if e.event_type == "fault_occurred")
        assert fault.payload["operation_id"] == result["receipt"]["operation_id"]


def test_status_string_or_forged_binding_is_not_approval_authority(service):
    settings, conv, pid, _ = start(service)
    event = service.confirm("alice", pid, True)
    # Even a CONFIRMED row cannot be selected without the exact persisted event.
    with pytest.raises(BusinessError, match="CONFIRMATION_EVENT_REQUIRED"):
        run_turn(settings, service, "alice", conv, resume=True)
    bad = {**event["confirmation"], "parameter_hash": "forged"}
    with pytest.raises(BusinessError, match="CONFIRMATION_BINDING_MISMATCH"):
        run_turn(settings, service, "alice", conv, resume=True, confirmation=bad)
    with pytest.raises(BusinessError):
        run_turn(settings, service, "bob", conv, resume=True, confirmation=event["confirmation"])
    assert counts(service) == (0, 0, 0)


def test_unresolved_write_not_marked_complete(service):
    settings, conv, pid, _ = start(service)
    event = service.confirm("alice", pid, True)
    service.commit_fault = "before_commit"
    result = resume(settings, service, conv, event)
    assert result["task_state"] == "EXECUTION_UNKNOWN"
    assert result["receipt"] is None and counts(service) == (0, 0, 0)
    assert "尚未确定" in result["communications"][0]


def test_confirmation_event_cannot_resume_other_pending_action(service):
    settings, conv, pid, _ = start(service, False)
    event = service.confirm("alice", pid, True)
    # Corrupting the event's object never re-targets the saved interrupt.
    bad = {**event["confirmation"], "preview_id": "another-preview"}
    with pytest.raises(BusinessError, match="CONFIRMATION_BINDING_MISMATCH"):
        run_turn(settings, service, "alice", conv, resume=True, confirmation=bad)
    assert counts(service) == (0, 0, 0)


def test_initial_natural_stop_is_waiting_confirmation_not_task_completion(service):
    settings, conv, pid, result = start(service)
    assert result["graph_state"] == "ENDED"
    assert result["task_state"] == "WAITING_CONFIRMATION"
    assert result["receipt"] is None
    assert "API" not in result["communications"][0]


def test_unknown_result_duplicate_event_only_queries_does_not_write_again(service):
    settings, conv, pid, _ = start(service)
    event = service.confirm("alice", pid, True)
    service.commit_fault = "before_commit"
    assert resume(settings, service, conv, event)["task_state"] == "EXECUTION_UNKNOWN"
    service.commit_fault = None
    assert resume(settings, service, conv, event)["task_state"] == "EXECUTION_UNKNOWN"
    assert counts(service) == (0, 0, 0)
    from aftercare.repositories.models import Event

    with service.sessions() as s:
        assert (
            s.scalar(
                select(func.count())
                .select_from(Event)
                .where(Event.conversation_id == conv, Event.event_type == "commit_execution")
            )
            == 1
        )


def test_receipt_without_final_notice_is_submitted_not_task_completed(service):
    settings, conv, pid, _ = start(service)
    service.confirm("alice", pid, True)
    service.commit("alice", pid)
    assert service.snapshot("alice", conv)["task_state"] == "SUBMITTED"


def test_workbench_expired_lease_and_duplicate_confirmation_keep_one_effect(service):
    from datetime import timedelta

    from fastapi.testclient import TestClient

    from aftercare.api.app import create_app
    from aftercare.domain.contracts import utc_now
    from aftercare.repositories.models import Job
    from aftercare.workers.worker import run_once

    service.clock = utc_now
    settings, conv, pid, _ = start(service)
    app = create_app(settings)
    app.state.business = service
    client = TestClient(app)
    who = client.post("/api/demo-sessions", json={"customer_id": "alice"}).json()
    client.headers["X-CSRF-Token"] = who["csrf_token"]
    for _ in range(2):
        assert (
            client.post(f"/api/previews/{pid}/confirm", json={"decision": "accept"}).status_code
            == 202
        )
    with service.sessions.begin() as s:
        jobs = list(s.scalars(select(Job).where(Job.conversation_id == conv)))
        assert len(jobs) == 1 and jobs[0].result["confirmation"]["preview_id"] == pid
        jobs[0].status = "RUNNING"
        jobs[0].lease_owner = "dead-process"
        jobs[0].lease_expires_at = utc_now() - timedelta(seconds=1)
    assert run_once(settings)
    assert not run_once(settings)
    assert counts(service) == (1, 1, 1)
    snap = client.get(f"/api/conversations/{conv}").json()
    assert snap["task_state"] == "COMPLETED"
    assert all(
        term not in snap["messages"][-1]["text"]
        for term in ("可信", "API", "commit_action", "银行通知")
    )
    with service.sessions() as s:
        assert (
            s.scalar(select(Job).where(Job.conversation_id == conv)).result["receipt"]["preview_id"]
            == pid
        )


def test_workbench_rejected_event_is_terminal_without_submit(service):
    from fastapi.testclient import TestClient

    from aftercare.api.app import create_app
    from aftercare.domain.contracts import utc_now
    from aftercare.workers.worker import run_once

    service.clock = utc_now
    settings, conv, pid, _ = start(service)
    app = create_app(settings)
    app.state.business = service
    client = TestClient(app)
    who = client.post("/api/demo-sessions", json={"customer_id": "alice"}).json()
    client.headers["X-CSRF-Token"] = who["csrf_token"]
    assert (
        client.post(f"/api/previews/{pid}/confirm", json={"decision": "reject"}).status_code == 202
    )
    assert run_once(settings)
    assert counts(service) == (0, 0, 0)
    assert client.get(f"/api/conversations/{conv}").json()["task_state"] == "REJECTED"


def test_model_cannot_report_payment_without_a_real_receipt(service):
    settings = config(service)
    setup_checkpoints(settings)
    conv = service.create_conversation("alice")
    msg = service.append_message("alice", conv, "查询订单 S1001", "first")
    model = ScriptedModel(
        ledger=BudgetLedger(service.sessions),
        conversation_id=conv,
        run_id="bad-notice",
        responses=[response("退款已到账。")],
    )
    result = run_turn(settings, service, "alice", conv, msg["id"], model=model)
    assert "已到账" not in result["communications"][0]
    assert result["task_state"] != "COMPLETED"


def test_legacy_completed_status_without_receipt_is_not_completed_task(service):
    from aftercare.repositories.models import Conversation

    conv = service.create_conversation("alice")
    with service.sessions.begin() as s:
        s.get(Conversation, conv).status = "COMPLETED"
    assert service.snapshot("alice", conv)["task_state"] == "FAILED"
