"""Known pre-dispatch limits after commit must not erase the bound final notice."""

import json

import pytest
from langchain_core.messages import ToolMessage
from sqlalchemy import func, select

from aftercare.agent.runtime import run_turn, setup_checkpoints
from aftercare.config import Settings
from aftercare.domain.contracts import BusinessError
from aftercare.llm.budget import BudgetLedger
from aftercare.llm.scripted import ScriptedModel
from aftercare.repositories.models import Application, Operation, Reservation


class PostCommitLimitModel(ScriptedModel):
    limit_code: str = "CONVERSATION_TOKEN_LIMIT"

    def _generate(self, messages, **kwargs):
        committed = any(
            isinstance(m, ToolMessage)
            and m.name == "commit_action"
            and json.loads(m.content).get("ok")
            for m in messages
        )
        if committed:
            self.shared["blocked"] = self.shared.get("blocked", 0) + 1
            raise BusinessError(self.limit_code)
        return super()._generate(messages, **kwargs)


def initial(service, code):
    settings = Settings(
        _env_file=None,
        database_url=service.sessions.kw["bind"].url.render_as_string(hide_password=False),
    )
    setup_checkpoints(settings)
    conv = service.create_conversation("alice")
    msg = service.append_message("alice", conv, "S1002 中 1 件换大一码", "first")
    model = PostCommitLimitModel(
        ledger=BudgetLedger(service.sessions),
        conversation_id=conv,
        run_id="initial",
        limit_code=code,
    )
    result = run_turn(settings, service, "alice", conv, msg["id"], model=model)
    assert result.get("__interrupt__")
    pid = service.snapshot("alice", conv)["previews"][0]["id"]
    event = service.confirm("alice", pid, True)["confirmation"]
    return settings, conv, pid, event, model


@pytest.mark.parametrize(
    "code",
    ["CONVERSATION_TOKEN_LIMIT", "BUDGET_EXHAUSTED", "MODEL_CALL_LIMIT", "BATCH_REQUEST_LIMIT"],
)
def test_post_commit_budget_stop_finishes_notice_but_preserves_stop_signal(service, code):
    settings, conv, pid, event, model = initial(service, code)
    calls = model.shared["calls"]
    result = run_turn(
        settings, service, "alice", conv, resume=True, confirmation=event, model=model
    )
    assert result["execution_stop"] == code
    assert result["task_state"] == "COMPLETED"
    assert result["graph_state"] == "STOPPED"
    assert result["receipt"]["preview_id"] == pid
    assert "已提交换货申请" in result["communications"][0]
    assert model.shared["calls"] == calls
    assert model.shared["blocked"] == 1
    from evaluation.evaluator.pilot_actor import output_for

    response = output_for(result)
    assert not response["ok"] and response["error"] == code
    assert response["communications"] == result["communications"]
    again = run_turn(settings, service, "alice", conv, resume=True, confirmation=event, model=model)
    assert again["receipt"]["operation_id"] == result["receipt"]["operation_id"]
    assert again["graph_state"] == "STOPPED"
    assert again["execution_stop"] == code
    assert model.shared["blocked"] == 1
    with service.sessions() as s:
        assert tuple(
            s.scalar(select(func.count()).select_from(cls))
            for cls in (Operation, Application, Reservation)
        ) == (1, 1, 1)


@pytest.mark.parametrize(
    "code",
    [
        "PROVIDER_PROTOCOL_FINISH",
        "PROVIDER_HTTP_401",
        "PROVIDER_HTTP_429",
        "USAGE_UNKNOWN",
        "SECURITY_UNAUTHORIZED_TOOL_ATTEMPT",
        "TASK_DEADLINE_EXCEEDED",
    ],
)
def test_post_commit_model_stop_keeps_original_error_and_failing_evaluation(service, code):
    from evaluation.evaluator.pilot import stop_batch
    from evaluation.evaluator.pilot_actor import output_for

    settings, conv, pid, event, model = initial(service, code)
    result = run_turn(
        settings, service, "alice", conv, resume=True, confirmation=event, model=model
    )
    assert result["task_state"] == "COMPLETED"
    assert result["graph_state"] == "STOPPED"
    assert result["execution_stop"] == code
    assert result["receipt"]["preview_id"] == pid
    assert output_for(result)["ok"] is False
    assert output_for(result)["error"] == code
    assert stop_batch(code)


def test_budget_stop_without_receipt_cannot_manufacture_completion(service):
    from aftercare.agent.continuation import continue_confirmation
    from aftercare.agent.runtime import agent_runtime

    settings, conv, pid, event, model = initial(service, "CONVERSATION_TOKEN_LIMIT")
    config = {"configurable": {"thread_id": conv}}
    with agent_runtime(settings, service, "alice", conv, model=model) as (graph, _):
        state = graph.get_state(config)

        class NeverSubmitted:
            def invoke(self, *args):
                raise BusinessError("CONVERSATION_TOKEN_LIMIT")

            def get_state(self, *args):
                return state

        with pytest.raises(BusinessError, match="CONVERSATION_TOKEN_LIMIT"):
            continue_confirmation(
                NeverSubmitted(), state, config, service, "alice", conv, event, "approve"
            )
    with service.sessions() as s:
        assert s.scalar(select(func.count()).select_from(Operation)) == 0


def test_workbench_queue_uses_same_finalization_and_keeps_stop_metadata(service, monkeypatch):
    from fastapi.testclient import TestClient

    from aftercare.api.app import create_app
    from aftercare.domain.contracts import utc_now
    from aftercare.repositories.models import Job
    from aftercare.workers import worker

    service.clock = utc_now
    settings = Settings(
        _env_file=None,
        demo_login_enabled=True,
        database_url=service.sessions.kw["bind"].url.render_as_string(hide_password=False),
    )
    setup_checkpoints(settings)
    app = create_app(settings)
    app.state.business = service
    client = TestClient(app)
    who = client.post("/api/demo-sessions", json={"customer_id": "alice"}).json()
    client.headers["X-CSRF-Token"] = who["csrf_token"]
    conv = service.create_conversation("alice")
    msg = service.append_message("alice", conv, "S1002 中 1 件换大一码", "first")
    model = ScriptedModel(
        ledger=BudgetLedger(service.sessions), conversation_id=conv, run_id="initial"
    )
    run_turn(settings, service, "alice", conv, msg["id"], model=model)
    pid = service.snapshot("alice", conv)["previews"][0]["id"]
    assert (
        client.post(f"/api/previews/{pid}/confirm", json={"decision": "accept"}).status_code == 202
    )

    def same_runtime(settings, business, principal, conversation, *args, **kwargs):
        model = PostCommitLimitModel(
            ledger=BudgetLedger(business.sessions),
            conversation_id=conversation,
            run_id="worker-limit",
        )
        return run_turn(settings, business, principal, conversation, *args, model=model, **kwargs)

    monkeypatch.setattr(worker, "run_turn", same_runtime)
    monkeypatch.setattr(worker, "bounded", lambda fn, args, **kwargs: fn(*args))
    assert worker.run_once(settings)
    assert not worker.run_once(settings)
    snap = client.get(f"/api/conversations/{conv}").json()
    assert snap["task_state"] == "COMPLETED"
    with service.sessions() as s:
        job = s.scalar(select(Job).where(Job.conversation_id == conv))
        assert job.status == "COMPLETED"
        assert job.result["execution_stop"] == "CONVERSATION_TOKEN_LIMIT"
        assert job.result["receipt"]["preview_id"] == pid
        assert "已提交换货申请" in job.result["text"]


def test_receipt_notice_does_not_zero_or_unpause_unknown_model_usage(service):
    from aftercare.repositories.models import Budget, ModelCall

    class UnknownUsageModel(PostCommitLimitModel):
        def _generate(self, messages, **kwargs):
            if any(
                isinstance(m, ToolMessage)
                and m.name == "commit_action"
                and json.loads(m.content).get("ok")
                for m in messages
            ):
                call = self.ledger.reserve(
                    "scripted",
                    self.model_name,
                    "offline",
                    self.conversation_id,
                    "unknown",
                    1024,
                    512,
                    self.cap,
                )
                self.ledger.settle(call, None)
                self.shared["unknown_call"] = call
                raise BusinessError("USAGE_UNKNOWN")
            return super()._generate(messages, **kwargs)

    settings, conv, pid, event, original = initial(service, "USAGE_UNKNOWN")
    model = UnknownUsageModel(ledger=original.ledger, conversation_id=conv, run_id="unknown")
    result = run_turn(
        settings, service, "alice", conv, resume=True, confirmation=event, model=model
    )
    assert result["execution_stop"] == "USAGE_UNKNOWN"
    assert result["task_state"] == "COMPLETED"
    with service.sessions() as s:
        call = s.get(ModelCall, model.shared["unknown_call"])
        budget = s.get(Budget, call.budget_id)
        assert call.usage is None and call.status == "USAGE_UNKNOWN"
        assert call.reserved == budget.reserved == 1536
        assert budget.paused
