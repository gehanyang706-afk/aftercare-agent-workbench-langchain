"""Framework migration contracts: native interrupts and model-selected tool paths."""

import importlib.util
import json

import pytest
from langchain_core.messages import ToolMessage

from aftercare.agent.runtime import agent_runtime, run_turn
from aftercare.agent.tools import ALLOWED
from aftercare.llm.budget import BudgetLedger
from aftercare.llm.scripted import ScriptedModel, call, response
from tests.agent_contract.test_confirmation_continuation import config, counts, start


def test_new_environment_and_product_do_not_need_deepagents():
    assert importlib.util.find_spec("deepagents") is None
    from langchain.agents import create_agent

    assert callable(create_agent)


@pytest.mark.parametrize("reverse", [False, True])
def test_model_can_choose_catalogue_before_order_or_order_before_catalogue(
    service, monkeypatch, reverse
):
    from aftercare.agent import workflow
    from aftercare.agent.runtime import setup_checkpoints

    def reject_fixed_workflow(*args, **kwargs):
        raise AssertionError("full must execute model-selected tools")

    monkeypatch.setattr(workflow, "run_workflow", reject_fixed_workflow)
    settings = config(service)
    setup_checkpoints(settings)
    conv = service.create_conversation("alice")
    msg = service.append_message("alice", conv, "查看我的订单和商品目录", "start")
    calls = [
        call("get_inventory", {"product_id": "tee"}),
        call("get_order", {"order_id": "S1002"}),
    ]
    if reverse:
        calls.reverse()
    model = ScriptedModel(
        ledger=BudgetLedger(service.sessions),
        conversation_id=conv,
        run_id="autonomous-tools",
        responses=[response(calls=[c]) for c in calls] + [response("订单和目录已查询。")],
    )
    result = run_turn(settings, service, "alice", conv, msg["id"], model=model)
    replies = [m for m in result["messages"] if isinstance(m, ToolMessage)]
    assert [m.name for m in replies] == [c["name"] for c in calls]
    assert all(json.loads(m.content)["ok"] for m in replies)
    assert set(model.shared["tool_names"]) == ALLOWED
    assert counts(service) == (0, 0, 0)
    assert result["task_state"] != "COMPLETED"


def test_native_batched_interrupt_preserves_existing_confirmation_protocol(service):
    settings, conv, pid, initial = start(service, False)
    assert initial["__interrupt__"]
    with agent_runtime(settings, service, "alice", conv) as (graph, _):
        state = graph.get_state({"configurable": {"thread_id": conv}})
        interrupts = [i for t in state.tasks for i in t.interrupts]
        assert len(interrupts) == 1
        payload = interrupts[0].value
        assert payload["action_requests"][0]["name"] == "commit_action"
        assert payload["action_requests"][0]["args"] == {"preview_id": pid}
        assert payload["review_configs"][0]["allowed_decisions"] == ["approve", "reject"]
        assert not {"files", "todos"} & set(state.values)
    assert counts(service) == (0, 0, 0)
    binding = service.confirm("alice", pid, True)["confirmation"]
    result = run_turn(settings, service, "alice", conv, resume=True, confirmation=binding)
    assert result["receipt"]["preview_id"] == pid
    assert result["task_state"] == "COMPLETED"
    assert counts(service) == (1, 1, 1)
