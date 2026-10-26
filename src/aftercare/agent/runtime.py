"""Single LangChain Agent; HITL checkpoints are separate from business receipts."""

from contextlib import contextmanager

from langchain.agents import create_agent
from langchain.agents.middleware import HumanInTheLoopMiddleware
from langchain_core.messages import HumanMessage
from langgraph.checkpoint.postgres import PostgresSaver
from langgraph.types import Command

from aftercare.agent.middleware import BusinessBoundary
from aftercare.agent.tools import toolset
from aftercare.llm.budget import BudgetLedger
from aftercare.llm.scripted import ScriptedModel
from aftercare.repositories.models import Message
from aftercare.retrieval.engine import PolicySearch


def factory(
    service,
    principal,
    conversation,
    checkpointer,
    run_id=None,
    model=None,
    profile="full",
    retrieval_profile=None,
):
    model = model or ScriptedModel(
        ledger=BudgetLedger(service.sessions),
        conversation_id=conversation,
        run_id=run_id or f"{conversation}:{service.snapshot(principal, conversation)['revision']}",
    )
    search = PolicySearch()
    retrieval_profile = retrieval_profile or (
        "full-policy" if profile == "react-full-policy" else "bm25"
    )
    if retrieval_profile not in {"full-policy", "bm25", "hybrid-rerank"}:
        raise ValueError("UNSUPPORTED_PRODUCT_RETRIEVAL_PROFILE")
    with service.sessions.begin() as session:
        service._conversation(session, principal, conversation)
        service.event(
            session,
            conversation,
            "runtime_profile",
            {
                "context_profile": profile,
                "retrieval_profile": retrieval_profile,
                "retrieval_corpus_hash": search.corpus_hash,
            },
        )
    tools = toolset(
        service,
        principal,
        conversation,
        search,
        retrieval_profile=retrieval_profile,
    )
    graph = create_agent(
        model=model,
        tools=tools,
        middleware=[
            BusinessBoundary(service, principal, conversation, profile),
            HumanInTheLoopMiddleware(
                interrupt_on={"commit_action": {"allowed_decisions": ["approve", "reject"]}},
                interrupt_mode="batched",
            ),
        ],
        checkpointer=checkpointer,
        name="aftercare-main",
    )
    return graph, model


@contextmanager
def agent_runtime(
    settings, service, principal, conversation, run_id=None, model=None, profile="full"
):
    url = service.sessions.kw["bind"].url
    conn_string = url.render_as_string(hide_password=False).replace(
        "postgresql+psycopg://", "postgresql://"
    )
    with PostgresSaver.from_conn_string(conn_string) as saver:
        graph, bound_model = factory(
            service, principal, conversation, saver, run_id, model, profile
        )
        yield graph, bound_model


def setup_checkpoints(settings):
    from aftercare.repositories.database import session_factory

    sessions = session_factory(settings)
    url = (
        sessions.kw["bind"]
        .url.render_as_string(hide_password=False)
        .replace("postgresql+psycopg://", "postgresql://")
    )
    with PostgresSaver.from_conn_string(url) as saver:
        saver.setup()
    sessions.kw["bind"].dispose()


def _run_turn(
    settings,
    service,
    principal,
    conversation,
    message_id=None,
    resume=False,
    run_id=None,
    profile="full",
    decision="approve",
    model=None,
    confirmation=None,
):
    if not resume and message_id:
        from aftercare.services.user_pause import pause_message

        if pause_message(service, principal, conversation, message_id):
            from aftercare.agent.presentation import present

            # append_message already invalidated older previews. Leave their checkpoints
            # untouched; a later new intent uses the existing invalidation/resume rules.
            return present(
                service,
                principal,
                conversation,
                {
                    "_user_paused": True,
                    "_graph_state": "NOT_INVOKED",
                    "messages": [],
                },
            )
    if profile == "fixed-workflow":
        from aftercare.agent.presentation import present

        if resume:
            from types import SimpleNamespace

            from aftercare.agent.continuation import continue_confirmation

            state = SimpleNamespace(tasks=[], next=(), values={"messages": []})
            result, receipt, error, recovered, pid = continue_confirmation(
                None, state, None, service, principal, conversation, confirmation, decision
            )
            return present(service, principal, conversation, result, receipt, pid, error, recovered)
        from aftercare.agent.workflow import run_workflow

        return present(
            service,
            principal,
            conversation,
            run_workflow(service, principal, conversation, message_id, False, decision),
        )
    if profile not in {"full", "retrieval", "react-full-policy", "harness-evidence-v1"}:
        raise ValueError("UNKNOWN_AGENT_PROFILE")
    config = {"configurable": {"thread_id": conversation}, "recursion_limit": 96}
    with agent_runtime(
        settings, service, principal, conversation, run_id=run_id, model=model, profile=profile
    ) as (graph, model):
        state = graph.get_state(config)
        if state.next and not resume:
            last_human = next(
                (
                    m
                    for m in reversed(state.values.get("messages", []))
                    if isinstance(m, HumanMessage)
                ),
                None,
            )
            if last_human is not None and last_human.id == message_id:
                interrupts = [i for task in state.tasks for i in task.interrupts]
                from aftercare.agent.presentation import present

                if interrupts:
                    return present(
                        service,
                        principal,
                        conversation,
                        {**state.values, "__interrupt__": interrupts},
                    )
                return present(service, principal, conversation, graph.invoke(None, config))
            # Reject the old interrupt only after the new message invalidates its business preview.
            graph.invoke(
                Command(
                    resume={
                        "decisions": [{"type": "reject", "message": "用户已更新诉求，旧方案失效"}]
                    }
                ),
                config,
            )
        if resume:
            from aftercare.agent.continuation import continue_confirmation
            from aftercare.agent.presentation import present

            result, receipt, error, recovered, pid = continue_confirmation(
                graph, state, config, service, principal, conversation, confirmation, decision
            )
            return present(service, principal, conversation, result, receipt, pid, error, recovered)
        else:
            with service.sessions() as s:
                msg = s.get(Message, message_id)
                if not msg or msg.conversation_id != conversation:
                    raise ValueError("MESSAGE_NOT_FOUND")
                human = HumanMessage(
                    content=msg.text, id=msg.id, additional_kwargs={"intent_revision": msg.revision}
                )
            result = graph.invoke({"messages": [human]}, config)
        from aftercare.agent.presentation import present

        return present(service, principal, conversation, result)


def run_turn(
    settings,
    service,
    principal,
    conversation,
    message_id=None,
    resume=False,
    run_id=None,
    profile="full",
    decision="approve",
    model=None,
    confirmation=None,
):
    # Shared by direct/evaluator and worker paths, including process restarts.
    from sqlalchemy import text

    with service.sessions.kw["bind"].connect() as lock:
        lock.execute(text("SELECT pg_advisory_lock(hashtextextended(:id,2))"), {"id": conversation})
        lock.commit()
        try:
            return _run_turn(
                settings,
                service,
                principal,
                conversation,
                message_id,
                resume,
                run_id,
                profile,
                decision,
                model,
                confirmation,
            )
        finally:
            lock.execute(
                text("SELECT pg_advisory_unlock(hashtextextended(:id,2))"), {"id": conversation}
            )
            lock.commit()
