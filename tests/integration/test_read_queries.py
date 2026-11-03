"""D1 fixed-query reads preserve the original authorization and snapshot protections."""

from contextlib import contextmanager
from datetime import timedelta

import pytest
from sqlalchemy import event, select, text
from test_business import prepared

from aftercare.domain.contracts import BusinessError
from aftercare.repositories.models import (
    Conversation,
    Event,
    Message,
    Order,
    OrderLine,
    Policy,
    Preview,
    Variant,
)
from aftercare.services.business import columns

pytestmark = pytest.mark.integration


@contextmanager
def sql_calls(service):
    statements = []
    engine = service.sessions.kw["bind"]

    def before(connection, cursor, statement, parameters, context, executemany):
        statements.append(" ".join(statement.lower().split()))

    event.listen(engine, "before_cursor_execute", before)
    try:
        yield statements
    finally:
        event.remove(engine, "before_cursor_execute", before)


def variant(index):
    return Variant(
        id=f"read-variant-{index:03d}",
        product_id="read-product",
        title=f"测试商品 {index}",
        color=f"颜色{index}",
        size=f"S{index}",
        price_fen=100 + index,
        returnable=index % 2 == 0,
        exchangeable=index % 3 == 0,
        category="ordinary",
    )


def seed_order(service, line_count, distinct):
    with service.sessions.begin() as session:
        policy = session.scalar(select(Policy).where(Policy.current.is_(True)))
        variants = [
            variant(index) for index in range(line_count if distinct else min(line_count, 1))
        ]
        order = Order(
            id="read-order",
            customer_id="alice",
            status="DELIVERED",
            paid_fen=100 * line_count,
            delivered_at=service.clock(),
            version=7,
            policy_version=policy.id,
        )
        session.add_all([*variants, order])
        session.flush()
        lines = [
            OrderLine(
                id=f"read-line-{index:03d}",
                order_id=order.id,
                variant_id=variants[index if distinct else 0].id,
                quantity=index % 3 + 1,
                occupied=0,
                unused=index % 2 == 0,
            )
            for index in range(line_count)
        ]
        session.add_all(lines)
        session.flush()
        expected_variants = {row.id: columns(row) for row in variants}
        return (
            order.id,
            columns(order),
            {
                line.id: {**columns(line), "variant": expected_variants[line.variant_id]}
                for line in lines
            },
        )


def seed_snapshot(service, preview_count, *, targets=True, distinct=True):
    now = service.clock()
    with service.sessions.begin() as session:
        policy = session.scalar(select(Policy).where(Policy.current.is_(True)))
        conv = Conversation(
            id="read-conversation",
            customer_id="alice",
            revision=3,
            status="WAITING_USER",
            case={"preserve": ["中文", False, 0]},
        )
        variants = [
            variant(index)
            for index in range(
                (preview_count if distinct else min(preview_count, 1)) if targets else 0
            )
        ]
        session.add_all([conv, *variants])
        session.flush()
        messages = [
            Message(
                id=f"read-message-{revision}",
                conversation_id=conv.id,
                client_message_id=f"client-{revision}",
                role="user" if revision != 2 else "assistant",
                text=f"原消息 {revision}",
                revision=revision,
                created_at=now + timedelta(seconds=revision),
            )
            for revision in (2, 1, 3)
        ]
        previews = [
            Preview(
                id=f"read-preview-{index:03d}",
                conversation_id=conv.id,
                customer_id="alice",
                order_id="S1002",
                payload=(
                    {"target_variant": variants[index if distinct else 0].id, "quantity": 1}
                    if targets
                    else {"action": "return", "quantity": 1}
                ),
                parameter_hash=f"read-parameters-{index}",
                revision=3,
                order_version=1,
                eligibility_policy=policy.id,
                flow_policy="v1",
                policy_hash=policy.content_hash,
                status="INVALIDATED",
                expires_at=now + timedelta(minutes=5),
            )
            for index in range(preview_count)
        ]
        session.add_all([*messages, *previews])
        session.add(
            Event(
                conversation_id=conv.id,
                event_type="runtime_state",
                payload={"revision": 3, "task_state": "WAITING_USER", "graph_state": "PAUSED"},
                created_at=now,
            )
        )
        session.flush()
        expected_previews = {
            row.id: {
                **columns(row),
                "target_label": (
                    f"颜色{index if distinct else 0} S{index if distinct else 0} 码"
                    if targets
                    else None
                ),
                "confirmation_available": False,
            }
            for index, row in enumerate(previews)
        }
        return (
            conv.id,
            columns(conv),
            [columns(row) for row in sorted(messages, key=lambda row: row.revision)],
            expected_previews,
        )


def variant_queries(statements):
    return [statement for statement in statements if "from variants " in statement]


@pytest.mark.parametrize("line_count", [1, 10, 100])
@pytest.mark.parametrize("distinct", [False, True])
def test_order_batches_variants_with_original_fields_and_constant_sql(
    service, line_count, distinct
):
    order_id, expected_order, expected_lines = seed_order(service, line_count, distinct)
    with sql_calls(service) as statements:
        result = service.get_order("alice", order_id)
    assert {key: value for key, value in result.items() if key != "lines"} == expected_order
    assert {row["id"]: row for row in result["lines"]} == expected_lines
    assert len(statements) == 3 and len(variant_queries(statements)) == 1
    assert " in (" in variant_queries(statements)[0]


def test_empty_order_does_not_query_variants(service):
    order_id, expected, _ = seed_order(service, 0, True)
    with sql_calls(service) as statements:
        assert service.get_order("alice", order_id) == {**expected, "lines": []}
    assert len(statements) == 2 and variant_queries(statements) == []


@pytest.mark.parametrize("missing", [False, True])
def test_order_authorizes_before_loading_lines_or_variants(service, missing):
    order_id, _, _ = seed_order(service, 10, True)
    with sql_calls(service) as statements, pytest.raises(BusinessError) as error:
        service.get_order("alice" if missing else "bob", "missing" if missing else order_id)
    assert error.value.code == "NOT_FOUND"
    assert len(statements) == 1 and "from orders " in statements[0]


@pytest.mark.parametrize("preview_count", [1, 10, 100])
@pytest.mark.parametrize("distinct", [False, True])
def test_snapshot_batches_target_labels_preserving_entire_read_shape(
    service, preview_count, distinct
):
    conversation_id, expected_conv, messages, previews = seed_snapshot(
        service, preview_count, distinct=distinct
    )
    with sql_calls(service) as statements:
        result = service.snapshot("alice", conversation_id)
    expected_keys = {*expected_conv, "task_state", "graph_state", "messages", "previews"}
    assert set(result) == expected_keys
    assert {key: result[key] for key in expected_conv} == expected_conv
    assert result["task_state"] == "WAITING_USER" and result["graph_state"] == "PAUSED"
    assert result["messages"] == messages
    assert {row["id"]: row for row in result["previews"]} == previews
    assert len(statements) == 7 and len(variant_queries(statements)) == 1
    assert "for update" in statements[0]


@pytest.mark.parametrize("preview_count", [0, 10, 100])
def test_snapshot_without_target_ids_avoids_variant_sql(service, preview_count):
    conversation_id, _, messages, previews = seed_snapshot(service, preview_count, targets=False)
    with sql_calls(service) as statements:
        result = service.snapshot("alice", conversation_id)
    assert result["messages"] == messages
    assert {row["id"]: row for row in result["previews"]} == previews
    assert len(statements) == 6 and variant_queries(statements) == []


def test_snapshot_mixed_preview_states_keep_current_revision_and_expiry_rules(service):
    conversation_id, _, _, _ = seed_snapshot(service, 6, distinct=False)
    with service.sessions.begin() as session:
        previews = list(session.scalars(select(Preview).order_by(Preview.id)))
        previews[0].status = "PENDING"
        previews[1].status = "CONFIRMED"
        previews[2].status, previews[2].expires_at = (
            "PENDING",
            service.clock() - timedelta(seconds=1),
        )
        previews[3].status, previews[3].revision = "PENDING", 2
        previews[4].status = "COMMITTED"
        previews[5].payload = {"action": "return", "quantity": 1}
    with sql_calls(service) as statements:
        result = service.snapshot("alice", conversation_id)
    assert result["task_state"] == "WAITING_SUBMISSION"
    previews = {row["id"]: row for row in result["previews"]}
    assert [
        previews[f"read-preview-{index:03d}"]["confirmation_available"] for index in range(6)
    ] == [True, True, False, False, False, False]
    assert previews["read-preview-005"]["target_label"] is None
    assert all(
        previews[f"read-preview-{index:03d}"]["target_label"] == "颜色0 S0 码" for index in range(5)
    )
    assert len(statements) == 7 and len(variant_queries(statements)) == 1


def test_snapshot_retains_per_preview_clock_evaluation_at_expiry_boundary(service):
    now = service.clock()
    conversation_id, _, _, _ = seed_snapshot(service, 3, distinct=False)
    with service.sessions.begin() as session:
        for row in session.scalars(select(Preview)):
            row.status, row.expires_at = "PENDING", now + timedelta(seconds=1)
    times = iter(now + timedelta(seconds=value) for value in (0, 1, 2))
    ticks = []

    def clock():
        value = next(times)
        ticks.append(value)
        return value

    service.clock = clock
    result = service.snapshot("alice", conversation_id)
    assert len(ticks) == 3
    assert [row["confirmation_available"] for row in result["previews"]] == [True, True, False]


def test_snapshot_keeps_guard_and_message_before_preview_query_order(service):
    conversation_id, _, _, _ = seed_snapshot(service, 10)
    guarded = []

    def guard(session, conv):
        assert session.in_transaction() and conv.id == conversation_id
        guarded.append(conv.id)
        session.execute(text("SELECT 1"))

    service.run_guard = guard
    with sql_calls(service) as statements:
        service.snapshot("alice", conversation_id)
    assert guarded == [conversation_id]
    assert "for update" in statements[0] and statements[1] == "select 1"
    message_position = next(
        index for index, sql in enumerate(statements) if "from messages " in sql
    )
    preview_position = next(
        index
        for index, sql in enumerate(statements)
        if "from action_previews " in sql and "status =" not in sql.split(" where ")[-1]
    )
    assert message_position < preview_position < statements.index(variant_queries(statements)[0])
    assert len(statements) == 8


@pytest.mark.parametrize("wrong_owner", [False, True])
def test_snapshot_refuses_before_association_reads_when_owner_or_guard_fails(service, wrong_owner):
    conversation_id, _, _, _ = seed_snapshot(service, 10)
    guard_calls = []

    def guard(session, conv):
        guard_calls.append(conv.id)
        raise BusinessError("JOB_LEASE_LOST")

    service.run_guard = guard
    with sql_calls(service) as statements, pytest.raises(BusinessError) as error:
        service.snapshot("bob" if wrong_owner else "alice", conversation_id)
    assert error.value.code == ("NOT_FOUND" if wrong_owner else "JOB_LEASE_LOST")
    assert guard_calls == ([] if wrong_owner else [conversation_id])
    assert len(statements) == 1 and "for update" in statements[0]


@pytest.mark.parametrize("runtime", ["SUBMITTED", "COMPLETED", "EXECUTION_UNKNOWN"])
def test_snapshot_preserves_committed_operation_task_state_and_original_receipt(service, runtime):
    conversation_id, preview_id = prepared(service)
    receipt = service.commit("alice", preview_id)
    if runtime != "SUBMITTED":
        with service.sessions.begin() as session:
            service.event(
                session,
                conversation_id,
                "user_notice" if runtime == "COMPLETED" else "runtime_state",
                (
                    {"stage": "final", "operation_id": receipt["operation_id"]}
                    if runtime == "COMPLETED"
                    else {"revision": 0, "task_state": "EXECUTION_UNKNOWN", "graph_state": "END"}
                ),
            )
    with sql_calls(service) as statements:
        result = service.snapshot("alice", conversation_id)
    assert result["task_state"] == runtime
    assert result["previews"][0]["target_label"] == "白色 L 码"
    assert result["previews"][0]["confirmation_available"] is False
    assert service.get_operation("alice", preview_id=preview_id) == receipt
    assert len(statements) == (7 if runtime == "EXECUTION_UNKNOWN" else 8)
