"""B1 domain contracts against PostgreSQL; Actors are trusted fixture inputs only."""

from concurrent.futures import ThreadPoolExecutor
from hashlib import sha256
from threading import Barrier

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from aftercare.domain.contracts import ActionInput, BusinessError
from aftercare.domain.fulfillment import Actor, FulfillmentCommandInput
from aftercare.repositories.models import (
    Application,
    ExchangeFulfillment,
    FulfillmentAudit,
    FulfillmentCommand,
    Inventory,
    Operation,
    OrderLine,
    Reservation,
)
from aftercare.services.business import columns
from aftercare.services.demo import settle_application
from aftercare.services.fulfillment import FulfillmentService
from aftercare.services.simulated_warehouse import SimulatedWarehouse

pytestmark = pytest.mark.integration

CUSTOMER = Actor(actor_id="alice", role="customer", source="test_fixture")
OTHER_CUSTOMER = Actor(actor_id="bob", role="customer", source="test_fixture")
SUPPORT = Actor(actor_id="support-1", role="support", source="test_fixture")
WAREHOUSE = Actor(actor_id="warehouse-1", role="warehouse", source="test_fixture")
ADAPTER = Actor(actor_id="warehouse-adapter", role="warehouse", source="trusted_adapter")

FLOW = [
    (SUPPORT, "approve", {"note": "Eligible exchange"}, "AWAITING_RETURN"),
    (
        CUSTOMER,
        "submit_return",
        {"carrier": "local-simulator", "tracking_number": "RETURN-001"},
        "RETURN_IN_TRANSIT",
    ),
    (WAREHOUSE, "receive", {"note": "Return accepted"}, "RETURN_RECEIVED"),
    (WAREHOUSE, "request_shipment", {}, "REPLACEMENT_PENDING"),
    (
        ADAPTER,
        "confirm_shipment",
        {"external_shipment_id": "SIM-SHIPMENT-001"},
        "REPLACEMENT_SHIPPED",
    ),
    (CUSTOMER, "complete", {}, "COMPLETED"),
]


def command(name, version, key=None, data=None):
    return FulfillmentCommandInput(
        command=name,
        expected_version=version,
        idempotency_key=key or f"{name}:{version}",
        data=data or {},
    )


def application(service, *, owner="alice", order="S1002", kind="exchange", approved=True):
    conversation = service.create_conversation(owner)
    service.append_message(owner, conversation, "Exchange one item", "initial-request")
    preview = service.preview(
        owner,
        conversation,
        ActionInput(
            action=kind,
            order_id=order,
            line_id=f"{order}-1",
            quantity=1,
            target_variant="tee-white-L" if kind == "exchange" else None,
        ),
    )
    if approved:
        service.confirm(owner, preview["id"], True)
    return conversation, preview["id"]


def prepared(service):
    conversation, preview = application(service)
    receipt = service.commit("alice", preview)
    return (
        FulfillmentService(service.sessions, clock=service.clock),
        receipt["application_id"],
        receipt,
        conversation,
    )


def failure(code, call):
    with pytest.raises(BusinessError) as raised:
        call()
    assert raised.value.code == code


def persisted(service, application_id):
    """Read every affected business record, including command and audit payloads."""
    with service.sessions() as session:
        app = session.get(Application, application_id)
        return {
            "application": columns(app),
            "fulfillment": columns(session.get(ExchangeFulfillment, application_id)),
            "inventory": columns(session.get(Inventory, "tee-white-L")),
            "old_inventory": columns(session.get(Inventory, "tee-white-M")),
            "line": columns(session.get(OrderLine, app.line_id)),
            "reservation": columns(session.get(Reservation, application_id)),
            "operation": columns(session.get(Operation, app.operation_id)),
            "commands": [
                columns(row)
                for row in session.scalars(
                    select(FulfillmentCommand)
                    .where(FulfillmentCommand.application_id == application_id)
                    .order_by(FulfillmentCommand.id)
                )
            ],
            "audits": [
                columns(row)
                for row in session.scalars(
                    select(FulfillmentAudit)
                    .where(FulfillmentAudit.application_id == application_id)
                    .order_by(FulfillmentAudit.to_version)
                )
            ],
        }


def advance(service, fulfillment, application_id, *, before):
    version = 1
    for actor, name, data, _status in FLOW:
        if name == before:
            return version
        result = fulfillment.execute(actor, application_id, command(name, version, data=data))
        version = result["version"]
    raise AssertionError(f"Unknown flow command: {before}")


def test_creation_requires_trusted_confirmation_and_creates_one_native_fulfillment(service):
    _conversation, preview = application(service, approved=False)
    failure("APPROVAL_REQUIRED", lambda: service.commit("alice", preview))
    with service.sessions() as session:
        for model in (Application, ExchangeFulfillment, FulfillmentAudit, FulfillmentCommand):
            assert session.scalar(select(func.count()).select_from(model)) == 0

    service.confirm("alice", preview, True)
    receipt = service.commit("alice", preview)
    assert service.commit("alice", preview) == receipt
    state = persisted(service, receipt["application_id"])
    assert state["application"]["status"] == "EXCHANGE_REQUESTED"
    assert state["application"]["version"] == 1
    assert state["fulfillment"]["origin"] == "native"
    assert state["commands"] == []
    assert len(state["audits"]) == 1
    assert state["audits"][0]["to_state"] == "EXCHANGE_REQUESTED"
    assert state["audits"][0]["to_version"] == 1
    assert state["reservation"]["status"] == "RESERVED"
    assert (state["inventory"]["available"], state["inventory"]["reserved"]) == (0, 1)


def test_complete_exchange_keeps_receipt_and_stock_consistent_with_replay_at_every_step(service):
    fulfillment, application_id, original_receipt, _conversation = prepared(service)
    version = 1
    for actor, name, data, expected_status in FLOW:
        value = command(name, version, data=data)
        result = fulfillment.execute(actor, application_id, value)
        assert result["application_id"] == application_id
        assert result["command"] == name
        assert result["status"] == expected_status
        assert result["version"] == version + 1
        assert result["command_id"]
        before_replay = persisted(service, application_id)
        assert fulfillment.execute(actor, application_id, value) == result
        assert persisted(service, application_id) == before_replay
        assert service.get_operation("alice", operation_id=original_receipt["operation_id"]) == (
            original_receipt
        )
        assert before_replay["old_inventory"]["available"] == 5
        assert before_replay["old_inventory"]["reserved"] == 0
        assert before_replay["line"]["occupied"] == 1
        if name in {"confirm_shipment", "complete"}:
            assert before_replay["reservation"]["status"] == "CONSUMED"
            assert before_replay["inventory"]["reserved"] == 0
        else:
            assert before_replay["reservation"]["status"] == "RESERVED"
            assert before_replay["inventory"]["reserved"] == 1
        version = result["version"]

    state = persisted(service, application_id)
    assert state["application"]["status"] == "COMPLETED"
    assert state["application"]["version"] == 7
    assert state["inventory"]["available"] == 0
    assert len(state["commands"]) == 6
    assert len(state["audits"]) == 7
    assert [row["to_version"] for row in state["audits"]] == list(range(1, 8))
    assert [row["to_state"] for row in state["audits"]] == [
        "EXCHANGE_REQUESTED",
        *[step[3] for step in FLOW],
    ]
    assert [row["actor_id"] for row in state["audits"][1:]] == [step[0].actor_id for step in FLOW]
    assert state["fulfillment"]["return_carrier"] == "local-simulator"
    assert state["fulfillment"]["return_tracking"] == "RETURN-001"
    assert state["fulfillment"]["effect_key"] == f"exchange:{application_id}"
    assert state["fulfillment"]["shipment_status"] == "SUCCEEDED"
    assert state["fulfillment"]["external_shipment_id"] == "SIM-SHIPMENT-001"
    view = fulfillment.get(CUSTOMER, application_id)
    assert (view["status"], view["version"]) == ("COMPLETED", 7)
    assert len(view["history"]) == 7
    assert original_receipt["result"] == "EXCHANGE_REQUESTED"


@pytest.mark.parametrize(
    "approved,terminal", [(False, "reject"), (False, "withdraw"), (True, "withdraw")]
)
def test_reject_and_early_withdraw_release_inventory_and_quantity_only_once(
    service, approved, terminal
):
    fulfillment, application_id, _receipt, _conversation = prepared(service)
    version = 1
    if approved:
        version = fulfillment.execute(SUPPORT, application_id, command("approve", version))[
            "version"
        ]
    actor = SUPPORT if terminal == "reject" else CUSTOMER
    value = command(terminal, version)
    result = fulfillment.execute(actor, application_id, value)
    state = persisted(service, application_id)
    assert state["application"]["status"] == ("REJECTED" if terminal == "reject" else "WITHDRAWN")
    assert (state["inventory"]["available"], state["inventory"]["reserved"]) == (1, 0)
    assert state["reservation"]["status"] == "RELEASED"
    assert state["line"]["occupied"] == 0
    assert fulfillment.execute(actor, application_id, value) == result
    failure(
        "ILLEGAL_TRANSITION",
        lambda: fulfillment.execute(
            actor, application_id, command(terminal, result["version"], key="different-key")
        ),
    )
    assert persisted(service, application_id) == state


@pytest.mark.parametrize("target", ["receive", "request_shipment", "confirm_shipment", "complete"])
def test_completed_step_with_a_new_key_cannot_repeat_inventory_or_state_effects(service, target):
    fulfillment, application_id, _receipt, _conversation = prepared(service)
    version = advance(service, fulfillment, application_id, before=target)
    actor, name, data, _status = next(step for step in FLOW if step[1] == target)
    result = fulfillment.execute(actor, application_id, command(name, version, data=data))
    state = persisted(service, application_id)
    failure(
        "ILLEGAL_TRANSITION",
        lambda: fulfillment.execute(
            actor, application_id, command(name, result["version"], key="repeat-new-key", data=data)
        ),
    )
    assert persisted(service, application_id) == state


@pytest.mark.parametrize(
    "actor,name,error",
    [
        (None, "approve", "ACTOR_REQUIRED"),
        ({"actor_id": "alice", "role": "support"}, "approve", "ACTOR_INVALID"),
        (Actor(actor_id="", role="support", source="test_fixture"), "approve", "ACTOR_INVALID"),
        (Actor(actor_id="admin", role="admin", source="test_fixture"), "approve", "ACTOR_INVALID"),
        (
            Actor(actor_id="support-1", role="support", source="client_header"),
            "approve",
            "ACTOR_INVALID",
        ),
        (CUSTOMER, "approve", "FORBIDDEN"),
        (CUSTOMER, "reject", "FORBIDDEN"),
        (SUPPORT, "receive", "FORBIDDEN"),
        (WAREHOUSE, "approve", "FORBIDDEN"),
        (SUPPORT, "request_shipment", "FORBIDDEN"),
        (WAREHOUSE, "complete", "FORBIDDEN"),
        (WAREHOUSE, "confirm_shipment", "FORBIDDEN"),
    ],
)
def test_service_enforces_actor_contract_before_any_mutation(service, actor, name, error):
    fulfillment, application_id, _receipt, _conversation = prepared(service)
    state = persisted(service, application_id)
    data = {"external_shipment_id": "SIM-UNTRUSTED"} if name == "confirm_shipment" else {}
    failure(error, lambda: fulfillment.execute(actor, application_id, command(name, 1, data=data)))
    assert persisted(service, application_id) == state


def test_customer_cannot_read_or_withdraw_another_customers_application(service):
    fulfillment, application_id, _receipt, _conversation = prepared(service)
    state = persisted(service, application_id)
    failure("NOT_FOUND", lambda: fulfillment.get(OTHER_CUSTOMER, application_id))
    failure(
        "NOT_FOUND",
        lambda: fulfillment.execute(OTHER_CUSTOMER, application_id, command("withdraw", 1)),
    )
    assert persisted(service, application_id) == state


def test_idempotent_replay_precedes_version_check_but_never_authorization(service):
    fulfillment, application_id, _receipt, _conversation = prepared(service)
    value = command("approve", 1, key="review-one", data={"note": "First approval"})
    first = fulfillment.execute(SUPPORT, application_id, value)
    state = persisted(service, application_id)
    assert fulfillment.execute(SUPPORT, application_id, value) == first
    failure("FORBIDDEN", lambda: fulfillment.execute(CUSTOMER, application_id, value))
    failure(
        "VERSION_CONFLICT",
        lambda: fulfillment.execute(
            SUPPORT, application_id, command("reject", 1, key="stale-review")
        ),
    )
    failure(
        "IDEMPOTENCY_CONFLICT",
        lambda: fulfillment.execute(
            SUPPORT,
            application_id,
            command("approve", 1, key="review-one", data={"note": "Changed"}),
        ),
    )
    assert persisted(service, application_id) == state


@pytest.mark.parametrize(
    "name,data",
    [
        ("approve", {"role": "support"}),
        ("withdraw", {"customer_id": "alice"}),
        ("submit_return", {"carrier": "local-simulator"}),
        ("submit_return", {"carrier": "", "tracking_number": "RETURN-001"}),
        ("submit_return", {"carrier": "local-simulator", "tracking_number": ""}),
        ("receive", {"accepted": True}),
        ("confirm_shipment", {"external_shipment_id": ""}),
    ],
)
def test_command_specific_parameters_are_validated_without_side_effects(service, name, data):
    fulfillment, application_id, _receipt, _conversation = prepared(service)
    version = (
        1 if name == "withdraw" else advance(service, fulfillment, application_id, before=name)
    )
    actor = next(step[0] for step in FLOW if step[1] == name) if name != "withdraw" else CUSTOMER
    state = persisted(service, application_id)
    failure(
        "INVALID_COMMAND_DATA",
        lambda: fulfillment.execute(actor, application_id, command(name, version, data=data)),
    )
    assert persisted(service, application_id) == state


def test_racing_review_and_withdraw_allow_only_one_versioned_transition(service):
    fulfillment, application_id, _receipt, _conversation = prepared(service)
    barrier = Barrier(2)

    def run(actor, name):
        barrier.wait(timeout=5)
        try:
            return fulfillment.execute(actor, application_id, command(name, 1))
        except BusinessError as error:
            return error.code

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(run, SUPPORT, "approve"), pool.submit(run, CUSTOMER, "withdraw")]
        results = [future.result(timeout=15) for future in futures]
    successes = [result for result in results if isinstance(result, dict)]
    assert len(successes) == 1
    assert "VERSION_CONFLICT" in results
    state = persisted(service, application_id)
    assert state["application"]["version"] == 2
    assert len(state["commands"]) == 1
    assert len(state["audits"]) == 2
    if successes[0]["status"] == "WITHDRAWN":
        assert (state["inventory"]["available"], state["inventory"]["reserved"]) == (1, 0)
        assert state["line"]["occupied"] == 0
    else:
        assert successes[0]["status"] == "AWAITING_RETURN"
        assert (state["inventory"]["available"], state["inventory"]["reserved"]) == (0, 1)
        assert state["line"]["occupied"] == 1


def test_parallel_duplicate_command_returns_one_receipt_and_one_transition(service):
    fulfillment, application_id, _receipt, _conversation = prepared(service)
    barrier = Barrier(2)
    value = command("approve", 1, key="parallel-same-key")

    def run():
        barrier.wait(timeout=5)
        return fulfillment.execute(SUPPORT, application_id, value)

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(run), pool.submit(run)]
        results = [future.result(timeout=15) for future in futures]
    assert results[0] == results[1]
    state = persisted(service, application_id)
    assert state["application"]["version"] == 2
    assert len(state["commands"]) == 1
    assert len(state["audits"]) == 2


@pytest.mark.parametrize("target", ["reject", "request_shipment", "confirm_shipment"])
def test_transaction_failure_rolls_back_inventory_version_receipt_audit_and_fulfillment(
    service, target
):
    fulfillment, application_id, _receipt, _conversation = prepared(service)
    version = (
        1 if target == "reject" else advance(service, fulfillment, application_id, before=target)
    )
    if target == "reject":
        actor, data = SUPPORT, {}
    else:
        actor, _name, data, _status = next(step for step in FLOW if step[1] == target)
    value = command(target, version, key="rollback-command", data=data)
    state = persisted(service, application_id)
    with pytest.raises(RuntimeError):
        fulfillment.execute(actor, application_id, value, fault="before_commit")
    assert persisted(service, application_id) == state
    result = fulfillment.execute(actor, application_id, value)
    assert result["version"] == version + 1
    assert len(persisted(service, application_id)["commands"]) == len(state["commands"]) + 1


@pytest.mark.parametrize("target", ["reject", "confirm_shipment"])
def test_corrupt_reservation_totals_are_rejected_before_release_or_consumption(service, target):
    fulfillment, application_id, _receipt, _conversation = prepared(service)
    version = (
        1 if target == "reject" else advance(service, fulfillment, application_id, before=target)
    )
    with service.sessions.begin() as session:
        session.get(Inventory, "tee-white-L").reserved = 0
    actor = SUPPORT if target == "reject" else ADAPTER
    data = {} if target == "reject" else {"external_shipment_id": "SIM-SHIPMENT-001"}
    state = persisted(service, application_id)
    failure(
        "INVENTORY_INVARIANT",
        lambda: fulfillment.execute(actor, application_id, command(target, version, data=data)),
    )
    assert persisted(service, application_id) == state


@pytest.mark.parametrize(
    "model,key,field,value",
    [
        (Inventory, "tee-white-L", "available", -1),
        (Inventory, "tee-white-L", "reserved", -1),
        (OrderLine, "S1002-1", "occupied", 3),
        (Application, None, "version", 0),
        (Application, None, "status", "INVENTED_STATE"),
    ],
)
def test_postgresql_constraints_reject_invalid_inventory_quantity_and_application_state(
    service, model, key, field, value
):
    _fulfillment, application_id, _receipt, _conversation = prepared(service)
    state = persisted(service, application_id)
    with pytest.raises(IntegrityError):
        with service.sessions.begin() as session:
            setattr(session.get(model, key or application_id), field, value)
    assert persisted(service, application_id) == state


def test_later_chat_does_not_invalidate_an_already_submitted_fulfillment(service):
    fulfillment, application_id, receipt, conversation = prepared(service)
    state = persisted(service, application_id)
    service.append_message("alice", conversation, "Change my next request", "later-intent")
    assert persisted(service, application_id) == state
    result = fulfillment.execute(CUSTOMER, application_id, command("withdraw", 1))
    assert result["status"] == "WITHDRAWN"
    assert service.get_operation("alice", operation_id=receipt["operation_id"]) == receipt


def test_return_application_keeps_existing_flow_and_cannot_use_exchange_commands(service):
    _conversation, preview = application(service, kind="return")
    receipt = service.commit("alice", preview)
    fulfillment = FulfillmentService(service.sessions, clock=service.clock)
    failure(
        "UNSUPPORTED_APPLICATION",
        lambda: fulfillment.execute(SUPPORT, receipt["application_id"], command("approve", 1)),
    )
    with service.sessions() as session:
        assert session.get(Application, receipt["application_id"]).status == "RETURN_REQUESTED"
        assert session.get(ExchangeFulfillment, receipt["application_id"]) is None
        assert session.scalar(select(func.count()).select_from(FulfillmentCommand)) == 0
        assert session.scalar(select(func.count()).select_from(FulfillmentAudit)) == 0


def test_initial_commit_failure_does_not_leave_fulfillment_or_creation_audit(service):
    _conversation, preview = application(service)
    with pytest.raises(RuntimeError, match="INJECTED_TRANSACTION_FAILURE"):
        service.commit("alice", preview, fault="before_commit")
    with service.sessions() as session:
        for model in (Application, ExchangeFulfillment, FulfillmentAudit, FulfillmentCommand):
            assert session.scalar(select(func.count()).select_from(model)) == 0
        stock = session.get(Inventory, "tee-white-L")
        assert (stock.available, stock.reserved) == (1, 0)
        assert session.get(OrderLine, "S1002-1").occupied == 0
    receipt = service.commit("alice", preview)
    assert persisted(service, receipt["application_id"])["fulfillment"]["origin"] == "native"


def test_last_inventory_race_creates_only_the_winners_fulfillment_and_creation_audit(service):
    _conversation, first = application(service)
    _other_conversation, second = application(service, owner="bob", order="S2001")
    barrier = Barrier(2)

    def commit(owner, preview):
        barrier.wait(timeout=5)
        try:
            return service.commit(owner, preview)
        except BusinessError as error:
            return error.code

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(commit, "alice", first), pool.submit(commit, "bob", second)]
        results = [future.result(timeout=15) for future in futures]
    successes = [result for result in results if isinstance(result, dict)]
    assert len(successes) == 1
    assert "STATE_CHANGED" in results
    with service.sessions() as session:
        for model in (Application, ExchangeFulfillment, FulfillmentAudit):
            assert session.scalar(select(func.count()).select_from(model)) == 1
        stock = session.get(Inventory, "tee-white-L")
        assert (stock.available, stock.reserved) == (0, 1)
        assert session.get(ExchangeFulfillment, successes[0]["application_id"]) is not None


@pytest.mark.parametrize("decision", ["WITHDRAWN", "COMPLETED"])
def test_existing_demo_settlement_is_versioned_audited_and_does_not_invent_external_shipping(
    service, decision
):
    _fulfillment, application_id, _receipt, _conversation = prepared(service)
    first = settle_application(service.sessions, application_id, decision)
    state = persisted(service, application_id)
    assert (state["application"]["status"], state["application"]["version"]) == (decision, 2)
    assert len(state["audits"]) == 2
    assert state["audits"][-1]["to_state"] == decision
    assert state["audits"][-1]["to_version"] == 2
    assert state["fulfillment"].get("external_shipment_id") is None
    assert state["commands"] == []
    assert settle_application(service.sessions, application_id, decision) == first
    assert persisted(service, application_id) == state


def test_demo_helper_cannot_bypass_a_formal_fulfillment_in_progress(service):
    fulfillment, application_id, _receipt, _conversation = prepared(service)
    fulfillment.execute(SUPPORT, application_id, command("approve", 1))
    state = persisted(service, application_id)
    failure(
        "APPLICATION_CLOSED",
        lambda: settle_application(service.sessions, application_id, "COMPLETED"),
    )
    assert persisted(service, application_id) == state


@pytest.mark.parametrize(
    "before,actor,name,data",
    [
        ("receive", CUSTOMER, "withdraw", {}),
        ("submit_return", SUPPORT, "reject", {}),
        ("receive", WAREHOUSE, "request_shipment", {}),
        ("confirm_shipment", CUSTOMER, "complete", {}),
        (
            "request_shipment",
            ADAPTER,
            "confirm_shipment",
            {"external_shipment_id": "SIM-PREMATURE-SHIPMENT"},
        ),
    ],
)
def test_illegal_shortcuts_cannot_skip_return_receipt_or_confirmed_shipping(
    service, before, actor, name, data
):
    fulfillment, application_id, _receipt, _conversation = prepared(service)
    version = advance(service, fulfillment, application_id, before=before)
    state = persisted(service, application_id)
    failure(
        "ILLEGAL_TRANSITION",
        lambda: fulfillment.execute(
            actor, application_id, command(name, version, key="illegal-shortcut", data=data)
        ),
    )
    assert persisted(service, application_id) == state


def pending_shipment(service, *, owner, order):
    """Prepare an owned application through the real commands, without confirming dispatch."""
    _conversation, preview = application(service, owner=owner, order=order)
    receipt = service.commit(owner, preview)
    fulfillment = FulfillmentService(service.sessions, clock=service.clock)
    customer = Actor(actor_id=owner, role="customer", source="test_fixture")
    version = 1
    for actor, name, data, _status in FLOW:
        if name == "confirm_shipment":
            break
        actor = customer if actor.role == "customer" else actor
        version = fulfillment.execute(
            actor, receipt["application_id"], command(name, version, data=data)
        )["version"]
    return fulfillment, receipt["application_id"], receipt, version


def test_simulated_warehouse_requires_requested_shipping_and_confirms_one_deterministic_result(
    service,
):
    fulfillment, application_id, _receipt, _conversation = prepared(service)
    adapter = SimulatedWarehouse()
    initial = persisted(service, application_id)
    failure(
        "SHIPMENT_NOT_REQUESTED",
        lambda: adapter.confirm(
            fulfillment, application_id, expected_version=1, idempotency_key="adapter-confirm"
        ),
    )
    assert persisted(service, application_id) == initial
    version = advance(service, fulfillment, application_id, before="confirm_shipment")
    pending = persisted(service, application_id)
    assert pending["application"]["status"] == "REPLACEMENT_PENDING"
    assert pending["reservation"]["status"] == "RESERVED"
    first = adapter.confirm(
        fulfillment, application_id, expected_version=version, idempotency_key="adapter-confirm"
    )
    state = persisted(service, application_id)
    assert first["status"] == "REPLACEMENT_SHIPPED"
    expected_id = "SIM-" + sha256(f"exchange:{application_id}".encode()).hexdigest()[:24]
    assert state["fulfillment"]["external_shipment_id"] == expected_id
    assert state["reservation"]["status"] == "CONSUMED"
    assert (state["inventory"]["available"], state["inventory"]["reserved"]) == (0, 0)
    assert (
        adapter.confirm(
            fulfillment, application_id, expected_version=version, idempotency_key="adapter-confirm"
        )
        == first
    )
    assert persisted(service, application_id) == state
    completed = fulfillment.execute(CUSTOMER, application_id, command("complete", first["version"]))
    final = persisted(service, application_id)
    assert completed["status"] == "COMPLETED"
    assert final["inventory"] == state["inventory"]
    assert final["reservation"] == state["reservation"]


def test_external_shipment_reference_conflict_rolls_back_the_second_application(service):
    with service.sessions.begin() as session:
        session.get(Inventory, "tee-white-L").available = 2
    fulfillment, first_id, _first_receipt, first_version = pending_shipment(
        service, owner="alice", order="S1002"
    )
    _other_service, second_id, second_receipt, second_version = pending_shipment(
        service, owner="bob", order="S2001"
    )
    data = {"external_shipment_id": "SIM-SAME-EXTERNAL-REFERENCE"}
    fulfillment.execute(ADAPTER, first_id, command("confirm_shipment", first_version, data=data))
    first_state = persisted(service, first_id)
    second_state = persisted(service, second_id)
    failure(
        "SHIPMENT_CONFLICT",
        lambda: fulfillment.execute(
            ADAPTER, second_id, command("confirm_shipment", second_version, data=data)
        ),
    )
    assert persisted(service, first_id) == first_state
    assert persisted(service, second_id) == second_state
    assert second_state["application"]["status"] == "REPLACEMENT_PENDING"
    assert second_state["application"]["version"] == second_version
    assert second_state["reservation"]["status"] == "RESERVED"
    assert (second_state["inventory"]["available"], second_state["inventory"]["reserved"]) == (0, 1)
    assert (
        service.get_operation("bob", operation_id=second_receipt["operation_id"]) == second_receipt
    )


def test_concurrent_withdraw_and_dispatch_on_different_orders_preserve_shared_stock(service):
    with service.sessions.begin() as session:
        session.get(Inventory, "tee-white-L").available = 2
    fulfillment, alice_id, alice_receipt, _conversation = prepared(service)
    _other_service, bob_id, bob_receipt, bob_version = pending_shipment(
        service, owner="bob", order="S2001"
    )
    barrier = Barrier(2)

    def withdraw():
        barrier.wait(timeout=5)
        return fulfillment.execute(CUSTOMER, alice_id, command("withdraw", 1))

    def confirm_dispatch():
        barrier.wait(timeout=5)
        return fulfillment.execute(
            ADAPTER,
            bob_id,
            command(
                "confirm_shipment",
                bob_version,
                key="concurrent-dispatch",
                data={"external_shipment_id": "SIM-CONCURRENT-BOB"},
            ),
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(withdraw), pool.submit(confirm_dispatch)]
        results = [future.result(timeout=15) for future in futures]
    assert {result["status"] for result in results} == {"WITHDRAWN", "REPLACEMENT_SHIPPED"}
    alice = persisted(service, alice_id)
    bob = persisted(service, bob_id)
    assert (alice["inventory"]["available"], alice["inventory"]["reserved"]) == (1, 0)
    assert bob["inventory"] == alice["inventory"]
    assert (alice["line"]["occupied"], alice["reservation"]["status"]) == (0, "RELEASED")
    assert (bob["line"]["occupied"], bob["reservation"]["status"]) == (1, "CONSUMED")
    assert (alice["application"]["version"], bob["application"]["version"]) == (2, 6)
    assert (
        service.get_operation("alice", operation_id=alice_receipt["operation_id"]) == alice_receipt
    )
    assert service.get_operation("bob", operation_id=bob_receipt["operation_id"]) == bob_receipt
