"""C1 transactional delivery contracts; transport tests exercise the separate HTTP provider."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import timedelta
from threading import Barrier
from types import SimpleNamespace

import pytest
from sqlalchemy import MetaData, func, select
from sqlalchemy.exc import IntegrityError
from test_fulfillment import CUSTOMER, FLOW, WAREHOUSE, command, prepared
from test_fulfillment_migration import legacy_schema as legacy_schema
from test_fulfillment_migration import migration, snapshot_rows, upgrade

from aftercare.domain.contracts import BusinessError
from aftercare.domain.fulfillment import Actor
from aftercare.repositories.models import (
    Application,
    ExchangeFulfillment,
    FulfillmentAudit,
    Inventory,
    Operation,
    OutboxAttempt,
    Reservation,
    ShipmentRequest,
    WarehouseOutbox,
)
from aftercare.services.business import columns, digest
from aftercare.services.fulfillment import FulfillmentService
from aftercare.services.outbox import OutboxService
from aftercare.services.simulated_warehouse import SimulatedWarehouse

pytestmark = pytest.mark.integration


class Clock:
    def __init__(self, now):
        self.now = now

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += timedelta(seconds=seconds)


@pytest.fixture
def pending(service):
    _old, appid, original, _conv = prepared(service)
    clock = Clock(service.clock())
    fulfillment = FulfillmentService(service.sessions, clock, async_delivery=True)
    version = 1
    for actor, name, data, _status in FLOW[:3]:
        version = fulfillment.execute(actor, appid, command(name, version, data=data))["version"]
    return SimpleNamespace(
        service=service,
        appid=appid,
        original=original,
        clock=clock,
        fulfillment=fulfillment,
        outbox=OutboxService(service.sessions, clock),
    )


def enqueue(p):
    receipt = p.fulfillment.execute(WAREHOUSE, p.appid, command("request_shipment", 4))
    return receipt, p.fulfillment.get(CUSTOMER, p.appid)["shipment_request"]["id"]


def started_claim(outbox, owner):
    claim = outbox.claim(owner)
    outbox.begin_delivery(claim)
    return claim


def fails(code, call):
    with pytest.raises(BusinessError) as error:
        call()
    assert error.value.code == code


def state(p):
    with p.service.sessions() as s:
        return {
            "app": columns(s.get(Application, p.appid)),
            "fulfillment": columns(s.get(ExchangeFulfillment, p.appid)),
            "stock": columns(s.get(Inventory, "tee-white-L")),
            "reservation": columns(s.get(Reservation, p.appid)),
            "original": s.get(Operation, p.original["operation_id"]).receipt,
            "requests": [columns(x) for x in s.scalars(select(ShipmentRequest))],
            "outbox": [columns(x) for x in s.scalars(select(WarehouseOutbox))],
            "attempts": [columns(x) for x in s.scalars(select(OutboxAttempt))],
        }


def test_enqueue_is_atomic_and_precise_command_replay_does_not_add_events(pending):
    p = pending
    before = state(p)
    with pytest.raises(RuntimeError, match="INJECTED_FULFILLMENT_FAILURE"):
        p.fulfillment.execute(
            WAREHOUSE, p.appid, command("request_shipment", 4), fault="before_commit"
        )
    assert state(p) == before
    receipt, _request_id = enqueue(p)
    after = state(p)
    assert p.fulfillment.execute(WAREHOUSE, p.appid, command("request_shipment", 4)) == receipt
    assert state(p) == after
    assert len(after["requests"]) == len(after["outbox"]) == 1
    request, event = after["requests"][0], after["outbox"][0]
    assert (
        request["payload"]
        == event["payload"]
        == {
            "effect_key": "exchange:" + p.appid,
            "application_id": p.appid,
            "variant_id": "tee-white-L",
            "quantity": 1,
        }
    )
    assert request["request_hash"] == digest(request["payload"])
    assert request["triggered_by"] == {
        "actor_id": WAREHOUSE.actor_id,
        "role": "warehouse",
        "source": "test_fixture",
    }
    assert after["app"]["version"] == 5 and after["stock"]["reserved"] == 1


def test_simulation_cannot_bypass_existing_async_request_even_when_mode_changes(pending):
    p = pending
    enqueue(p)
    before = state(p)
    in_process = FulfillmentService(p.service.sessions, p.clock)
    fails(
        "ASYNC_DELIVERY_REQUIRED",
        lambda: SimulatedWarehouse().confirm(
            in_process, p.appid, expected_version=5, idempotency_key="bypass"
        ),
    )
    assert state(p) == before


def test_default_mode_keeps_in_process_contract_without_enqueue(pending):
    p = pending
    in_process = FulfillmentService(p.service.sessions, p.clock)
    in_process.execute(WAREHOUSE, p.appid, command("request_shipment", 4))
    assert state(p)["requests"] == [] and state(p)["outbox"] == []
    result = SimulatedWarehouse().confirm(
        in_process, p.appid, expected_version=5, idempotency_key="sim"
    )
    assert result["status"] == "REPLACEMENT_SHIPPED"


def test_concurrent_enqueue_replays_one_command_and_one_event(pending):
    p = pending
    barrier = Barrier(2)

    def call():
        barrier.wait(timeout=5)
        return p.fulfillment.execute(WAREHOUSE, p.appid, command("request_shipment", 4))

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: call(), range(2)))
    assert results[0] == results[1]
    assert len(state(p)["requests"]) == len(state(p)["outbox"]) == 1


def test_concurrent_claims_take_one_event_once(pending):
    p = pending
    enqueue(p)
    barrier = Barrier(2)

    def call(owner):
        barrier.wait(timeout=5)
        return p.outbox.claim(owner)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(call, ["one", "two"]))
    claimed = [r for r in results if r is not None]
    assert len(claimed) == 1 and claimed[0].generation == claimed[0].attempts == 1
    assert p.outbox.claim("third") is None
    assert len(state(p)["attempts"]) == 1


def test_success_consumes_stock_once_and_keeps_original_receipt_and_triggered_by(pending):
    p = pending
    _receipt, request_id = enqueue(p)
    claim = p.outbox.claim("one")
    assert p.outbox.check_claim(claim) is None
    p.outbox.begin_delivery(claim)
    result = p.outbox.finish(claim, "SUCCEEDED", external_shipment_id="HTTP-SHIPMENT-1")
    assert result["request"]["status"] == "SUCCEEDED" and result["outbox"]["status"] == "DELIVERED"
    after = state(p)
    assert after["app"]["status"] == "REPLACEMENT_SHIPPED" and after["app"]["version"] == 6
    assert after["stock"]["reserved"] == 0 and after["stock"]["available"] == 0
    assert after["reservation"]["status"] == "CONSUMED" and after["original"] == p.original
    with p.service.sessions() as s:
        audit = s.scalar(
            select(FulfillmentAudit).where(
                FulfillmentAudit.application_id == p.appid, FulfillmentAudit.to_version == 6
            )
        )
        assert audit.actor_id == "warehouse-outbox" and audit.actor_source == "trusted_adapter"
        assert audit.details["shipment_request_id"] == request_id
        assert audit.details["triggered_by"]["actor_id"] == WAREHOUSE.actor_id
    fails(
        "OUTBOX_LEASE_LOST",
        lambda: p.outbox.finish(claim, "SUCCEEDED", external_shipment_id="HTTP-SHIPMENT-1"),
    )
    assert state(p) == after and p.outbox.claim("two") is None
    assert p.fulfillment.execute(CUSTOMER, p.appid, command("complete", 6))["version"] == 7


@pytest.mark.parametrize("change", [{"owner": "other"}, {"generation": 2}, {"request_id": "wrong"}])
def test_forged_or_stale_claim_cannot_modify_business(pending, change):
    p = pending
    enqueue(p)
    claim = p.outbox.claim("one")
    before = state(p)
    bad = replace(claim, **change)
    fails("OUTBOX_LEASE_LOST", lambda: p.outbox.check_claim(bad))
    fails(
        "OUTBOX_LEASE_LOST", lambda: p.outbox.finish(bad, "SUCCEEDED", external_shipment_id="stale")
    )
    assert state(p) == before


def test_expired_claim_becomes_unknown_and_is_never_automatically_reposted(pending):
    p = pending
    _receipt, request_id = enqueue(p)
    claim = started_claim(p.outbox, "one")
    p.clock.advance(15)
    fails("OUTBOX_LEASE_LOST", lambda: p.outbox.check_claim(claim))
    fails(
        "OUTBOX_LEASE_LOST",
        lambda: p.outbox.finish(claim, "SUCCEEDED", external_shipment_id="late"),
    )
    assert p.outbox.expire_leases() == 1 and p.outbox.expire_leases() == 0
    result = p.outbox.get(CUSTOMER, request_id)
    assert result["request"]["status"] == "UNKNOWN" and result["outbox"]["status"] == "BLOCKED"
    assert result["attempts"][0]["error_code"] == "LEASE_EXPIRED"
    assert state(p)["fulfillment"]["shipment_status"] == "UNKNOWN"
    assert state(p)["stock"]["reserved"] == 1 and p.outbox.claim("new") is None


@pytest.mark.parametrize(
    "outcome,request_status,outbox_status",
    [("UNKNOWN", "UNKNOWN", "BLOCKED"), ("FAILED", "FAILED", "DEAD")],
)
def test_uncertain_and_permanent_outcomes_preserve_stock_and_stop_claiming(
    pending, outcome, request_status, outbox_status
):
    p = pending
    enqueue(p)
    result = p.outbox.finish(started_claim(p.outbox, "one"), outcome, error_code="PROVIDER_TEST")
    assert (
        result["request"]["status"] == request_status
        and result["outbox"]["status"] == outbox_status
    )
    assert state(p)["stock"]["reserved"] == 1 and state(p)["app"]["version"] == 5
    p.clock.advance(100)
    assert p.outbox.claim("next") is None


def test_proven_not_accepted_retry_is_delayed_bounded_and_stale_generation_is_fenced(pending):
    p = pending
    enqueue(p)
    claims = []
    for attempt in range(1, 4):
        claim = started_claim(p.outbox, "one")
        assert claim.attempts == claim.generation == attempt
        claims.append(claim)
        result = p.outbox.finish(claim, "RETRYABLE", error_code="NOT_ACCEPTED_BUSY")
        assert p.outbox.claim("early") is None
        if attempt < 3:
            assert result["outbox"]["status"] == "PENDING"
            p.clock.advance(2 ** (attempt - 1))
        else:
            assert (
                result["outbox"]["status"] == "DEAD"
                and result["request"]["status"] == "MANUAL_REVIEW"
            )
    assert len(state(p)["attempts"]) == 3 and state(p)["stock"]["reserved"] == 1
    fails(
        "OUTBOX_LEASE_LOST",
        lambda: p.outbox.finish(claims[0], "SUCCEEDED", external_shipment_id="stale"),
    )


def test_payload_snapshots_are_immutable_and_bad_insert_is_quarantined(pending):
    p = pending
    enqueue(p)
    for model, attribute in (
        (ShipmentRequest, "payload"),
        (WarehouseOutbox, "payload"),
        (ShipmentRequest, "request_hash"),
    ):
        with pytest.raises(IntegrityError):
            with p.service.sessions.begin() as s:
                row = s.scalar(select(model))
                setattr(row, attribute, {"tamper": True} if attribute == "payload" else "0" * 64)
    with p.service.sessions.begin() as s:
        old = s.scalar(select(WarehouseOutbox))
        request_id, now = old.request_id, p.clock()
        s.delete(old)
        s.flush()
        s.add(
            WarehouseOutbox(
                id="outbox_bad_fixture",
                request_id=request_id,
                payload={"bad": True},
                next_attempt_at=now,
                created_at=now,
                updated_at=now,
            )
        )
    assert p.outbox.claim("one") is None
    assert state(p)["outbox"][0]["status"] == "DEAD"
    assert state(p)["requests"][0]["status"] == "MANUAL_REVIEW"
    assert state(p)["attempts"] == []


def test_claim_payload_mutation_is_blocked_before_network_and_not_requeued(pending):
    p = pending
    enqueue(p)
    claim = p.outbox.claim("one")
    claim.payload["quantity"] = 9
    fails("OUTBOX_LEASE_LOST", lambda: p.outbox.check_claim(claim))
    assert state(p)["outbox"][0]["status"] == "DEAD"
    assert state(p)["requests"][0]["status"] == "MANUAL_REVIEW"


def test_get_is_authorized_and_omits_internal_payload_lease_owner_and_raw_errors(pending):
    p = pending
    _receipt, request_id = enqueue(p)
    p.outbox.claim("private-owner")
    result = p.outbox.get(CUSTOMER, request_id)
    assert set(result) == {"request", "outbox", "attempts"}
    assert (
        not {"payload", "lease_owner", "lease_expires_at", "generation"} & result["outbox"].keys()
    )
    assert "payload" not in result["request"] and "owner" not in result["attempts"][0]
    fails("NOT_FOUND", lambda: p.outbox.get(Actor("bob", "customer", "test_fixture"), request_id))


def test_0009_preserves_existing_0008_business_and_does_not_enqueue_legacy_rows(legacy_schema):
    connection, _old = legacy_schema
    upgrade(connection, "0007_exchange_fulfillment")
    upgrade(connection, "0008_staff_auth")
    metadata = MetaData()
    metadata.reflect(bind=connection)
    before = snapshot_rows(connection, metadata.tables)
    upgrade(connection, "0009_warehouse_outbox")
    assert snapshot_rows(connection, metadata.tables) == before
    metadata = MetaData()
    metadata.reflect(bind=connection)
    for name in ("shipment_requests", "warehouse_outbox", "outbox_attempts"):
        assert connection.scalar(select(func.count()).select_from(metadata.tables[name])) == 0
    with pytest.raises(RuntimeError, match="WAREHOUSE_OUTBOX_DOWNGRADE_FORBIDDEN"):
        migration("0009_warehouse_outbox").downgrade()
