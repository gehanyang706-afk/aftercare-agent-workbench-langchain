"""Real PG Worker contention preserves one effect and same-stock invariants.

These service/Worker concurrency tests use a deterministic transport stub; D3's
separate process smoke covers real HTTP, SIGKILL and natural lease expiry.
"""

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from threading import Barrier, Lock

import pytest
from sqlalchemy import func, select
from test_fulfillment import CUSTOMER, FLOW, WAREHOUSE, command
from test_outbox import enqueue
from test_outbox import pending as pending

from aftercare.domain.contracts import ActionInput
from aftercare.repositories.models import (
    Application,
    FulfillmentAudit,
    Inventory,
    Operation,
    Order,
    OrderLine,
    OutboxAttempt,
    ReconciliationTask,
    Reservation,
    ShipmentRequest,
    Variant,
)
from aftercare.services.fulfillment import FulfillmentService
from aftercare.services.outbox import OutboxService
from aftercare.services.recovery import RecoveryService
from aftercare.services.warehouse_http import DeliveryResult
from aftercare.workers.outbox_worker import OutboxWorker
from aftercare.workers.reconciliation_worker import ReconciliationWorker

pytestmark = pytest.mark.integration


def run_together(workers):
    gate = Barrier(len(workers))

    def once(worker):
        gate.wait(timeout=5)
        return worker.run_once()

    with ThreadPoolExecutor(max_workers=len(workers)) as pool:
        futures = [pool.submit(once, worker) for worker in workers]
        return [future.result(timeout=10) for future in futures]


def assert_single_result(results):
    assert sum(row.get("request", {}).get("status") == "SUCCEEDED" for row in results) == 1
    assert sum(row.get("status") == "IDLE" for row in results) == len(results) - 1


@pytest.mark.parametrize("worker_count", [1, 2, 4])
def test_outbox_workers_contend_for_one_request_without_duplicate_delivery(pending, worker_count):
    p = pending
    enqueue(p)
    calls, lock = [], Lock()

    class Client:
        def deliver(self, payload, request_hash):
            with lock:
                calls.append((payload, request_hash))
            return DeliveryResult("SUCCEEDED", external_shipment_id="ONE-WAREHOUSE-EFFECT")

    workers = [
        OutboxWorker(p.outbox, Client(), owner=f"contention-{i}") for i in range(worker_count)
    ]
    assert_single_result(run_together(workers))
    assert len(calls) == 1
    with p.service.sessions() as session:
        request = session.scalar(select(ShipmentRequest))
        attempt = session.scalar(select(OutboxAttempt))
        assert (
            request.status == "SUCCEEDED" and request.triggered_by["actor_id"] == WAREHOUSE.actor_id
        )
        assert attempt.outcome == "SUCCEEDED" and attempt.send_state == "STARTED"
        assert session.scalar(select(func.count()).select_from(OutboxAttempt)) == 1
        assert session.get(Application, p.appid).version == 6
        assert session.get(Reservation, p.appid).status == "CONSUMED"
        assert session.get(Inventory, "tee-white-L").reserved == 0
        assert session.get(Operation, p.original["operation_id"]).receipt == p.original
        audits = list(
            session.scalars(
                select(FulfillmentAudit).where(
                    FulfillmentAudit.application_id == p.appid,
                    FulfillmentAudit.command == "confirm_shipment",
                )
            )
        )
        assert len(audits) == 1 and audits[0].details["triggered_by"] == request.triggered_by


def independent_pending(service, number):
    fulfillment = FulfillmentService(service.sessions, service.clock, async_delivery=True)
    with service.sessions.begin() as session:
        target = session.get(Inventory, "tee-white-L")
        target.available += number
        initial_available = target.available
        source = session.get(Variant, "tee-white-M")
        for i in range(number):
            order = Order(
                id=f"same-stock-order-{i}",
                customer_id="alice",
                status="DELIVERED",
                paid_fen=source.price_fen,
                delivered_at=service.clock() - timedelta(days=2),
                version=1,
                policy_version="v1",
            )
            session.add(order)
            session.flush()
            session.add(
                OrderLine(
                    id=f"same-stock-line-{i}",
                    order_id=order.id,
                    variant_id=source.id,
                    quantity=1,
                    occupied=0,
                    unused=True,
                )
            )
    cases = []
    for i in range(number):
        conversation = service.create_conversation("alice")
        preview = service.preview(
            "alice",
            conversation,
            ActionInput(
                action="exchange",
                order_id=f"same-stock-order-{i}",
                line_id=f"same-stock-line-{i}",
                quantity=1,
                target_variant="tee-white-L",
            ),
        )
        service.confirm("alice", preview["id"], True)
        receipt = service.commit("alice", preview["id"])
        appid = receipt["application_id"]
        for version, (actor, name, data, _) in enumerate(FLOW[:3], 1):
            fulfillment.execute(actor, appid, command(name, version, data=data))
        fulfillment.execute(WAREHOUSE, appid, command("request_shipment", 4))
        cases.append((appid, receipt))
    return cases, initial_available


def test_four_workers_complete_distinct_requests_sharing_stock_without_quantity_drift(service):
    cases, initial_available = independent_pending(service, 4)
    calls, lock = [], Lock()

    class Client:
        def deliver(self, payload, request_hash):
            with lock:
                calls.append(payload["effect_key"])
            return DeliveryResult(
                "SUCCEEDED", external_shipment_id="effect-" + payload["application_id"]
            )

    workers = [
        OutboxWorker(
            OutboxService(service.sessions, service.clock), Client(), owner=f"shared-stock-{i}"
        )
        for i in range(4)
    ]
    results = run_together(workers)
    assert all(row.get("request", {}).get("status") == "SUCCEEDED" for row in results)
    assert len(calls) == len(set(calls)) == 4
    with service.sessions() as session:
        stock = session.get(Inventory, "tee-white-L")
        assert stock.available == initial_available - 4 and stock.reserved == 0
        assert session.scalar(select(func.count()).select_from(OutboxAttempt)) == 4
        assert (
            session.scalar(
                select(func.sum(Reservation.quantity)).where(Reservation.status == "CONSUMED")
            )
            == 4
        )
        for appid, original in cases:
            assert session.get(Application, appid).version == 6
            assert session.get(Operation, original["operation_id"]).receipt == original
            assert (
                session.scalar(
                    select(func.count())
                    .select_from(FulfillmentAudit)
                    .where(
                        FulfillmentAudit.application_id == appid,
                        FulfillmentAudit.command == "confirm_shipment",
                    )
                )
                == 1
            )
            assert session.get(OrderLine, session.get(Application, appid).line_id).occupied == 1


@pytest.mark.parametrize("worker_count", [1, 2, 4])
def test_query_workers_contend_for_one_unknown_without_reposting_or_reopening_budget(
    pending, worker_count
):
    p = pending
    enqueue(p)
    first = p.outbox.claim("original-http")
    p.outbox.begin_delivery(first)
    p.outbox.finish(first, "UNKNOWN", error_code="RESPONSE_LOST")
    recovery = RecoveryService(p.service.sessions, p.clock)
    calls, lock = [], Lock()

    class Client:
        def query(self, effect_key, application_id, request_hash):
            with lock:
                calls.append((effect_key, application_id, request_hash))
            return DeliveryResult(
                "SUCCEEDED", external_shipment_id="QUERY-ONE-EFFECT", provider_sequence=2
            )

        def deliver(self, *args, **kwargs):
            pytest.fail("UNKNOWN reconciliation must not call POST")

    workers = [
        ReconciliationWorker(recovery, Client(), outbox=p.outbox, owner=f"query-contender-{i}")
        for i in range(worker_count)
    ]
    assert_single_result(run_together(workers))
    assert len(calls) == 1
    assert recovery.ensure_pending() == 0
    with p.service.sessions() as session:
        task = session.scalar(select(ReconciliationTask))
        assert task.status == "DONE" and task.attempts == task.cycle_attempts == 1
        assert task.cycle == 1 and task.owner is None and task.lease_expires_at is None
        assert session.scalar(select(func.count()).select_from(OutboxAttempt)) == 1
        assert session.scalar(select(OutboxAttempt)).outcome == "UNKNOWN"
        assert session.get(Application, p.appid).version == 6
        assert session.get(Reservation, p.appid).status == "CONSUMED"
        assert session.get(Inventory, "tee-white-L").reserved == 0
        assert session.get(Operation, p.original["operation_id"]).receipt == p.original
    assert p.fulfillment.get(CUSTOMER, p.appid)["version"] == 6
