"""C2 durable Inbox, bounded GET recovery and deliberate manual delivery cycles."""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import timedelta
from threading import Barrier
from types import SimpleNamespace

import pytest
from sqlalchemy import MetaData, func, select
from test_fulfillment import CUSTOMER, FLOW, SUPPORT, WAREHOUSE, command, prepared
from test_fulfillment_migration import NOW, migration, snapshot_rows, upgrade
from test_fulfillment_migration import legacy_schema as legacy_schema
from test_outbox import enqueue, fails, started_claim, state
from test_outbox import pending as pending

from aftercare.domain.contracts import BusinessError
from aftercare.domain.fulfillment import Actor
from aftercare.domain.warehouse import PROVIDER_ID
from aftercare.repositories.models import (
    ExchangeFulfillment,
    FulfillmentAudit,
    InboxConflict,
    Inventory,
    OrderLine,
    ReconciliationTask,
    RecoveryCommand,
    ShipmentRequest,
    WarehouseInbox,
    WarehouseOutbox,
)
from aftercare.services import fulfillment as fulfillment_module
from aftercare.services import recovery as recovery_module
from aftercare.services.business import columns, digest
from aftercare.services.recovery import RecoveryService

pytestmark = pytest.mark.integration


def recovery(p, **limits):
    return RecoveryService(p.service.sessions, p.clock, **limits)


def event(p, sequence=1, *, event_id=None, **changes):
    request = next(row for row in state(p)["requests"] if row["application_id"] == p.appid)
    result = {
        "event_id": event_id or f"event-{sequence}",
        "effect_key": request["effect_key"],
        "application_id": p.appid,
        "request_hash": request["request_hash"],
        "external_shipment_id": "HTTP-SHIPMENT-1",
        "sequence": sequence,
        "status": "ACCEPTED" if sequence == 1 else "SUCCEEDED",
    }
    return {**result, **changes}


def unknown(p, **limits):
    _, request_id = enqueue(p)
    p.outbox.finish(started_claim(p.outbox, "delivery"), "UNKNOWN", error_code="RESPONSE_LOST")
    service = recovery(p, **limits)
    service.ensure_pending()
    return service, request_id


def records(p, model):
    with p.service.sessions() as session:
        return [columns(row) for row in session.scalars(select(model))]


def second_application(p):
    with p.service.sessions.begin() as session:
        session.get(Inventory, "tee-white-L").available += 1
    _, application_id, original, _ = prepared(p.service)
    result = SimpleNamespace(**{**vars(p), "appid": application_id, "original": original})
    version = 1
    for actor, name, data, _ in FLOW[:3]:
        version = result.fulfillment.execute(
            actor, application_id, command(name, version, data=data)
        )["version"]
    enqueue(result)
    return result


def persist_before_processing(p, payload):
    with p.service.sessions.begin() as session:
        session.add(
            WarehouseInbox(
                id="inbox-" + payload["event_id"],
                source=PROVIDER_ID,
                event_id=payload["event_id"],
                payload=payload,
                body_hash=digest(payload),
                sequence=payload["sequence"],
                effect_key=payload["effect_key"],
                application_id=payload["application_id"],
                external_shipment_id=payload["external_shipment_id"],
                request_hash=payload["request_hash"],
                status="RECEIVED",
                created_at=p.clock(),
                updated_at=p.clock(),
            )
        )


def test_sequence_gap_is_durable_and_late_acceptance_drains_success_once(pending):
    p = pending
    enqueue(p)
    service = recovery(p)
    assert service.receive_event(event(p, 2))["status"] == "PENDING"
    assert state(p)["stock"]["reserved"] == 1 and state(p)["app"]["version"] == 5
    assert records(p, ReconciliationTask)[0]["status"] == "PENDING"
    assert service.receive_event(event(p, 1))["status"] == "PROCESSED"
    after = state(p)
    assert after["requests"][0]["last_sequence"] == 2
    assert after["requests"][0]["provider_external_id"] == "HTTP-SHIPMENT-1"
    assert after["app"]["version"] == 6 and after["app"]["status"] == "REPLACEMENT_SHIPPED"
    assert after["reservation"]["status"] == "CONSUMED" and after["stock"]["reserved"] == 0
    assert after["original"] == p.original
    assert {row["status"] for row in records(p, WarehouseInbox)} == {"PROCESSED"}
    assert records(p, ReconciliationTask)[0]["status"] == "DONE"
    duplicate = service.receive_event(event(p, 2))
    assert duplicate["duplicate"] is True and duplicate["status"] == "PROCESSED"
    assert service.receive_event(event(p, 1, event_id="late-acceptance"))["status"] == "STALE"
    assert state(p) == after


def test_same_event_id_changed_body_retains_original_and_deduplicates_conflict_evidence(pending):
    p = pending
    enqueue(p)
    service = recovery(p)
    original = event(p)
    service.receive_event(original)
    changed = {**original, "external_shipment_id": "DIFFERENT"}
    for _ in range(2):
        result = service.receive_event(changed)
        assert result["status"] == "CONFLICT" and result["error_code"] == "EVENT_ID_CONFLICT"
    inbox, conflicts = records(p, WarehouseInbox), records(p, InboxConflict)
    assert len(inbox) == len(conflicts) == 1
    assert inbox[0]["payload"] == original and inbox[0]["status"] == "PROCESSED"
    assert conflicts[0]["original_hash"] == digest(original)
    assert conflicts[0]["new_hash"] == digest(changed) and conflicts[0]["payload"] == changed
    assert state(p)["app"]["version"] == 5 and state(p)["stock"]["reserved"] == 1


@pytest.mark.parametrize(
    "changes,status,code",
    [
        (
            {"effect_key": "exchange:unknown", "application_id": "unknown"},
            "PENDING",
            "UNKNOWN_REQUEST",
        ),
        ({"application_id": "different-application"}, "CONFLICT", "EVENT_BINDING_CONFLICT"),
        ({"request_hash": "0" * 64}, "CONFLICT", "EVENT_BINDING_CONFLICT"),
    ],
)
def test_unknown_and_false_binding_events_are_preserved_without_consuming_stock(
    pending, changes, status, code
):
    p = pending
    enqueue(p)
    result = recovery(p).receive_event(event(p, **changes))
    assert result["status"] == status and result["error_code"] == code
    assert records(p, WarehouseInbox)[0]["payload"] == event(p, **changes)
    assert state(p)["app"]["version"] == 5 and state(p)["stock"]["reserved"] == 1
    if status == "CONFLICT":
        assert records(p, ReconciliationTask)[0]["status"] == "PENDING"


def test_accepted_binding_and_same_sequence_content_cannot_be_replaced(pending):
    p = pending
    enqueue(p)
    service = recovery(p)
    assert service.receive_event(event(p))["status"] == "PROCESSED"
    different = service.receive_event(
        event(p, event_id="same-sequence-other-id", external_shipment_id="OTHER")
    )
    assert different["status"] == "CONFLICT"
    assert different["error_code"] == "EVENT_EXTERNAL_CONFLICT"
    semantic = service.receive_event(
        event(p, event_id="same-sequence-other-status", status="SUCCEEDED")
    )
    assert semantic["status"] == "CONFLICT" and semantic["error_code"] == "EVENT_SEQUENCE_CONFLICT"
    assert state(p)["requests"][0]["provider_external_id"] == "HTTP-SHIPMENT-1"
    assert state(p)["stock"]["reserved"] == 1


def test_callback_fences_inflight_delivery_and_closes_its_attempt(pending):
    p = pending
    enqueue(p)
    delivery = started_claim(p.outbox, "delivery-paused")
    service = recovery(p)
    service.receive_event(event(p, 1))
    service.receive_event(event(p, 2))
    after = state(p)
    assert after["outbox"][0]["status"] == "DELIVERED"
    assert after["outbox"][0]["generation"] == delivery.generation + 1
    assert after["attempts"][0]["outcome"] == "SUCCEEDED"
    assert after["attempts"][0]["finished_at"] is not None
    fails("OUTBOX_LEASE_LOST", lambda: p.outbox.check_claim(delivery))
    fails(
        "OUTBOX_LEASE_LOST",
        lambda: p.outbox.finish(delivery, "SUCCEEDED", external_shipment_id="HTTP-SHIPMENT-1"),
    )
    assert state(p) == after


def test_processing_failure_rolls_back_domain_savepoint_but_keeps_received_inbox(
    pending, monkeypatch
):
    p = pending
    enqueue(p)
    service = recovery(p)
    service.receive_event(event(p, 1))
    original = recovery_module.merge_success

    def fail_after_domain(*args, **kwargs):
        original(*args, **kwargs)
        raise RuntimeError("INJECTED_RECOVERY_FAILURE")

    monkeypatch.setattr(recovery_module, "merge_success", fail_after_domain)
    with pytest.raises(RuntimeError, match="INJECTED_RECOVERY_FAILURE"):
        service.receive_event(event(p, 2))
    inbox = {row["event_id"]: row for row in records(p, WarehouseInbox)}
    assert inbox["event-2"]["status"] == "RECEIVED"
    assert state(p)["app"]["version"] == 5 and state(p)["stock"]["reserved"] == 1
    assert state(p)["original"] == p.original
    monkeypatch.setattr(recovery_module, "merge_success", original)
    service.ensure_pending()
    assert state(p)["app"]["version"] == 6 and state(p)["stock"]["reserved"] == 0


def test_concurrent_identical_callbacks_persist_one_inbox_and_one_business_effect(pending):
    p = pending
    enqueue(p)
    service = recovery(p)
    service.receive_event(event(p, 1))
    payload = event(p, 2)
    barrier = Barrier(2)

    def receive():
        barrier.wait(timeout=5)
        return service.receive_event(payload)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: receive(), range(2)))
    assert {item["duplicate"] for item in results} == {False, True}
    assert {item["status"] for item in results} == {"PROCESSED"}
    assert len(records(p, WarehouseInbox)) == 2 and records(p, InboxConflict) == []
    assert state(p)["app"]["version"] == 6 and state(p)["stock"]["reserved"] == 0


def test_query_success_skips_gap_preserves_conflicts_and_writes_provenance(pending):
    p = pending
    service, request_id = unknown(p)
    service.receive_event(event(p, 2))
    service.receive_event(event(p, 2, event_id="wrong-binding", request_hash="0" * 64))
    query = service.claim("query")
    result = service.finish(
        query, "SUCCEEDED", external_shipment_id="HTTP-SHIPMENT-1", provider_sequence=2
    )
    assert result["reconciliation"]["status"] == "DONE"
    assert result["request"]["status"] == "SUCCEEDED" and result["receipt"]["version"] == 6
    inbox = {row["event_id"]: row for row in records(p, WarehouseInbox)}
    assert inbox["event-2"]["status"] == "PROCESSED"
    assert inbox["wrong-binding"]["status"] == "CONFLICT"
    assert state(p)["stock"]["reserved"] == 0 and state(p)["original"] == p.original
    with p.service.sessions() as session:
        audit = session.scalar(select(FulfillmentAudit).where(FulfillmentAudit.to_version == 6))
        assert audit.details["triggered_by"]["actor_id"] == WAREHOUSE.actor_id
        assert audit.details["shipment_request_id"] == request_id
        assert audit.details["recovery"]["source"] == "query"
        assert audit.details["recovery"]["task_id"] == query.task_id
    fails(
        "RECONCILIATION_LEASE_LOST",
        lambda: service.finish(
            query, "SUCCEEDED", external_shipment_id="HTTP-SHIPMENT-1", provider_sequence=2
        ),
    )


def test_callback_success_revokes_inflight_query_generation(pending):
    p = pending
    service, _ = unknown(p)
    query = service.claim("query-paused")
    service.receive_event(event(p, 1))
    service.receive_event(event(p, 2))
    before = state(p)
    fails("RECONCILIATION_LEASE_LOST", lambda: service.check_claim(query))
    fails(
        "RECONCILIATION_LEASE_LOST",
        lambda: service.finish(
            query, "SUCCEEDED", external_shipment_id="HTTP-SHIPMENT-1", provider_sequence=2
        ),
    )
    assert state(p) == before and records(p, ReconciliationTask)[0]["status"] == "DONE"


def test_callback_and_query_race_merge_one_stock_consumption_and_one_domain_audit(pending):
    p = pending
    service, _ = unknown(p)
    service.receive_event(event(p, 1))
    query = service.claim("query")
    barrier = Barrier(2)

    def callback():
        barrier.wait(timeout=5)
        return service.receive_event(event(p, 2))

    def finish_query():
        barrier.wait(timeout=5)
        try:
            return service.finish(
                query, "SUCCEEDED", external_shipment_id="HTTP-SHIPMENT-1", provider_sequence=2
            )
        except BusinessError as error:
            assert error.code == "RECONCILIATION_LEASE_LOST"
            return error.code

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(callback), pool.submit(finish_query)]
        results = [future.result(timeout=10) for future in futures]
    assert results[0]["status"] in {"PROCESSED", "STALE"}
    after = state(p)
    assert after["app"]["version"] == 6 and after["reservation"]["status"] == "CONSUMED"
    assert after["stock"]["reserved"] == 0 and after["original"] == p.original
    assert len([row for row in records(p, FulfillmentAudit) if row["to_version"] == 6]) == 1


def test_wrong_application_inbox_belongs_only_to_its_effect_during_two_request_race(pending):
    p = pending
    service, request_id = unknown(p)
    query = service.claim("query-a")
    other = second_application(p)
    service.receive_event(event(other, 1, event_id="other-accepted", external_shipment_id="SHIP-B"))
    invalid = event(p, 2, event_id="wrong-application", application_id=other.appid)
    persist_before_processing(p, invalid)
    valid_other = event(other, 2, event_id="other-success", external_shipment_id="SHIP-B")
    persist_before_processing(p, valid_other)
    barrier = Barrier(2)

    def query_a():
        barrier.wait(timeout=5)
        return service.finish(
            query, "SUCCEEDED", external_shipment_id="HTTP-SHIPMENT-1", provider_sequence=2
        )

    def callback_b():
        barrier.wait(timeout=5)
        return service.receive_event(valid_other)

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(query_a), pool.submit(callback_b)]
        results = [future.result(timeout=10) for future in futures]
    assert results[0]["request"]["status"] == "SUCCEEDED"
    assert results[1]["status"] == "PROCESSED"
    assert state(p)["app"]["version"] == state(other)["app"]["version"] == 6
    assert state(p)["stock"]["reserved"] == 0
    wrong = next(
        row for row in records(p, WarehouseInbox) if row["event_id"] == "wrong-application"
    )
    assert wrong["status"] == "CONFLICT" and wrong["error_code"] == "EVENT_BINDING_CONFLICT"
    summary_a = service.get_status(SUPPORT, request_id)
    request_b = next(row for row in state(p)["requests"] if row["application_id"] == other.appid)
    summary_b = service.get_status(SUPPORT, request_b["id"])
    assert "wrong-application" in {row["event_id"] for row in summary_a["inbox"]}
    assert "wrong-application" not in {row["event_id"] for row in summary_b["inbox"]}


def test_competing_external_id_query_and_callback_do_not_form_stock_index_lock_cycle(pending):
    p = pending
    service, _ = unknown(p)
    query = service.claim("query-a")
    other = second_application(p)
    service.receive_event(event(other, 2, event_id="other-success"))
    barrier = Barrier(2)

    def query_a():
        barrier.wait(timeout=5)
        return service.finish(
            query, "SUCCEEDED", external_shipment_id="HTTP-SHIPMENT-1", provider_sequence=2
        )

    def callback_b():
        barrier.wait(timeout=5)
        return service.receive_event(event(other, 1, event_id="other-accepted"))

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(query_a), pool.submit(callback_b)]
        results = [future.result(timeout=10) for future in futures]
    assert results[0]["request"]["status"] in {"SUCCEEDED", "MANUAL_REVIEW"}
    assert results[1]["status"] in {"PROCESSED", "CONFLICT"}
    requests = state(p)["requests"]
    assert sum(row["status"] == "SUCCEEDED" for row in requests) == 1
    with p.service.sessions() as session:
        provider_ids = list(session.scalars(select(ShipmentRequest.provider_external_id)))
    assert provider_ids.count("HTTP-SHIPMENT-1") == 1
    assert state(p)["stock"]["reserved"] == 1 and state(p)["stock"]["available"] == 0


def test_concurrent_query_claims_only_claim_one_task(pending):
    p = pending
    service, _ = unknown(p)
    barrier = Barrier(2)

    def claim(owner):
        barrier.wait(timeout=5)
        return service.claim(owner)

    with ThreadPoolExecutor(max_workers=2) as pool:
        claims = list(pool.map(claim, ["one", "two"]))
    assert sum(claim is not None for claim in claims) == 1
    assert records(p, ReconciliationTask)[0]["attempts"] == 1


@pytest.mark.parametrize(
    "change",
    [
        {"owner": "forged"},
        {"generation": 99},
        {"cycle": 2},
        {"request_id": "missing-request"},
        {"task_id": "missing-task"},
        {"outbox_id": "missing-outbox"},
    ],
)
def test_forged_query_claim_is_rejected_before_merge(pending, change):
    p = pending
    service, _ = unknown(p)
    claim = replace(service.claim("query"), **change)
    before = state(p)
    fails("RECONCILIATION_LEASE_LOST", lambda: service.check_claim(claim))
    fails(
        "RECONCILIATION_LEASE_LOST",
        lambda: service.finish(
            claim, "SUCCEEDED", external_shipment_id="OTHER", provider_sequence=2
        ),
    )
    assert state(p) == before and records(p, ReconciliationTask)[0]["status"] == "CLAIMED"


def test_expired_get_lease_is_reclaimed_safely_and_old_generation_cannot_finish(pending):
    p = pending
    service, _ = unknown(p, lease_seconds=2)
    old = service.claim("old")
    p.clock.advance(2)
    fails("RECONCILIATION_LEASE_LOST", lambda: service.check_claim(old))
    assert service.expire_leases() == 1
    new = service.claim("new")
    assert new.generation > old.generation
    assert records(p, ReconciliationTask)[0]["attempts"] == 2
    fails("RECONCILIATION_LEASE_LOST", lambda: service.finish(old, "NOT_FOUND"))
    service.finish(new, "SUCCEEDED", external_shipment_id="HTTP-SHIPMENT-1", provider_sequence=2)
    assert state(p)["app"]["version"] == 6 and state(p)["stock"]["reserved"] == 0


def test_not_found_is_bounded_get_retry_and_never_proof_to_repeat_post(pending):
    p = pending
    service, request_id = unknown(p)
    for delay in (1, 2, 0):
        claim = service.claim("query")
        service.finish(claim, "NOT_FOUND")
        if delay:
            assert service.claim("too-soon") is None
            p.clock.advance(delay)
    before = records(p, ReconciliationTask)
    assert before[0]["status"] == "EXHAUSTED" and before[0]["attempts"] == 3
    assert service.ensure_pending() == 0 and records(p, ReconciliationTask) == before
    assert service.claim("automatic") is None and p.outbox.claim("post-again") is None
    after = state(p)
    assert after["requests"][0]["status"] == "MANUAL_REVIEW"
    assert after["outbox"][0]["status"] == "DEAD" and after["stock"]["reserved"] == 1
    fails(
        "REDRIVE_NOT_ALLOWED",
        lambda: service.redrive(
            SUPPORT,
            after["outbox"][0]["id"],
            after["outbox"][0]["generation"],
            "unsafe",
            "404 only",
        ),
    )
    assert service.get_status(CUSTOMER, request_id)["request"]["status"] == "MANUAL_REVIEW"


def test_total_query_deadline_exhausts_pending_without_starting_another_get(pending):
    p = pending
    service, _ = unknown(p, deadline_seconds=2)
    p.clock.advance(2)
    assert service.expire_leases() == 1 and service.claim("late") is None
    assert records(p, ReconciliationTask)[0]["attempts"] == 0
    assert records(p, ReconciliationTask)[0]["status"] == "EXHAUSTED"
    assert state(p)["stock"]["reserved"] == 1


def test_twenty_one_historical_tasks_and_conflicts_do_not_starve_new_unknown(pending, monkeypatch):
    p = pending
    with p.service.sessions.begin() as session:
        session.get(OrderLine, "S1002-1").quantity = 22
    original_id = fulfillment_module.new_id
    history_ids = iter(f"000-history-{index:02d}" for index in range(21))

    def history_id(prefix):
        return next(history_ids) if prefix == "shipreq" else original_id(prefix)

    old_ids = set()
    with monkeypatch.context() as patch:
        patch.setattr(fulfillment_module, "new_id", history_id)
        for index in range(21):
            old = second_application(p)
            claim = started_claim(p.outbox, "history-worker")
            p.outbox.finish(claim, "UNKNOWN", error_code="OLD_RESPONSE_LOST")
            old_ids.add(claim.request_id)
            status = ("PENDING", "CLAIMED", "DONE", "EXHAUSTED")[index % 4]
            now = p.clock()
            attempts = {"PENDING": 0, "CLAIMED": 2, "DONE": 1, "EXHAUSTED": 3}[status]
            payload = event(old, 2, event_id=f"old-conflict-{index}")
            with p.service.sessions.begin() as session:
                request = session.get(ShipmentRequest, claim.request_id)
                row = session.get(WarehouseOutbox, claim.outbox_id)
                if status == "EXHAUSTED":
                    request.status = "MANUAL_REVIEW"
                    row.status, row.generation = "DEAD", row.generation + 1
                    session.get(ExchangeFulfillment, old.appid).shipment_status = "MANUAL_REVIEW"
                session.add(
                    ReconciliationTask(
                        id=f"recon-history-{index:02d}",
                        request_id=claim.request_id,
                        status=status,
                        owner="history-owner" if status == "CLAIMED" else None,
                        generation=4,
                        lease_expires_at=now + timedelta(seconds=15)
                        if status == "CLAIMED"
                        else None,
                        attempts=attempts,
                        cycle_attempts=attempts,
                        cycle=2,
                        next_attempt_at=now,
                        deadline_at=now + timedelta(seconds=60),
                        last_error_code="KEPT_OLD_CONFLICT",
                        created_at=now,
                        updated_at=now,
                    )
                )
            persist_before_processing(p, payload)
            with p.service.sessions.begin() as session:
                inbox = session.get(WarehouseInbox, "inbox-" + payload["event_id"])
                inbox.status, inbox.error_code = "CONFLICT", "EVENT_BINDING_CONFLICT"
    _, request_id = enqueue(p)
    p.outbox.finish(
        started_claim(p.outbox, "new-delivery"), "UNKNOWN", error_code="NEW_RESPONSE_LOST"
    )
    assert max(old_ids) < request_id
    old_tasks = {row["id"]: row for row in records(p, ReconciliationTask)}
    old_inbox, before = records(p, WarehouseInbox), state(p)
    service = recovery(p)
    assert service.ensure_pending(limit=20) == 1
    tasks = records(p, ReconciliationTask)
    assert len(tasks) == 22
    assert {row["id"]: row for row in tasks if row["request_id"] in old_ids} == old_tasks
    new = next(row for row in tasks if row["request_id"] == request_id)
    assert new["status"] == "PENDING" and new["attempts"] == new["cycle_attempts"] == 0
    assert new["cycle"] == 1 and state(p) == before
    assert records(p, WarehouseInbox) == old_inbox
    assert state(p)["stock"]["reserved"] == 22
    assert service.ensure_pending(limit=20) == 0
    assert records(p, ReconciliationTask) == tasks and state(p) == before


@pytest.mark.parametrize("status", ["UNKNOWN", "FAILED"])
def test_trusted_query_success_can_merge_after_delivery_uncertainty_or_rejection(pending, status):
    p = pending
    _, request_id = enqueue(p)
    p.outbox.finish(started_claim(p.outbox, "delivery"), status, error_code="PROVIDER_TEST")
    service = recovery(p)
    generation = state(p)["outbox"][0]["generation"]
    service.manual_reconcile(SUPPORT, request_id, generation, "investigate", "Check provider truth")
    service.finish(
        service.claim("query"),
        "SUCCEEDED",
        external_shipment_id="HTTP-SHIPMENT-1",
        provider_sequence=2,
    )
    assert state(p)["app"]["version"] == 6 and state(p)["stock"]["reserved"] == 0


@pytest.mark.parametrize(
    "actor",
    [CUSTOMER, WAREHOUSE, Actor("support-adapter", "support", "trusted_adapter")],
)
def test_manual_commands_require_trusted_support_before_any_idempotent_replay(pending, actor):
    p = pending
    service, request_id = unknown(p)
    row = state(p)["outbox"][0]
    fails(
        "FORBIDDEN",
        lambda: service.manual_reconcile(actor, request_id, row["generation"], "denied", "Check"),
    )
    fails(
        "FORBIDDEN",
        lambda: service.redrive(actor, row["id"], row["generation"], "denied", "Check"),
    )
    assert records(p, RecoveryCommand) == []


def test_manual_reconcile_replay_precedes_cas_and_preserves_existing_cycle_deadline(pending):
    p = pending
    service, request_id = unknown(p)
    original_task = records(p, ReconciliationTask)[0]
    generation = state(p)["outbox"][0]["generation"]
    receipt = service.manual_reconcile(SUPPORT, request_id, generation, "manual", "Investigate")
    assert receipt["outbox_generation"] == generation + 1 and receipt["status"] == "PENDING"
    task = records(p, ReconciliationTask)[0]
    assert (
        task["cycle"] == original_task["cycle"]
        and task["deadline_at"] == original_task["deadline_at"]
    )
    assert (
        service.manual_reconcile(SUPPORT, request_id, generation, "manual", "Investigate")
        == receipt
    )
    fails(
        "IDEMPOTENCY_CONFLICT",
        lambda: service.manual_reconcile(
            SUPPORT, request_id, generation, "manual", "Different reason"
        ),
    )
    fails(
        "GENERATION_CONFLICT",
        lambda: service.manual_reconcile(SUPPORT, request_id, generation, "new-key", "Investigate"),
    )
    assert len(records(p, RecoveryCommand)) == 1
    fails(
        "FORBIDDEN",
        lambda: service.manual_reconcile(
            Actor(SUPPORT.actor_id, "customer", "test_fixture"),
            request_id,
            generation,
            "manual",
            "Investigate",
        ),
    )
    service.finish(
        service.claim("query"),
        "SUCCEEDED",
        external_shipment_id="HTTP-SHIPMENT-1",
        provider_sequence=2,
    )
    assert (
        service.manual_reconcile(SUPPORT, request_id, generation, "manual", "Investigate")
        == receipt
    )


def test_manual_reconcile_fences_claimed_get_without_refreshing_its_budget(pending):
    p = pending
    service, request_id = unknown(p)
    old = service.claim("query")
    before = records(p, ReconciliationTask)[0]
    generation = state(p)["outbox"][0]["generation"]
    service.manual_reconcile(SUPPORT, request_id, generation, "recheck", "Pause old query")
    after = records(p, ReconciliationTask)[0]
    assert after["status"] == "PENDING" and after["attempts"] == before["attempts"]
    assert after["cycle_attempts"] == before["cycle_attempts"]
    assert after["deadline_at"] == before["deadline_at"]
    fails("RECONCILIATION_LEASE_LOST", lambda: service.finish(old, "NOT_FOUND"))


def test_explicit_query_cycles_reset_only_cycle_budget_and_stop_at_limit(pending):
    p = pending
    service, request_id = unknown(p, max_attempts=1, max_cycles=3)
    for cycle in range(1, 4):
        service.finish(service.claim("query"), "UNKNOWN")
        task = records(p, ReconciliationTask)[0]
        assert task["status"] == "EXHAUSTED" and task["attempts"] == cycle
        assert task["cycle_attempts"] == 1 and task["cycle"] == cycle
        row = state(p)["outbox"][0]
        if cycle < 3:
            service.manual_reconcile(
                SUPPORT, request_id, row["generation"], f"cycle-{cycle}", "Retry GET"
            )
            task = records(p, ReconciliationTask)[0]
            assert task["cycle_attempts"] == 0 and task["attempts"] == cycle
    fails(
        "RECOVERY_LIMIT_EXCEEDED",
        lambda: service.manual_reconcile(
            SUPPORT, request_id, row["generation"], "cycle-4", "Retry GET"
        ),
    )
    assert state(p)["stock"]["reserved"] == 1 and state(p)["app"]["version"] == 5


def test_trusted_query_can_merge_after_manual_review_without_repeating_delivery(pending):
    p = pending
    service, request_id = unknown(p, max_attempts=1)
    service.finish(service.claim("first-query"), "UNKNOWN")
    row = state(p)["outbox"][0]
    assert row["status"] == "DEAD" and state(p)["requests"][0]["status"] == "MANUAL_REVIEW"
    service.manual_reconcile(
        SUPPORT, request_id, row["generation"], "investigate", "Provider recovered"
    )
    service.finish(
        service.claim("second-query"),
        "SUCCEEDED",
        external_shipment_id="HTTP-SHIPMENT-1",
        provider_sequence=2,
    )
    assert state(p)["app"]["version"] == 6 and state(p)["stock"]["reserved"] == 0
    assert len(state(p)["attempts"]) == 1 and state(p)["original"] == p.original


def test_concurrent_manual_cas_allows_only_one_new_command(pending):
    p = pending
    service, request_id = unknown(p)
    generation = state(p)["outbox"][0]["generation"]
    barrier = Barrier(2)

    def call(key):
        barrier.wait(timeout=5)
        try:
            return service.manual_reconcile(SUPPORT, request_id, generation, key, "Check")
        except BusinessError as error:
            return error.code

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(call, ["one", "two"]))
    assert len([result for result in results if isinstance(result, dict)]) == 1
    assert "GENERATION_CONFLICT" in results and len(records(p, RecoveryCommand)) == 1


def test_redrive_proven_rejection_retains_monotonic_totals_and_consumes_once(pending):
    p = pending
    _, request_id = enqueue(p)
    first = started_claim(p.outbox, "delivery")
    p.outbox.finish(first, "FAILED", error_code="WAREHOUSE_PERMANENT_REJECTION")
    service = recovery(p)
    row = state(p)["outbox"][0]
    receipt = service.redrive(
        SUPPORT, row["id"], row["generation"], "retry-delivery", "Provider fixed"
    )
    after = state(p)
    assert receipt["status"] == "PENDING" and receipt["cycle"] == 1
    assert after["outbox"][0]["attempts"] == 1 and after["outbox"][0]["cycle_attempts"] == 0
    assert (
        service.redrive(SUPPORT, row["id"], row["generation"], "retry-delivery", "Provider fixed")
        == receipt
    )
    second = started_claim(p.outbox, "delivery-again")
    assert second.generation > first.generation and second.attempts == 2
    p.outbox.finish(second, "SUCCEEDED", external_shipment_id="HTTP-SHIPMENT-1")
    assert state(p)["app"]["version"] == 6 and state(p)["stock"]["reserved"] == 0
    assert service.get_status(CUSTOMER, request_id)["request"]["status"] == "SUCCEEDED"


def test_finite_safe_redrive_cycles_do_not_reset_total_delivery_attempts(pending):
    p = pending
    enqueue(p)
    service = recovery(p, max_cycles=2)
    for cycle in range(3):
        p.outbox.finish(
            started_claim(p.outbox, f"delivery-{cycle}"), "FAILED", error_code="PROVIDER_REJECTED"
        )
        row = state(p)["outbox"][0]
        assert row["attempts"] == cycle + 1 and row["redrive_count"] == cycle
        if cycle < 2:
            service.redrive(
                SUPPORT, row["id"], row["generation"], f"redrive-{cycle}", "Confirmed not accepted"
            )
    fails(
        "RECOVERY_LIMIT_EXCEEDED",
        lambda: service.redrive(
            SUPPORT, row["id"], row["generation"], "redrive-limit", "Another retry"
        ),
    )
    assert state(p)["stock"]["reserved"] == 1 and len(records(p, RecoveryCommand)) == 2


def test_recovery_get_keeps_c1_shapes_and_customer_ownership_without_management_secrets(pending):
    p = pending
    service, request_id = unknown(p)
    service.receive_event(event(p, 2))
    service.claim("sensitive-owner")
    result = service.get_status(CUSTOMER, request_id)
    assert set(result) == {"request", "outbox", "attempts", "reconciliation", "inbox", "conflicts"}
    assert "payload" not in result["request"] and "last_sequence" not in result["request"]
    assert not {"lease_owner", "lease_expires_at", "generation"} & result["outbox"].keys()
    assert not {"owner", "lease_expires_at", "generation"} & result["reconciliation"].keys()
    assert all("payload" not in item and "body_hash" not in item for item in result["inbox"])
    assert "sensitive-owner" not in str(result)
    assert service.get_status(SUPPORT, request_id)["management"]["outbox_generation"] == 1
    fails(
        "NOT_FOUND",
        lambda: service.get_status(Actor("bob", "customer", "test_fixture"), request_id),
    )


def test_0010_preserves_old_columns_unknown_reservations_and_retry_counters(legacy_schema):
    connection, _old = legacy_schema
    for revision in ("0007_exchange_fulfillment", "0008_staff_auth", "0009_warehouse_outbox"):
        upgrade(connection, revision)
    metadata = MetaData()
    metadata.reflect(bind=connection)
    tables = metadata.tables
    application_id = connection.scalar(
        select(tables["aftercare_requests"].c.id).where(
            tables["aftercare_requests"].c.status == "EXCHANGE_REQUESTED"
        )
    )
    payload = {
        "effect_key": "exchange:" + application_id,
        "application_id": application_id,
        "variant_id": "legacy-new",
        "quantity": 1,
    }
    connection.execute(
        tables["aftercare_requests"]
        .update()
        .where(tables["aftercare_requests"].c.id == application_id)
        .values(status="REPLACEMENT_PENDING", version=5)
    )
    connection.execute(
        tables["exchange_fulfillments"]
        .update()
        .where(tables["exchange_fulfillments"].c.application_id == application_id)
        .values(effect_key=payload["effect_key"], shipment_status="UNKNOWN")
    )
    connection.execute(
        tables["shipment_requests"].insert(),
        {
            "id": "shipreq-existing-unknown",
            "application_id": application_id,
            "effect_key": payload["effect_key"],
            "payload": payload,
            "request_hash": digest(payload),
            "status": "UNKNOWN",
            "triggered_by": {
                "actor_id": "old-warehouse",
                "role": "warehouse",
                "source": "server_session",
            },
            "created_at": NOW,
            "updated_at": NOW,
        },
    )
    connection.execute(
        tables["warehouse_outbox"].insert(),
        {
            "id": "outbox-existing-unknown",
            "request_id": "shipreq-existing-unknown",
            "payload": payload,
            "status": "BLOCKED",
            "generation": 1,
            "attempts": 1,
            "next_attempt_at": NOW,
            "last_error_code": "RESPONSE_LOST",
            "created_at": NOW,
            "updated_at": NOW,
        },
    )
    connection.execute(
        tables["outbox_attempts"].insert(),
        {
            "id": "attempt-existing-unknown",
            "outbox_id": "outbox-existing-unknown",
            "generation": 1,
            "owner": "old-worker",
            "started_at": NOW,
            "finished_at": NOW,
            "outcome": "UNKNOWN",
            "error_code": "RESPONSE_LOST",
        },
    )
    before = snapshot_rows(connection, tables)
    upgrade(connection, "0010_warehouse_recovery")
    assert snapshot_rows(connection, tables) == before
    new = MetaData()
    new.reflect(bind=connection)
    assert connection.scalar(select(new.tables["warehouse_outbox"].c.cycle_attempts)) == 1
    assert connection.scalar(select(new.tables["warehouse_outbox"].c.redrive_count)) == 0
    assert connection.scalar(select(new.tables["shipment_requests"].c.last_sequence)) == 0
    assert connection.scalar(select(new.tables["shipment_requests"].c.provider_external_id)) is None
    for name in ("warehouse_inbox", "inbox_conflicts", "reconciliation_tasks", "recovery_commands"):
        assert connection.scalar(select(func.count()).select_from(new.tables[name])) == 0
    with pytest.raises(RuntimeError, match="WAREHOUSE_RECOVERY_DOWNGRADE_FORBIDDEN"):
        migration("0010_warehouse_recovery").downgrade()
