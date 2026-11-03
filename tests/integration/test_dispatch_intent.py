"""A committed claim is safe to repeat only while its sending permission is absent."""

from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from threading import Barrier, Event

import pytest
from sqlalchemy import MetaData, select
from sqlalchemy import event as sqlalchemy_event
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session
from test_fulfillment import CUSTOMER, SUPPORT
from test_fulfillment_migration import NOW, migration, snapshot_rows, upgrade
from test_fulfillment_migration import legacy_schema as legacy_schema
from test_outbox import enqueue, fails, state
from test_outbox import pending as pending
from test_warehouse_recovery import event

from aftercare.domain.contracts import BusinessError
from aftercare.repositories.models import (
    Inventory,
    OutboxAttempt,
    ReconciliationTask,
    WarehouseOutbox,
)
from aftercare.services.business import digest
from aftercare.services.recovery import RecoveryService
from aftercare.services.warehouse_http import DeliveryResult
from aftercare.workers.outbox_worker import OutboxWorker

pytestmark = pytest.mark.integration


def sending(p):
    with p.service.sessions() as session:
        row = session.scalar(select(OutboxAttempt).order_by(OutboxAttempt.generation.desc()))
        return {
            "state": row.send_state,
            "time": row.send_started_at,
            "generation": row.generation,
            "outcome": row.outcome,
            "error": row.error_code,
        }


def test_new_claim_and_check_have_no_sending_permission(pending):
    p = pending
    enqueue(p)
    claim = p.outbox.claim("one")
    assert sending(p)["state"] == "NOT_STARTED" and sending(p)["time"] is None
    before = state(p)
    assert p.outbox.check_claim(claim) is None
    assert state(p) == before
    assert p.outbox.begin_delivery(claim) is None
    assert sending(p)["state"] == "STARTED" and sending(p)["time"] == p.clock()
    after = state(p)
    fails("DELIVERY_ALREADY_STARTED", lambda: p.outbox.begin_delivery(claim))
    assert state(p) == after


@pytest.mark.parametrize("outcome", ["SUCCEEDED", "UNKNOWN", "FAILED", "RETRYABLE"])
def test_unarmed_attempt_cannot_invent_http_result(pending, outcome):
    p = pending
    enqueue(p)
    claim = p.outbox.claim("one")
    before = state(p)
    fields = {"external_shipment_id": "FAKE-SUCCESS"} if outcome == "SUCCEEDED" else {}
    fails("DELIVERY_NOT_STARTED", lambda: p.outbox.finish(claim, outcome, **fields))
    assert state(p) == before


def test_claim_expired_before_permission_requeues_original_key_and_fences_old_worker(pending):
    p = pending
    _, request_id = enqueue(p)
    old = p.outbox.claim("killed-before-arm")
    p.clock.advance(15)
    assert p.outbox.expire_leases() == 1
    after = state(p)
    assert after["outbox"][0]["status"] == "PENDING"
    assert after["requests"][0]["status"] == after["fulfillment"]["shipment_status"] == "PENDING"
    assert after["outbox"][0]["generation"] == old.generation + 1
    assert sending(p)["outcome"] == "RETRYABLE" and sending(p)["error"] == "HTTP_NOT_STARTED"
    assert sending(p)["state"] == "NOT_STARTED" and sending(p)["time"] is None
    assert after["stock"]["reserved"] == 1 and after["app"]["version"] == 5
    assert RecoveryService(p.service.sessions, p.clock).ensure_pending() == 0
    with p.service.sessions() as session:
        assert session.scalar(select(ReconciliationTask)) is None
    fails("OUTBOX_LEASE_LOST", lambda: p.outbox.begin_delivery(old))
    fails("OUTBOX_LEASE_LOST", lambda: p.outbox.finish(old, "UNKNOWN"))
    new = p.outbox.claim("replacement")
    assert new.generation > old.generation and new.attempts == 2
    assert new.payload == old.payload and new.request_hash == old.request_hash
    p.outbox.begin_delivery(new)
    result = p.outbox.finish(new, "SUCCEEDED", external_shipment_id="REAL-SUCCESS")
    assert result["request"]["id"] == request_id and result["request"]["status"] == "SUCCEEDED"
    assert state(p)["reservation"]["status"] == "CONSUMED"
    assert state(p)["stock"]["reserved"] == 0 and state(p)["original"] == p.original


def test_repeated_unsent_expiry_is_finite_and_existing_redrive_requires_support(pending):
    p = pending
    enqueue(p)
    for number in range(1, 4):
        claim = p.outbox.claim("dies-before-arm")
        assert claim.attempts == number
        p.clock.advance(15)
        assert p.outbox.expire_leases() == 1
    after = state(p)
    row = after["outbox"][0]
    assert row["status"] == "DEAD" and row["cycle_attempts"] == row["attempts"] == 3
    assert after["requests"][0]["status"] == "MANUAL_REVIEW"
    assert all(
        a["outcome"] == "RETRYABLE" and a["error_code"] == "HTTP_NOT_STARTED"
        for a in after["attempts"]
    )
    assert after["stock"]["reserved"] == 1 and p.outbox.claim("automatic") is None
    recovery = RecoveryService(p.service.sessions, p.clock)
    fails(
        "FORBIDDEN",
        lambda: recovery.redrive(CUSTOMER, row["id"], row["generation"], "denied", "Retry unsent"),
    )
    recovery.redrive(
        SUPPORT, row["id"], row["generation"], "safe-redrive", "Three attempts never armed"
    )
    new = p.outbox.claim("after-support-command")
    assert new.attempts == 4 and new.generation > claim.generation
    p.outbox.begin_delivery(new)
    p.outbox.finish(new, "SUCCEEDED", external_shipment_id="AFTER-REDRIVE")
    assert state(p)["stock"]["reserved"] == 0 and state(p)["app"]["version"] == 6


@pytest.mark.parametrize("phase", ["STARTED", "LEGACY_UNKNOWN"])
def test_permission_or_legacy_uncertainty_stays_unknown_and_404_never_authorizes_post(
    pending, phase
):
    p = pending
    _, request_id = enqueue(p)
    claim = p.outbox.claim("possibly-sent")
    if phase == "STARTED":
        p.outbox.begin_delivery(claim)
    else:
        with p.service.sessions.begin() as session:
            session.scalar(select(OutboxAttempt)).send_state = "LEGACY_UNKNOWN"
        fails("DELIVERY_ALREADY_STARTED", lambda: p.outbox.begin_delivery(claim))
    p.clock.advance(15)
    assert p.outbox.expire_leases() == 1
    assert state(p)["requests"][0]["status"] == "UNKNOWN"
    assert state(p)["outbox"][0]["status"] == "BLOCKED" and p.outbox.claim("repeat-post") is None
    service = RecoveryService(p.service.sessions, p.clock, max_attempts=1)
    service.ensure_pending()
    service.finish(service.claim("query"), "NOT_FOUND")
    row = state(p)["outbox"][0]
    assert service.get_status(CUSTOMER, request_id)["reconciliation"]["status"] == "EXHAUSTED"
    fails(
        "REDRIVE_NOT_ALLOWED",
        lambda: service.redrive(SUPPORT, row["id"], row["generation"], "unsafe", "Only a 404"),
    )
    assert state(p)["stock"]["reserved"] == 1 and state(p)["original"] == p.original


def test_legacy_inflight_finish_remains_conservatively_compatible(pending):
    p = pending
    enqueue(p)
    claim = p.outbox.claim("old-consumer")
    with p.service.sessions.begin() as session:
        session.scalar(select(OutboxAttempt)).send_state = "LEGACY_UNKNOWN"
    p.outbox.finish(claim, "SUCCEEDED", external_shipment_id="OLD-CONSUMER-SUCCESS")
    assert state(p)["app"]["version"] == 6 and state(p)["stock"]["reserved"] == 0


def test_competing_permissions_grant_only_one_delivery_attempt(pending):
    p = pending
    enqueue(p)
    claim = p.outbox.claim("one")
    barrier = Barrier(2)

    def permit():
        barrier.wait(timeout=5)
        try:
            return p.outbox.begin_delivery(claim)
        except BusinessError as error:
            return error.code

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _: permit(), range(2)))
    assert results.count(None) == results.count("DELIVERY_ALREADY_STARTED") == 1
    assert sending(p)["state"] == "STARTED" and state(p)["stock"]["reserved"] == 1


def test_permission_rechecks_deadline_after_waiting_for_domain_lock(pending, monkeypatch):
    p = pending
    enqueue(p)
    claim = p.outbox.claim("blocked")
    entered = Event()
    original = p.outbox._domain

    def domain(session, request):
        entered.set()
        return original(session, request)

    monkeypatch.setattr(p.outbox, "_domain", domain)
    with ThreadPoolExecutor(max_workers=1) as pool:
        with p.service.sessions.begin() as blocker:
            blocker.scalar(
                select(Inventory).where(Inventory.variant_id == "tee-white-L").with_for_update()
            )
            future = pool.submit(p.outbox.begin_delivery, claim)
            assert entered.wait(timeout=5)
            p.clock.advance(15)
        with pytest.raises(BusinessError) as error:
            future.result(timeout=10)
    assert error.value.code == "OUTBOX_LEASE_LOST"
    assert sending(p)["state"] == "NOT_STARTED" and sending(p)["time"] is None
    assert state(p)["stock"]["reserved"] == 1 and state(p)["app"]["version"] == 5


def test_send_intent_is_rolled_back_if_lease_expires_during_flush(pending):
    p = pending
    enqueue(p)
    claim = p.outbox.claim("one")

    def after_flush(session, context):
        if any(
            isinstance(row, OutboxAttempt) and row.send_state == "STARTED" for row in session.dirty
        ):
            p.clock.advance(15)

    sqlalchemy_event.listen(Session, "after_flush", after_flush)
    try:
        fails("OUTBOX_LEASE_LOST", lambda: p.outbox.begin_delivery(claim))
    finally:
        sqlalchemy_event.remove(Session, "after_flush", after_flush)
    assert sending(p)["state"] == "NOT_STARTED" and sending(p)["time"] is None
    assert p.outbox.expire_leases() == 1
    assert state(p)["outbox"][0]["status"] == "PENDING" and state(p)["stock"]["reserved"] == 1


def test_tampered_claim_is_quarantined_without_arming_or_consuming_stock(pending):
    p = pending
    enqueue(p)
    claim = p.outbox.claim("one")
    claim.payload["quantity"] = 9
    fails("OUTBOX_LEASE_LOST", lambda: p.outbox.begin_delivery(claim))
    assert sending(p)["state"] == "NOT_STARTED" and sending(p)["time"] is None
    assert state(p)["outbox"][0]["status"] == "DEAD"
    assert state(p)["requests"][0]["status"] == "MANUAL_REVIEW"
    assert state(p)["stock"]["reserved"] == 1 and state(p)["original"] == p.original


def test_worker_opens_http_only_after_permission_commit_and_outside_database_locks(pending):
    p = pending
    enqueue(p)
    calls = []

    class Client:
        def deliver(self, payload, request_hash):
            with p.service.sessions.begin() as observer:
                row = observer.scalar(select(WarehouseOutbox).with_for_update(nowait=True))
                attempt = observer.scalar(select(OutboxAttempt))
                assert row.status == "CLAIMED" and attempt.send_state == "STARTED"
                assert attempt.send_started_at == p.clock()
            calls.append((payload, request_hash))
            return DeliveryResult("SUCCEEDED", external_shipment_id="WORKER-SUCCESS")

    result = OutboxWorker(p.outbox, Client()).run_once()
    assert result["request"]["status"] == "SUCCEEDED" and len(calls) == 1
    assert state(p)["app"]["version"] == 6 and state(p)["stock"]["reserved"] == 0


@pytest.mark.parametrize("expired", [False, True])
def test_worker_with_duplicate_or_stale_permission_never_opens_another_http(pending, expired):
    p = pending
    enqueue(p)
    claim = p.outbox.claim("one")
    p.outbox.begin_delivery(claim)
    if expired:
        p.clock.advance(15)
    before = state(p)

    class Service:
        def expire_leases(self):
            return 0

        def claim(self, owner):
            return claim

        def begin_delivery(self, value):
            return p.outbox.begin_delivery(value)

        def finish(self, *args, **kwargs):
            pytest.fail("No result may be merged after refused permission")

    class Client:
        def deliver(self, *args, **kwargs):
            pytest.fail("A refused permission must not open HTTP")

    result = OutboxWorker(Service(), Client()).run_once()
    expected = "OUTBOX_LEASE_LOST" if expired else "DELIVERY_ALREADY_STARTED"
    assert result["error_code"] == expected and state(p) == before


def test_callback_completion_before_arm_fences_delivery_and_cannot_be_requeued(pending):
    p = pending
    enqueue(p)
    claim = p.outbox.claim("not-yet-sent")
    service = RecoveryService(p.service.sessions, p.clock)
    service.receive_event(event(p, 1))
    service.receive_event(event(p, 2))
    after = state(p)
    fails("OUTBOX_LEASE_LOST", lambda: p.outbox.begin_delivery(claim))
    p.clock.advance(15)
    assert p.outbox.expire_leases() == 0 and p.outbox.claim("later") is None
    assert state(p) == after and state(p)["stock"]["reserved"] == 0


def test_known_acceptance_is_not_erased_by_an_unarmed_attempt_expiry(pending):
    p = pending
    enqueue(p)
    p.outbox.claim("not-yet-sent")
    RecoveryService(p.service.sessions, p.clock).receive_event(event(p, 1))
    p.clock.advance(15)
    assert p.outbox.expire_leases() == 1
    assert state(p)["outbox"][0]["status"] == "BLOCKED"
    assert state(p)["requests"][0]["status"] == "UNKNOWN" and sending(p)["outcome"] == "UNKNOWN"
    assert state(p)["stock"]["reserved"] == 1 and p.outbox.claim("repeat") is None


def test_accepted_fact_refuses_permission_and_hands_off_to_query_without_post(pending):
    p = pending
    enqueue(p)
    claim = p.outbox.claim("paused-before-arm")
    recovery = RecoveryService(p.service.sessions, p.clock)
    assert recovery.receive_event(event(p, 1))["status"] == "PROCESSED"
    before = state(p)
    calls = []

    class OwnedClaimService:
        def expire_leases(self):
            return 0

        def claim(self, owner):
            return claim

        def begin_delivery(self, value):
            return p.outbox.begin_delivery(value)

        def finish(self, *args, **kwargs):
            pytest.fail("Known acceptance must be merged through query, not another POST")

    class Client:
        def deliver(self, *args, **kwargs):
            calls.append("POST")
            pytest.fail("A known ACCEPTED fact must not grant another sending permission")

    result = OutboxWorker(OwnedClaimService(), Client()).run_once()
    assert result["status"] == "LEASE_LOST" and result["error_code"] == "OUTBOX_LEASE_LOST"
    assert calls == []
    after = state(p)
    assert after["app"] == before["app"] and after["app"]["version"] == 5
    assert after["stock"] == before["stock"] and after["stock"]["reserved"] == 1
    assert after["requests"][0]["status"] == after["fulfillment"]["shipment_status"] == "UNKNOWN"
    assert after["outbox"][0]["status"] == "BLOCKED"
    assert after["requests"][0]["last_sequence"] == 1
    assert after["requests"][0]["provider_external_id"] == "HTTP-SHIPMENT-1"
    assert sending(p)["state"] == "NOT_STARTED" and sending(p)["time"] is None
    assert sending(p)["outcome"] == "UNKNOWN" and sending(p)["error"] == "PROVIDER_ALREADY_ACCEPTED"
    assert after["original"] == p.original
    fails("OUTBOX_LEASE_LOST", lambda: p.outbox.begin_delivery(claim))
    assert recovery.ensure_pending() == 1
    recovery.finish(
        recovery.claim("query-known-acceptance"),
        "SUCCEEDED",
        external_shipment_id="HTTP-SHIPMENT-1",
        provider_sequence=2,
    )
    completed = state(p)
    assert completed["app"]["version"] == 6 and completed["stock"]["reserved"] == 0
    assert completed["reservation"]["status"] == "CONSUMED" and completed["original"] == p.original
    fails(
        "OUTBOX_LEASE_LOST",
        lambda: p.outbox.finish(claim, "SUCCEEDED", external_shipment_id="HTTP-SHIPMENT-1"),
    )
    assert recovery.receive_event(event(p, 2))["status"] == "STALE"
    assert state(p) == completed and calls == []


def test_0011_preserves_all_prior_facts_and_never_infers_legacy_not_sent(legacy_schema):
    connection, _ = legacy_schema
    for revision in (
        "0007_exchange_fulfillment",
        "0008_staff_auth",
        "0009_warehouse_outbox",
        "0010_warehouse_recovery",
    ):
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
        tables["shipment_requests"].insert(),
        {
            "id": "legacy-request",
            "application_id": application_id,
            "effect_key": payload["effect_key"],
            "payload": payload,
            "request_hash": digest(payload),
            "status": "PENDING",
            "triggered_by": {"actor_id": "legacy-worker"},
            "created_at": NOW,
            "updated_at": NOW,
        },
    )
    connection.execute(
        tables["warehouse_outbox"].insert(),
        {
            "id": "legacy-outbox",
            "request_id": "legacy-request",
            "payload": payload,
            "status": "CLAIMED",
            "lease_owner": "legacy-owner",
            "generation": 1,
            "lease_expires_at": NOW + timedelta(seconds=15),
            "attempts": 1,
            "cycle_attempts": 1,
            "next_attempt_at": NOW,
            "created_at": NOW,
            "updated_at": NOW,
        },
    )
    attempt = {
        "id": "legacy-attempt",
        "outbox_id": "legacy-outbox",
        "generation": 1,
        "owner": "legacy-owner",
        "started_at": NOW,
    }
    connection.execute(tables["outbox_attempts"].insert(), attempt)
    before = snapshot_rows(connection, tables)
    upgrade(connection, "0011_dispatch_intent")
    assert snapshot_rows(connection, tables) == before
    latest = MetaData()
    latest.reflect(bind=connection)
    attempts = latest.tables["outbox_attempts"]
    row = connection.execute(select(attempts)).mappings().one()
    assert row["send_state"] == "LEGACY_UNKNOWN" and row["send_started_at"] is None
    connection.execute(
        attempts.insert(), {**attempt, "id": "mixed-worker-attempt", "generation": 2}
    )
    assert (
        connection.scalar(select(attempts.c.send_state).where(attempts.c.generation == 2))
        == "LEGACY_UNKNOWN"
    )
    with pytest.raises(IntegrityError), connection.begin_nested():
        connection.execute(
            attempts.update().where(attempts.c.generation == 2).values(send_state="STARTED")
        )
    with pytest.raises(RuntimeError, match="DISPATCH_INTENT_DOWNGRADE_FORBIDDEN"):
        migration("0011_dispatch_intent").downgrade()
