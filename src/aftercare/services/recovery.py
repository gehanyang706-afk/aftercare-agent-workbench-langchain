"""Durable callbacks and bounded GET recovery. No network I/O inside transactions."""

from dataclasses import dataclass
from datetime import timedelta

from pydantic import ValidationError
from sqlalchemy import exists, or_, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from aftercare.domain.contracts import BusinessError, utc_now
from aftercare.domain.fulfillment import FulfillmentCommandInput
from aftercare.domain.rules import require
from aftercare.domain.warehouse import PROVIDER_ID, WarehouseEvent
from aftercare.repositories.models import (
    Application,
    FulfillmentAudit,
    FulfillmentCommand,
    InboxConflict,
    Order,
    OrderLine,
    OutboxAttempt,
    ReconciliationTask,
    RecoveryCommand,
    ShipmentRequest,
    WarehouseInbox,
    WarehouseOutbox,
)
from aftercare.services.business import columns, digest, new_id
from aftercare.services.fulfillment import (
    FulfillmentService,
    _locked,
    _owned,
    shipment_request_view,
    validate_actor,
)
from aftercare.services.outbox import OUTBOX_ACTOR, OutboxService, attempt_view, outbox_view


@dataclass(frozen=True)
class ReconciliationClaim:
    task_id: str
    request_id: str
    outbox_id: str
    owner: str
    generation: int
    cycle: int
    effect_key: str
    application_id: str
    request_hash: str


def _task(s, request_id):
    return s.scalar(
        select(ReconciliationTask)
        .where(ReconciliationTask.request_id == request_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )


def _binding(s, request, external_id):
    require(
        isinstance(external_id, str)
        and 0 < len(external_id.strip()) <= 100
        and external_id == external_id.strip()
        and request.provider_external_id in {None, external_id}
        and request.external_shipment_id in {None, external_id},
        "RECOVERY_BINDING_CONFLICT",
    )
    other = s.scalar(
        select(ShipmentRequest.id).where(
            ShipmentRequest.id != request.id,
            or_(
                ShipmentRequest.provider_external_id == external_id,
                ShipmentRequest.external_shipment_id == external_id,
            ),
        )
    )
    require(other is None, "RECOVERY_BINDING_CONFLICT")


def _snapshot(request, row, app, fulfillment):
    expected = {
        "effect_key": "exchange:" + app.id,
        "application_id": app.id,
        "variant_id": app.target_variant,
        "quantity": app.quantity,
    }
    require(
        request.application_id == app.id
        and row.request_id == request.id
        and row.payload == request.payload == expected
        and request.request_hash == digest(expected)
        and request.effect_key == fulfillment.effect_key == expected["effect_key"],
        "RECOVERY_BINDING_CONFLICT",
    )


def merge_success(s, clock, row, request, external_id, sequence, *, evidence, fence=True):
    """Caller holds Outbox -> request; all paths merge the same trusted shipment fact."""
    require(type(sequence) is int and sequence >= 2, "RECOVERY_BINDING_CONFLICT")
    task = _task(s, request.id)
    app, fulfillment = OutboxService(None, clock)._domain(s, request)
    _snapshot(request, row, app, fulfillment)
    _binding(s, request, external_id)
    receipt = None
    if request.status == "SUCCEEDED":
        require(
            app.status in {"REPLACEMENT_SHIPPED", "COMPLETED"}
            and fulfillment.external_shipment_id == external_id,
            "RECOVERY_BINDING_CONFLICT",
        )
        prior = s.scalar(
            select(FulfillmentCommand).where(
                FulfillmentCommand.application_id == app.id,
                FulfillmentCommand.actor_id == OUTBOX_ACTOR.actor_id,
                FulfillmentCommand.idempotency_key == request.id,
            )
        )
        receipt = prior.receipt if prior else None
    else:
        require(app.status == "REPLACEMENT_PENDING", "RECOVERY_BINDING_CONFLICT")
        bound = sessionmaker(
            s.connection(), expire_on_commit=False, join_transaction_mode="create_savepoint"
        )
        receipt = FulfillmentService(bound, clock=clock).execute(
            OUTBOX_ACTOR,
            app.id,
            FulfillmentCommandInput(
                command="confirm_shipment",
                expected_version=app.version,
                idempotency_key=request.id,
                data={"external_shipment_id": external_id},
            ),
        )
        audit = s.scalar(
            select(FulfillmentAudit).where(FulfillmentAudit.command_id == receipt["command_id"])
        )
        require(audit is not None, "RECOVERY_INVARIANT")
        audit.details = {
            **audit.details,
            "triggered_by": request.triggered_by,
            "shipment_request_id": request.id,
            "recovery": evidence,
        }
    now, old_generation = clock(), row.generation
    request.status, request.external_shipment_id = "SUCCEEDED", external_id
    request.provider_external_id = external_id
    request.last_sequence = max(request.last_sequence, sequence)
    request.updated_at = now
    if row.status == "CLAIMED":
        attempt = s.scalar(
            select(OutboxAttempt)
            .where(
                OutboxAttempt.outbox_id == row.id,
                OutboxAttempt.generation == old_generation,
                OutboxAttempt.finished_at.is_(None),
            )
            .with_for_update()
        )
        if attempt:
            attempt.finished_at, attempt.outcome, attempt.error_code = now, "SUCCEEDED", None
    row.status, row.lease_owner, row.lease_expires_at = "DELIVERED", None, None
    row.updated_at, row.last_error_code = now, None
    if fence:
        row.generation += 1
    if task is not None:
        if task.status != "DONE":
            task.generation += 1
        task.status, task.owner, task.lease_expires_at = "DONE", None, None
        task.updated_at, task.last_error_code = now, None
    return receipt


def _inbox_view(row):
    allowed = {
        "id",
        "source",
        "event_id",
        "sequence",
        "effect_key",
        "application_id",
        "external_shipment_id",
        "request_hash",
        "status",
        "error_code",
        "created_at",
        "updated_at",
    }
    return {k: v for k, v in columns(row).items() if k in allowed}


def _task_view(task, request):
    allowed = {
        "id",
        "status",
        "attempts",
        "cycle",
        "next_attempt_at",
        "deadline_at",
        "last_error_code",
        "created_at",
        "updated_at",
    }
    result = (
        {k: v for k, v in columns(task).items() if k in allowed}
        if task
        else {"status": "NOT_SCHEDULED"}
    )
    result["last_sequence"] = request.last_sequence
    if request.provider_external_id is not None:
        result["provider_external_id"] = request.provider_external_id
    return result


class RecoveryService:
    def __init__(
        self,
        sessions,
        clock=utc_now,
        lease_seconds=15,
        max_attempts=3,
        deadline_seconds=60,
        max_cycles=3,
    ):
        if not (
            1 <= lease_seconds <= 300
            and 1 <= max_attempts <= 10
            and 1 <= deadline_seconds <= 3600
            and 1 <= max_cycles <= 10
        ):
            raise ValueError("INVALID_RECOVERY_LIMITS")
        self.sessions, self.clock = sessions, clock
        self.lease_seconds, self.max_attempts = lease_seconds, max_attempts
        self.deadline_seconds, self.max_cycles = deadline_seconds, max_cycles

    def _locked_request(self, s, request_id, *, missing_code="NOT_FOUND"):
        row = s.scalar(
            select(WarehouseOutbox)
            .where(WarehouseOutbox.request_id == request_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        require(row is not None, missing_code)
        request = _locked(s, ShipmentRequest, request_id)
        require(request is not None, missing_code)
        return row, request

    def _schedule(self, s, request):
        task = _task(s, request.id)
        if task is None and request.status != "SUCCEEDED":
            now = self.clock()
            task = ReconciliationTask(
                id=new_id("recon"),
                request_id=request.id,
                status="PENDING",
                generation=0,
                attempts=0,
                cycle_attempts=0,
                cycle=1,
                next_attempt_at=now,
                deadline_at=now + timedelta(seconds=self.deadline_seconds),
                created_at=now,
                updated_at=now,
            )
            s.add(task)
            s.flush()
        return task

    def _exhaust(self, s, row, request, task, code):
        now = self.clock()
        task.status, task.owner, task.lease_expires_at = "EXHAUSTED", None, None
        task.last_error_code, task.updated_at = code, now
        if request.status != "SUCCEEDED":
            app, fulfillment = OutboxService(None, self.clock)._domain(s, request)
            request.status, request.updated_at = "MANUAL_REVIEW", now
            if app.status == "REPLACEMENT_PENDING":
                fulfillment.shipment_status, fulfillment.updated_at = "MANUAL_REVIEW", now
            if row.status == "CLAIMED":
                attempt = s.scalar(
                    select(OutboxAttempt)
                    .where(
                        OutboxAttempt.outbox_id == row.id,
                        OutboxAttempt.generation == row.generation,
                        OutboxAttempt.finished_at.is_(None),
                    )
                    .with_for_update()
                )
                if attempt:
                    attempt.finished_at, attempt.outcome, attempt.error_code = now, "UNKNOWN", code
            row.status, row.lease_owner, row.lease_expires_at = "DEAD", None, None
            row.generation += 1
            row.last_error_code, row.updated_at = code, now

    def _event_state(self, event, status, code=None):
        event.status, event.error_code, event.updated_at = status, code, self.clock()

    def _drain(self, s, row, request):
        # The Outbox gate serializes every inbox/task for this request before their row locks.
        events = list(
            s.scalars(
                select(WarehouseInbox)
                .where(
                    WarehouseInbox.source == PROVIDER_ID,
                    WarehouseInbox.effect_key == request.effect_key,
                    WarehouseInbox.status.in_(["RECEIVED", "PENDING"]),
                )
                .order_by(WarehouseInbox.sequence, WarehouseInbox.id)
                .with_for_update()
            )
        )
        for event in events:
            if (
                event.application_id != request.application_id
                or event.request_hash != request.request_hash
            ):
                self._event_state(event, "CONFLICT", "EVENT_BINDING_CONFLICT")
                self._schedule(s, request)
                continue
            try:
                _binding(s, request, event.external_shipment_id)
            except BusinessError:
                self._event_state(event, "CONFLICT", "EVENT_EXTERNAL_CONFLICT")
                self._schedule(s, request)
                continue
            semantic = {k: v for k, v in event.payload.items() if k != "event_id"}
            peers = s.scalars(
                select(WarehouseInbox).where(
                    WarehouseInbox.id != event.id,
                    WarehouseInbox.effect_key == request.effect_key,
                    WarehouseInbox.sequence == event.sequence,
                    WarehouseInbox.status.in_(["PROCESSED", "STALE"]),
                )
            )
            if any(
                {k: v for k, v in p.payload.items() if k != "event_id"} != semantic for p in peers
            ):
                self._event_state(event, "CONFLICT", "EVENT_SEQUENCE_CONFLICT")
                self._schedule(s, request)
                continue
            if (
                event.payload["status"] == "ACCEPTED"
                and event.sequence != 1
                or event.payload["status"] == "SUCCEEDED"
                and event.sequence < 2
            ):
                self._event_state(event, "CONFLICT", "EVENT_SEQUENCE_CONFLICT")
                self._schedule(s, request)
                continue
            if event.sequence <= request.last_sequence:
                self._event_state(event, "STALE")
                continue
            if event.sequence != request.last_sequence + 1:
                self._event_state(event, "PENDING", "EVENT_SEQUENCE_GAP")
                self._schedule(s, request)
                continue
            if event.payload["status"] == "ACCEPTED":
                try:
                    with s.begin_nested():
                        # Match success' domain -> unique-binding order. An ACCEPTED event
                        # must not hold the provider index while waiting for shared stock.
                        app, fulfillment = OutboxService(None, self.clock)._domain(s, request)
                        _snapshot(request, row, app, fulfillment)
                        _binding(s, request, event.external_shipment_id)
                        request.provider_external_id, request.last_sequence = (
                            event.external_shipment_id,
                            1,
                        )
                        request.updated_at = self.clock()
                        s.flush()
                except (BusinessError, IntegrityError):
                    self._event_state(event, "CONFLICT", "EVENT_EXTERNAL_CONFLICT")
                    self._schedule(s, request)
                else:
                    self._event_state(event, "PROCESSED")
                continue
            try:
                with s.begin_nested():
                    merge_success(
                        s,
                        self.clock,
                        row,
                        request,
                        event.external_shipment_id,
                        event.sequence,
                        evidence={
                            "source": "callback",
                            "inbox_id": event.id,
                            "event_id": event.event_id,
                            "sequence": event.sequence,
                        },
                    )
            except (BusinessError, IntegrityError):
                self._event_state(event, "CONFLICT", "EVENT_BINDING_CONFLICT")
                self._schedule(s, request)
                continue
            self._event_state(event, "PROCESSED")

    def receive_event(self, event_dict):
        try:
            payload = WarehouseEvent.model_validate(event_dict).model_dump()
        except ValidationError:
            raise BusinessError("INVALID_WAREHOUSE_EVENT") from None
        body_hash, now = digest(payload), self.clock()
        with self.sessions.begin() as s:
            created = s.scalar(
                insert(WarehouseInbox)
                .values(
                    id=new_id("inbox"),
                    source=PROVIDER_ID,
                    event_id=payload["event_id"],
                    payload=payload,
                    body_hash=body_hash,
                    sequence=payload["sequence"],
                    effect_key=payload["effect_key"],
                    application_id=payload["application_id"],
                    external_shipment_id=payload["external_shipment_id"],
                    request_hash=payload["request_hash"],
                    status="RECEIVED",
                    created_at=now,
                    updated_at=now,
                )
                .on_conflict_do_nothing(index_elements=["source", "event_id"])
                .returning(WarehouseInbox.id)
            )
            event = s.scalar(
                select(WarehouseInbox).where(
                    WarehouseInbox.source == PROVIDER_ID,
                    WarehouseInbox.event_id == payload["event_id"],
                )
            )
            duplicate = created is None
            if event.body_hash != body_hash:
                s.execute(
                    insert(InboxConflict)
                    .values(
                        id=new_id("conflict"),
                        inbox_id=event.id,
                        original_hash=event.body_hash,
                        new_hash=body_hash,
                        payload=payload,
                        created_at=now,
                    )
                    .on_conflict_do_nothing(index_elements=["inbox_id", "new_hash"])
                )
                return {
                    "inbox_id": event.id,
                    "event_id": event.event_id,
                    "status": "CONFLICT",
                    "error_code": "EVENT_ID_CONFLICT",
                    "duplicate": False,
                }
            event_id = event.id
        # The durable Inbox commit above survives every failure in processing below.
        with self.sessions.begin() as s:
            event = s.get(WarehouseInbox, event_id)
            request = s.scalar(
                select(ShipmentRequest).where(ShipmentRequest.effect_key == event.effect_key)
            )
            if request is None:
                event = _locked(s, WarehouseInbox, event_id)
                self._event_state(event, "PENDING", "UNKNOWN_REQUEST")
            else:
                row, request = self._locked_request(s, request.id)
                self._drain(s, row, request)
            s.flush()
            event = s.get(WarehouseInbox, event_id, populate_existing=True)
            result = {
                "inbox_id": event.id,
                "event_id": event.event_id,
                "status": event.status,
                "duplicate": duplicate,
            }
            if event.error_code:
                result["error_code"] = event.error_code
            return result

    def ensure_pending(self, limit=20):
        require(type(limit) is int and 1 <= limit <= 100, "RECOVERY_LIMIT_INVALID")
        with self.sessions() as s:
            ids = list(
                s.scalars(
                    select(ShipmentRequest.id)
                    .join(WarehouseOutbox, WarehouseOutbox.request_id == ShipmentRequest.id)
                    .where(
                        ShipmentRequest.status != "SUCCEEDED",
                        ~exists(
                            select(ReconciliationTask.id).where(
                                ReconciliationTask.request_id == ShipmentRequest.id
                            )
                        ),
                        or_(
                            (ShipmentRequest.status == "UNKNOWN")
                            & (WarehouseOutbox.status == "BLOCKED"),
                            exists(
                                select(WarehouseInbox.id).where(
                                    WarehouseInbox.status.in_(["RECEIVED", "PENDING", "CONFLICT"]),
                                    WarehouseInbox.effect_key == ShipmentRequest.effect_key,
                                )
                            ),
                        ),
                    )
                    .order_by(ShipmentRequest.id)
                    .limit(limit)
                )
            )
        count = 0
        for identity in ids:
            with self.sessions.begin() as s:
                row, request = self._locked_request(s, identity)
                self._drain(s, row, request)
                previous = _task(s, request.id)
                task = self._schedule(s, request)
                count += int(previous is None and task is not None)
        return count

    def _candidate(self, s, condition):
        hint = s.execute(
            select(ReconciliationTask, WarehouseOutbox)
            .join(WarehouseOutbox, WarehouseOutbox.request_id == ReconciliationTask.request_id)
            .where(condition)
            .order_by(ReconciliationTask.next_attempt_at, ReconciliationTask.id)
            .with_for_update(of=WarehouseOutbox, skip_locked=True)
            .limit(1)
        ).first()
        if hint is None:
            return None
        row = hint[1]
        request = _locked(s, ShipmentRequest, row.request_id)
        task = _locked(s, ReconciliationTask, hint[0].id)
        return row, request, task

    def claim(self, owner):
        require(isinstance(owner, str) and 0 < len(owner.strip()) <= 100, "RECOVERY_OWNER_INVALID")
        with self.sessions.begin() as s:
            selected = self._candidate(
                s,
                (ReconciliationTask.status == "PENDING")
                & (ReconciliationTask.next_attempt_at <= self.clock()),
            )
            if selected is None:
                return None
            row, request, task = selected
            if task.status != "PENDING" or task.next_attempt_at > self.clock():
                return None
            if request.status == "SUCCEEDED":
                task.status, task.updated_at = "DONE", self.clock()
                return None
            if task.deadline_at <= self.clock() or task.cycle_attempts >= self.max_attempts:
                self._exhaust(s, row, request, task, "RECOVERY_BUDGET_EXHAUSTED")
                return None
            now = self.clock()
            task.status, task.owner = "CLAIMED", owner
            task.generation += 1
            task.attempts += 1
            task.cycle_attempts += 1
            task.lease_expires_at = min(
                now + timedelta(seconds=self.lease_seconds), task.deadline_at
            )
            task.updated_at = now
            return ReconciliationClaim(
                task.id,
                request.id,
                row.id,
                owner,
                task.generation,
                task.cycle,
                request.effect_key,
                request.application_id,
                request.request_hash,
            )

    def _guard(self, task, request, row, claim):
        require(
            task is not None
            and task.status == "CLAIMED"
            and task.owner == claim.owner
            and task.generation == claim.generation
            and task.cycle == claim.cycle
            and task.request_id == request.id == claim.request_id
            and row.id == claim.outbox_id
            and task.lease_expires_at > self.clock()
            and task.deadline_at > self.clock()
            and request.effect_key == claim.effect_key
            and request.application_id == claim.application_id
            and request.request_hash == claim.request_hash,
            "RECONCILIATION_LEASE_LOST",
        )

    def check_claim(self, claim):
        require(isinstance(claim, ReconciliationClaim), "RECONCILIATION_LEASE_LOST")
        with self.sessions.begin() as s:
            row, request = self._locked_request(
                s, claim.request_id, missing_code="RECONCILIATION_LEASE_LOST"
            )
            self._guard(_locked(s, ReconciliationTask, claim.task_id), request, row, claim)

    def expire_leases(self, limit=20):
        require(type(limit) is int and 1 <= limit <= 100, "RECOVERY_LIMIT_INVALID")
        changed = 0
        for _ in range(limit):
            with self.sessions.begin() as s:
                now = self.clock()
                selected = self._candidate(
                    s,
                    or_(
                        (ReconciliationTask.status == "CLAIMED")
                        & (ReconciliationTask.lease_expires_at <= now),
                        (ReconciliationTask.status == "PENDING")
                        & (ReconciliationTask.deadline_at <= now),
                    ),
                )
                if selected is None:
                    break
                row, request, task = selected
                if task.status not in {"CLAIMED", "PENDING"}:
                    continue
                if task.deadline_at <= now or task.cycle_attempts >= self.max_attempts:
                    self._exhaust(s, row, request, task, "RECOVERY_BUDGET_EXHAUSTED")
                else:
                    task.status, task.owner, task.lease_expires_at = "PENDING", None, None
                    task.next_attempt_at, task.updated_at = now, now
                    task.last_error_code = "QUERY_LEASE_EXPIRED"
                changed += 1
        return changed

    def _settle_inbox(self, s, request):
        for event in s.scalars(
            select(WarehouseInbox)
            .where(
                WarehouseInbox.effect_key == request.effect_key,
                WarehouseInbox.status.in_(["RECEIVED", "PENDING"]),
            )
            .with_for_update()
        ):
            if (
                event.application_id != request.application_id
                or event.request_hash != request.request_hash
                or event.external_shipment_id != request.external_shipment_id
            ):
                self._event_state(event, "CONFLICT", "EVENT_BINDING_CONFLICT")
            elif (
                event.payload["status"] == "ACCEPTED"
                and event.sequence != 1
                or event.payload["status"] == "SUCCEEDED"
                and event.sequence < 2
            ):
                self._event_state(event, "CONFLICT", "EVENT_SEQUENCE_CONFLICT")
            elif event.sequence <= request.last_sequence:
                self._event_state(
                    event, "PROCESSED" if event.payload["status"] == "SUCCEEDED" else "STALE"
                )

    def finish(
        self, claim, outcome, *, external_shipment_id=None, provider_sequence=None, error_code=None
    ):
        require(isinstance(claim, ReconciliationClaim), "RECONCILIATION_LEASE_LOST")
        require(
            outcome in {"SUCCEEDED", "RETRYABLE", "UNKNOWN", "NOT_FOUND", "FAILED"},
            "RECOVERY_OUTCOME_INVALID",
        )
        require(
            error_code is None
            or isinstance(error_code, str)
            and 0 < len(error_code) <= 100
            and all(c.isupper() or c.isdigit() or c == "_" for c in error_code),
            "RECOVERY_OUTCOME_INVALID",
        )
        with self.sessions.begin() as s:
            row, request = self._locked_request(
                s, claim.request_id, missing_code="RECONCILIATION_LEASE_LOST"
            )
            task = _locked(s, ReconciliationTask, claim.task_id)
            self._guard(task, request, row, claim)
            deadline = min(task.lease_expires_at, task.deadline_at)
            receipt = None
            if outcome == "SUCCEEDED":
                try:
                    with s.begin_nested():
                        receipt = merge_success(
                            s,
                            self.clock,
                            row,
                            request,
                            external_shipment_id,
                            provider_sequence,
                            evidence={
                                "source": "query",
                                "task_id": task.id,
                                "generation": claim.generation,
                                "cycle": claim.cycle,
                                "sequence": provider_sequence,
                            },
                        )
                except (BusinessError, IntegrityError):
                    self._exhaust(s, row, request, task, "RECOVERY_BINDING_CONFLICT")
                else:
                    self._settle_inbox(s, request)
            else:
                now = self.clock()
                code = error_code or "QUERY_" + outcome
                if task.cycle_attempts >= self.max_attempts or now >= task.deadline_at:
                    self._exhaust(s, row, request, task, code)
                else:
                    task.status, task.owner, task.lease_expires_at = "PENDING", None, None
                    task.next_attempt_at = min(
                        now + timedelta(seconds=2 ** (task.cycle_attempts - 1)), task.deadline_at
                    )
                    task.updated_at, task.last_error_code = now, code
            s.flush()
            require(self.clock() < deadline, "RECONCILIATION_LEASE_LOST")
            result = {
                "request": shipment_request_view(request),
                "outbox": outbox_view(row),
                "reconciliation": _task_view(task, request),
            }
            if receipt:
                result["receipt"] = receipt
            return result

    def _manual(self, actor, identity, expected_generation, key, reason, action):
        validate_actor(actor)
        require(
            actor.role == "support" and actor.source in {"server_session", "test_fixture"},
            "FORBIDDEN",
        )
        require(
            type(expected_generation) is int
            and expected_generation >= 0
            and isinstance(key, str)
            and 0 < len(key) <= 100
            and key.strip()
            and isinstance(reason, str)
            and 0 < len(reason.strip()) <= 500,
            "INVALID_RECOVERY_COMMAND",
        )
        reason = reason.strip()
        request_hash = digest(
            {
                "action": action,
                "identity": identity,
                "expected_generation": expected_generation,
                "reason": reason,
            }
        )
        with self.sessions.begin() as s:
            s.execute(
                text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 4))"),
                {"key": actor.actor_id + ":" + key},
            )
            prior = s.scalar(
                select(RecoveryCommand).where(
                    RecoveryCommand.actor_id == actor.actor_id,
                    RecoveryCommand.idempotency_key == key,
                )
            )
            if prior:
                require(prior.request_hash == request_hash, "IDEMPOTENCY_CONFLICT")
                return prior.receipt
            if action == "redrive":
                row = _locked(s, WarehouseOutbox, identity)
                require(row is not None, "NOT_FOUND")
                request = _locked(s, ShipmentRequest, row.request_id)
            else:
                row, request = self._locked_request(s, identity)
            require(row.generation == expected_generation, "GENERATION_CONFLICT")
            require(request.status != "SUCCEEDED", "RECOVERY_NOT_ALLOWED")
            task = _task(s, request.id)
            now = self.clock()
            if action == "redrive":
                attempts = list(
                    s.scalars(select(OutboxAttempt).where(OutboxAttempt.outbox_id == row.id))
                )
                safe = (
                    row.status == "DEAD"
                    and request.status in {"FAILED", "MANUAL_REVIEW"}
                    and request.provider_external_id is None
                    and request.last_sequence == 0
                    and attempts
                    and all(a.outcome in {"FAILED", "RETRYABLE"} for a in attempts)
                )
                require(safe, "REDRIVE_NOT_ALLOWED")
                require(row.redrive_count < self.max_cycles, "RECOVERY_LIMIT_EXCEEDED")
                app, fulfillment = OutboxService(None, self.clock)._domain(s, request)
                require(app.status == "REPLACEMENT_PENDING", "REDRIVE_NOT_ALLOWED")
                row.redrive_count += 1
                row.cycle_attempts = 0
                row.status, row.next_attempt_at, row.last_error_code = "PENDING", now, None
                request.status, request.updated_at = "PENDING", now
                fulfillment.shipment_status, fulfillment.updated_at = "PENDING", now
                if task:
                    task.status, task.owner, task.lease_expires_at = "DONE", None, None
                    task.generation += 1
                    task.updated_at = now
            else:
                if task is None:
                    task = self._schedule(s, request)
                elif task.status in {"EXHAUSTED", "DONE"}:
                    require(task.cycle < self.max_cycles, "RECOVERY_LIMIT_EXCEEDED")
                    task.cycle += 1
                    task.cycle_attempts = 0
                    task.deadline_at = now + timedelta(seconds=self.deadline_seconds)
                task.generation += 1
                task.status, task.owner, task.lease_expires_at = "PENDING", None, None
                task.next_attempt_at, task.updated_at = now, now
                task.last_error_code = None
            if row.status == "CLAIMED":
                attempt = s.scalar(
                    select(OutboxAttempt)
                    .where(
                        OutboxAttempt.outbox_id == row.id,
                        OutboxAttempt.generation == row.generation,
                        OutboxAttempt.finished_at.is_(None),
                    )
                    .with_for_update()
                )
                if attempt:
                    attempt.finished_at, attempt.outcome, attempt.error_code = (
                        now,
                        "UNKNOWN",
                        "MANUAL_RECONCILE",
                    )
                row.status, row.lease_owner, row.lease_expires_at = "BLOCKED", None, None
                request.status, request.updated_at = "UNKNOWN", now
                app, fulfillment = OutboxService(None, self.clock)._domain(s, request)
                if app.status == "REPLACEMENT_PENDING":
                    fulfillment.shipment_status, fulfillment.updated_at = "UNKNOWN", now
            row.generation += 1
            row.updated_at = now
            receipt = {
                "command_id": new_id("recovercmd"),
                "action": action,
                "request_id": request.id,
                "outbox_id": row.id,
                "outbox_generation": row.generation,
                "status": task.status if action == "reconcile" else row.status,
                "cycle": task.cycle if action == "reconcile" else row.redrive_count,
            }
            if task:
                receipt["task_id"] = task.id
            s.add(
                RecoveryCommand(
                    id=receipt["command_id"],
                    actor_id=actor.actor_id,
                    actor_role=actor.role,
                    actor_source=actor.source,
                    idempotency_key=key,
                    action=action,
                    request_id=request.id,
                    outbox_id=row.id,
                    expected_generation=expected_generation,
                    request_hash=request_hash,
                    receipt=receipt,
                    reason=reason,
                    created_at=now,
                )
            )
            s.flush()
            return receipt

    def manual_reconcile(self, actor, request_id, expected_generation, idempotency_key, reason):
        return self._manual(
            actor, request_id, expected_generation, idempotency_key, reason, "reconcile"
        )

    def redrive(self, actor, outbox_id, expected_generation, key, reason):
        return self._manual(actor, outbox_id, expected_generation, key, reason, "redrive")

    def get_status(self, actor, request_id):
        validate_actor(actor)
        with self.sessions.begin() as s:
            s.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
            request = s.get(ShipmentRequest, request_id)
            require(request is not None, "NOT_FOUND")
            app = s.get(Application, request.application_id)
            require(app is not None, "RECOVERY_INVARIANT")
            line = s.get(OrderLine, app.line_id)
            require(line is not None, "RECOVERY_INVARIANT")
            _owned(actor, s.get(Order, line.order_id))
            row = s.scalar(select(WarehouseOutbox).where(WarehouseOutbox.request_id == request.id))
            require(row is not None, "RECOVERY_INVARIANT")
            task = s.scalar(
                select(ReconciliationTask).where(ReconciliationTask.request_id == request.id)
            )
            inbox = list(
                s.scalars(
                    select(WarehouseInbox)
                    .where(WarehouseInbox.effect_key == request.effect_key)
                    .order_by(WarehouseInbox.sequence, WarehouseInbox.id)
                )
            )
            conflicts = s.scalars(
                select(InboxConflict).where(InboxConflict.inbox_id.in_([e.id for e in inbox]))
            )
            attempts = s.scalars(
                select(OutboxAttempt)
                .where(OutboxAttempt.outbox_id == row.id)
                .order_by(OutboxAttempt.generation)
            )
            result = {
                "request": shipment_request_view(request),
                "outbox": outbox_view(row),
                "attempts": [attempt_view(a) for a in attempts],
                "reconciliation": _task_view(task, request),
                "inbox": [_inbox_view(e) for e in inbox],
                "conflicts": [
                    {k: v for k, v in columns(c).items() if k != "payload"} for c in conflicts
                ],
            }
            if actor.role == "support":
                result["management"] = {
                    "outbox_generation": row.generation,
                    "redrive_count": row.redrive_count,
                }
            return result
