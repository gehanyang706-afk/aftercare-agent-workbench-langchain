"""Fenced, bounded warehouse delivery state; networking belongs outside these transactions."""

from copy import deepcopy
from dataclasses import dataclass
from datetime import timedelta

from sqlalchemy import select, text

from aftercare.domain.contracts import utc_now
from aftercare.domain.fulfillment import Actor
from aftercare.domain.rules import require
from aftercare.repositories.models import (
    Application,
    Order,
    OrderLine,
    OutboxAttempt,
    ReconciliationTask,
    ShipmentRequest,
    WarehouseOutbox,
)
from aftercare.services.business import columns, digest, new_id
from aftercare.services.fulfillment import (
    _invariants,
    _locked,
    _owned,
    _resources,
    _stock,
    shipment_request_view,
    validate_actor,
)

OUTBOX_ACTOR = Actor("warehouse-outbox", "warehouse", "trusted_adapter")


@dataclass(frozen=True)
class OutboxClaim:
    outbox_id: str
    request_id: str
    owner: str
    generation: int
    attempts: int
    payload: dict
    request_hash: str

    @property
    def provider_payload(self):
        return {**deepcopy(self.payload), "request_hash": self.request_hash}


def outbox_view(row):
    allowed = {
        "id",
        "status",
        "attempts",
        "next_attempt_at",
        "last_error_code",
        "created_at",
        "updated_at",
    }
    return {key: value for key, value in columns(row).items() if key in allowed}


def attempt_view(row):
    allowed = {"generation", "outcome", "error_code", "started_at", "finished_at"}
    return {key: value for key, value in columns(row).items() if key in allowed}


class OutboxService:
    def __init__(self, sessions, clock=utc_now, lease_seconds=15, max_attempts=3):
        if not 1 <= lease_seconds <= 300 or not 1 <= max_attempts <= 10:
            raise ValueError("INVALID_OUTBOX_LIMITS")
        self.sessions, self.clock = sessions, clock
        self.lease_seconds, self.max_attempts = lease_seconds, max_attempts

    def _domain(self, s, request):
        _order, app, line = _resources(s, request.application_id, OUTBOX_ACTOR)
        fulfillment, reservation, stock = _stock(s, app)
        _invariants(s, app, line, reservation, stock)
        return app, fulfillment

    def _snapshot_valid(self, row, request, app, fulfillment):
        expected = {
            "effect_key": "exchange:" + app.id,
            "application_id": app.id,
            "variant_id": app.target_variant,
            "quantity": app.quantity,
        }
        return (
            row.request_id == request.id
            and row.payload == request.payload == expected
            and request.request_hash == digest(expected)
            and request.effect_key == expected["effect_key"] == fulfillment.effect_key
            and request.status == "PENDING"
            and app.status == "REPLACEMENT_PENDING"
            and fulfillment.shipment_status == "PENDING"
        )

    def _stop(self, row, request, app, fulfillment, *, status, outbox_status, error_code, now):
        request.status, request.updated_at = status, now
        if app.status == "REPLACEMENT_PENDING":
            fulfillment.shipment_status, fulfillment.updated_at = status, now
        row.status, row.last_error_code, row.updated_at = outbox_status, error_code, now
        row.lease_owner, row.lease_expires_at = None, None

    def claim(self, owner):
        require(isinstance(owner, str) and 0 < len(owner.strip()) <= 100, "OUTBOX_OWNER_INVALID")
        with self.sessions.begin() as s:
            now = self.clock()
            row = s.scalar(
                select(WarehouseOutbox)
                .where(WarehouseOutbox.status == "PENDING", WarehouseOutbox.next_attempt_at <= now)
                .order_by(WarehouseOutbox.next_attempt_at, WarehouseOutbox.id)
                .with_for_update(skip_locked=True)
                .limit(1)
            )
            if row is None:
                return None
            request = _locked(s, ShipmentRequest, row.request_id)
            require(request is not None, "OUTBOX_INVARIANT")
            app, fulfillment = self._domain(s, request)
            if row.cycle_attempts >= self.max_attempts or not self._snapshot_valid(
                row, request, app, fulfillment
            ):
                self._stop(
                    row,
                    request,
                    app,
                    fulfillment,
                    status="MANUAL_REVIEW",
                    outbox_status="DEAD",
                    error_code="ATTEMPTS_EXHAUSTED"
                    if row.cycle_attempts >= self.max_attempts
                    else "PAYLOAD_INVARIANT",
                    now=now,
                )
                return None
            now = self.clock()
            row.status, row.lease_owner = "CLAIMED", owner
            row.generation += 1
            row.attempts += 1
            row.cycle_attempts += 1
            row.lease_expires_at = now + timedelta(seconds=self.lease_seconds)
            row.updated_at = now
            s.add(
                OutboxAttempt(
                    id=new_id("attempt"),
                    outbox_id=row.id,
                    generation=row.generation,
                    owner=owner,
                    started_at=now,
                    send_state="NOT_STARTED",
                )
            )
            return OutboxClaim(
                row.id,
                request.id,
                owner,
                row.generation,
                row.attempts,
                deepcopy(row.payload),
                request.request_hash,
            )

    def _attempt(self, s, row):
        attempt = s.scalar(
            select(OutboxAttempt)
            .where(OutboxAttempt.outbox_id == row.id, OutboxAttempt.generation == row.generation)
            .with_for_update()
        )
        require(attempt is not None and attempt.finished_at is None, "OUTBOX_INVARIANT")
        return attempt

    def _guard(self, row, claim):
        require(
            row is not None
            and row.status == "CLAIMED"
            and row.lease_owner == claim.owner
            and row.generation == claim.generation
            and row.request_id == claim.request_id
            and row.lease_expires_at > self.clock(),
            "OUTBOX_LEASE_LOST",
        )

    def check_claim(self, claim):
        require(isinstance(claim, OutboxClaim), "OUTBOX_LEASE_LOST")
        valid = False
        with self.sessions.begin() as s:
            row = _locked(s, WarehouseOutbox, claim.outbox_id)
            self._guard(row, claim)
            request = _locked(s, ShipmentRequest, row.request_id)
            require(request is not None, "OUTBOX_INVARIANT")
            app, fulfillment = self._domain(s, request)
            self._guard(row, claim)
            valid = (
                self._snapshot_valid(row, request, app, fulfillment)
                and claim.payload == row.payload
                and claim.request_hash == request.request_hash
            )
            if not valid:
                now = self.clock()
                self._stop(
                    row,
                    request,
                    app,
                    fulfillment,
                    status="MANUAL_REVIEW",
                    outbox_status="DEAD",
                    error_code="PAYLOAD_INVARIANT",
                    now=now,
                )
                attempt = self._attempt(s, row)
                attempt.finished_at, attempt.outcome, attempt.error_code = (
                    now,
                    "UNKNOWN",
                    "PAYLOAD_INVARIANT",
                )
        require(valid, "OUTBOX_LEASE_LOST")

    def begin_delivery(self, claim):
        """Commit one sending permission; the caller may open a socket only after return."""
        require(isinstance(claim, OutboxClaim), "OUTBOX_LEASE_LOST")
        valid = False
        with self.sessions.begin() as s:
            row = _locked(s, WarehouseOutbox, claim.outbox_id)
            self._guard(row, claim)
            request = _locked(s, ShipmentRequest, row.request_id)
            require(request is not None, "OUTBOX_INVARIANT")
            app, fulfillment = self._domain(s, request)
            self._guard(row, claim)
            attempt = self._attempt(s, row)
            self._guard(row, claim)
            valid = (
                self._snapshot_valid(row, request, app, fulfillment)
                and claim.payload == row.payload
                and claim.request_hash == request.request_hash
            )
            if not valid:
                now = self.clock()
                self._stop(
                    row,
                    request,
                    app,
                    fulfillment,
                    status="MANUAL_REVIEW",
                    outbox_status="DEAD",
                    error_code="PAYLOAD_INVARIANT",
                    now=now,
                )
                attempt.finished_at, attempt.outcome, attempt.error_code = (
                    now,
                    "UNKNOWN",
                    "PAYLOAD_INVARIANT",
                )
            elif request.provider_external_id is not None or request.last_sequence > 0:
                now, deadline = self.clock(), row.lease_expires_at
                self._stop(
                    row,
                    request,
                    app,
                    fulfillment,
                    status="UNKNOWN",
                    outbox_status="BLOCKED",
                    error_code="PROVIDER_ALREADY_ACCEPTED",
                    now=now,
                )
                attempt.finished_at, attempt.outcome, attempt.error_code = (
                    now,
                    "UNKNOWN",
                    "PROVIDER_ALREADY_ACCEPTED",
                )
                s.flush()
                require(self.clock() < deadline, "OUTBOX_LEASE_LOST")
                valid = False
            else:
                require(attempt.send_state == "NOT_STARTED", "DELIVERY_ALREADY_STARTED")
                attempt.send_state, attempt.send_started_at = "STARTED", self.clock()
                s.flush()
                self._guard(row, claim)
        require(valid, "OUTBOX_LEASE_LOST")

    def expire_leases(self, limit=20):
        require(isinstance(limit, int) and 1 <= limit <= 100, "OUTBOX_LIMIT_INVALID")
        expired = 0
        # Commit each request separately: never hold one order's stock while locking another order.
        for _ in range(limit):
            with self.sessions.begin() as s:
                now = self.clock()
                row = s.scalar(
                    select(WarehouseOutbox)
                    .where(
                        WarehouseOutbox.status == "CLAIMED", WarehouseOutbox.lease_expires_at <= now
                    )
                    .order_by(WarehouseOutbox.lease_expires_at, WarehouseOutbox.id)
                    .with_for_update(skip_locked=True)
                    .limit(1)
                )
                if row is None:
                    break
                request = _locked(s, ShipmentRequest, row.request_id)
                require(request is not None, "OUTBOX_INVARIANT")
                app, fulfillment = self._domain(s, request)
                valid = self._snapshot_valid(row, request, app, fulfillment)
                attempt = self._attempt(s, row)
                if (
                    valid
                    and attempt.send_state == "NOT_STARTED"
                    and request.provider_external_id is None
                    and request.last_sequence == 0
                ):
                    attempt.finished_at, attempt.outcome, attempt.error_code = (
                        now,
                        "RETRYABLE",
                        "HTTP_NOT_STARTED",
                    )
                    row.generation += 1
                    if row.cycle_attempts >= self.max_attempts:
                        self._stop(
                            row,
                            request,
                            app,
                            fulfillment,
                            status="MANUAL_REVIEW",
                            outbox_status="DEAD",
                            error_code="ATTEMPTS_EXHAUSTED",
                            now=now,
                        )
                    else:
                        row.status, row.lease_owner, row.lease_expires_at = "PENDING", None, None
                        row.next_attempt_at, row.updated_at = now, now
                        row.last_error_code = "HTTP_NOT_STARTED"
                    expired += 1
                    continue
                code = "LEASE_EXPIRED" if valid else "PAYLOAD_INVARIANT"
                self._stop(
                    row,
                    request,
                    app,
                    fulfillment,
                    status="UNKNOWN" if valid else "MANUAL_REVIEW",
                    outbox_status="BLOCKED" if valid else "DEAD",
                    error_code=code,
                    now=now,
                )
                attempt.finished_at, attempt.outcome, attempt.error_code = now, "UNKNOWN", code
                expired += 1
        return expired

    def finish(self, claim, outcome, *, external_shipment_id=None, error_code=None):
        require(isinstance(claim, OutboxClaim), "OUTBOX_LEASE_LOST")
        require(
            outcome in {"SUCCEEDED", "RETRYABLE", "FAILED", "UNKNOWN"}, "OUTBOX_OUTCOME_INVALID"
        )
        require(
            error_code is None
            or isinstance(error_code, str)
            and 0 < len(error_code) <= 100
            and all(c.isupper() or c.isdigit() or c == "_" for c in error_code),
            "OUTBOX_OUTCOME_INVALID",
        )
        if outcome == "SUCCEEDED":
            require(
                isinstance(external_shipment_id, str)
                and 0 < len(external_shipment_id.strip()) <= 100,
                "OUTBOX_OUTCOME_INVALID",
            )
            external_shipment_id = external_shipment_id.strip()
        with self.sessions.begin() as s:
            row = _locked(s, WarehouseOutbox, claim.outbox_id)
            self._guard(row, claim)
            request = _locked(s, ShipmentRequest, row.request_id)
            require(request is not None, "OUTBOX_INVARIANT")
            s.scalar(
                select(ReconciliationTask)
                .where(ReconciliationTask.request_id == request.id)
                .with_for_update()
            )
            app, fulfillment = self._domain(s, request)
            self._guard(row, claim)
            now, lease_deadline = self.clock(), row.lease_expires_at
            attempt = self._attempt(s, row)
            require(attempt.send_state in {"STARTED", "LEGACY_UNKNOWN"}, "DELIVERY_NOT_STARTED")
            if not self._snapshot_valid(row, request, app, fulfillment) or (
                claim.payload != row.payload or claim.request_hash != request.request_hash
            ):
                self._stop(
                    row,
                    request,
                    app,
                    fulfillment,
                    status="MANUAL_REVIEW",
                    outbox_status="DEAD",
                    error_code="PAYLOAD_INVARIANT",
                    now=now,
                )
                attempt.finished_at, attempt.outcome, attempt.error_code = (
                    now,
                    "UNKNOWN",
                    "PAYLOAD_INVARIANT",
                )
                return {"request": shipment_request_view(request), "outbox": outbox_view(row)}
            receipt = None
            if outcome == "SUCCEEDED":
                from aftercare.services.recovery import merge_success

                receipt = merge_success(
                    s,
                    self.clock,
                    row,
                    request,
                    external_shipment_id,
                    2,
                    evidence={
                        "source": "delivery",
                        "outbox_id": row.id,
                        "generation": claim.generation,
                    },
                    fence=False,
                )
            elif outcome == "RETRYABLE" and row.cycle_attempts < self.max_attempts:
                row.status, row.last_error_code = "PENDING", error_code or "PROVIDER_RETRYABLE"
                row.next_attempt_at = now + timedelta(seconds=2 ** (row.cycle_attempts - 1))
                row.lease_owner, row.lease_expires_at = None, None
            else:
                status = {"UNKNOWN": "UNKNOWN", "FAILED": "FAILED", "RETRYABLE": "MANUAL_REVIEW"}[
                    outcome
                ]
                self._stop(
                    row,
                    request,
                    app,
                    fulfillment,
                    status=status,
                    outbox_status="BLOCKED" if outcome == "UNKNOWN" else "DEAD",
                    error_code=error_code
                    or ("ATTEMPTS_EXHAUSTED" if outcome == "RETRYABLE" else "PROVIDER_" + outcome),
                    now=now,
                )
            request.updated_at, row.updated_at = now, now
            attempt.finished_at, attempt.outcome, attempt.error_code = (
                now,
                outcome,
                row.last_error_code,
            )
            require(self.clock() < lease_deadline, "OUTBOX_LEASE_LOST")
            s.flush()
            require(self.clock() < lease_deadline, "OUTBOX_LEASE_LOST")
            result = {"request": shipment_request_view(request), "outbox": outbox_view(row)}
            if receipt is not None:
                result["receipt"] = receipt
            return result

    def get(self, actor, request_id):
        validate_actor(actor)
        with self.sessions.begin() as s:
            s.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
            request = s.get(ShipmentRequest, request_id)
            require(request is not None, "NOT_FOUND")
            app = s.get(Application, request.application_id)
            require(app is not None, "OUTBOX_INVARIANT")
            line = s.get(OrderLine, app.line_id)
            require(line is not None, "OUTBOX_INVARIANT")
            _owned(actor, s.get(Order, line.order_id))
            row = s.scalar(select(WarehouseOutbox).where(WarehouseOutbox.request_id == request.id))
            require(row is not None, "OUTBOX_INVARIANT")
            attempts = s.scalars(
                select(OutboxAttempt)
                .where(OutboxAttempt.outbox_id == row.id)
                .order_by(OutboxAttempt.generation)
            )
            return {
                "request": shipment_request_view(request),
                "outbox": outbox_view(row),
                "attempts": [attempt_view(attempt) for attempt in attempts],
            }
