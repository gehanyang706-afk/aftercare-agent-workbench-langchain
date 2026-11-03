"""Versioned exchange commands in one PostgreSQL transaction.

Lock order: order -> application -> line -> fulfillment -> reservation -> inventory.
No HTTP, model calls, automatic retries or external I/O inside these transactions.
"""

from pydantic import ValidationError
from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError

from aftercare.domain.contracts import BusinessError, utc_now
from aftercare.domain.fulfillment import (
    DATA_MODELS,
    ROLES,
    TRANSITIONS,
    Actor,
    FulfillmentCommandInput,
)
from aftercare.domain.rules import require
from aftercare.repositories.models import (
    Application,
    ExchangeFulfillment,
    FulfillmentAudit,
    FulfillmentCommand,
    Inventory,
    Order,
    OrderLine,
    Reservation,
    ShipmentRequest,
    WarehouseOutbox,
)
from aftercare.services.business import columns, digest, new_id


def shipment_request_view(row):
    allowed = {
        "id",
        "application_id",
        "effect_key",
        "request_hash",
        "status",
        "external_shipment_id",
        "triggered_by",
        "created_at",
        "updated_at",
    }
    return {key: value for key, value in columns(row).items() if key in allowed}


def validate_actor(actor):
    require(actor is not None, "ACTOR_REQUIRED")
    require(
        isinstance(actor, Actor)
        and isinstance(actor.actor_id, str)
        and 0 < len(actor.actor_id.strip()) <= 100
        and isinstance(actor.role, str)
        and isinstance(actor.source, str)
        and actor.role in {"customer", "support", "warehouse"}
        and actor.source in {"test_fixture", "server_session", "trusted_adapter"},
        "ACTOR_INVALID",
    )


def _owned(actor, order):
    require(order is not None, "NOT_FOUND")
    require(actor.role != "customer" or order.customer_id == actor.actor_id, "NOT_FOUND")


def _locked(s, model, identity):
    return s.scalar(
        select(model)
        .where(model.__mapper__.primary_key[0] == identity)
        .with_for_update()
        .execution_options(populate_existing=True)
    )


def _resources(s, application_id, actor):
    # These foreign-key relationships are immutable through application commands.
    initial = s.get(Application, application_id)
    require(initial is not None, "NOT_FOUND")
    line_info = s.get(OrderLine, initial.line_id)
    require(line_info is not None, "INVENTORY_INVARIANT")
    order_info = s.get(Order, line_info.order_id)
    _owned(actor, order_info)
    order = _locked(s, Order, line_info.order_id)
    _owned(actor, order)
    app = _locked(s, Application, application_id)
    require(app is not None and app.line_id == line_info.id, "INVENTORY_INVARIANT")
    require(app.action == "exchange", "UNSUPPORTED_APPLICATION")
    line = _locked(s, OrderLine, app.line_id)
    require(line is not None and line.order_id == order.id, "INVENTORY_INVARIANT")
    return order, app, line


def _stock(s, app):
    fulfillment = _locked(s, ExchangeFulfillment, app.id)
    reservation = _locked(s, Reservation, app.id)
    require(
        fulfillment is not None
        and reservation is not None
        and reservation.variant_id == app.target_variant
        and reservation.quantity == app.quantity,
        "INVENTORY_INVARIANT",
    )
    stock = _locked(s, Inventory, reservation.variant_id)
    require(stock is not None, "INVENTORY_INVARIANT")
    return fulfillment, reservation, stock


def _invariants(s, app, line, reservation, stock):
    expected = (
        "RELEASED"
        if app.status in {"REJECTED", "WITHDRAWN"}
        else "CONSUMED"
        if app.status in {"REPLACEMENT_SHIPPED", "COMPLETED"}
        else "RESERVED"
    )
    require(
        reservation.status == expected
        and stock.available >= 0
        and stock.reserved >= 0
        and 0 <= line.occupied <= line.quantity,
        "INVENTORY_INVARIANT",
    )
    reserved = s.scalar(
        select(func.coalesce(func.sum(Reservation.quantity), 0)).where(
            Reservation.variant_id == stock.variant_id, Reservation.status == "RESERVED"
        )
    )
    occupied = s.scalar(
        select(func.coalesce(func.sum(Application.quantity), 0)).where(
            Application.line_id == line.id, Application.status.not_in(["REJECTED", "WITHDRAWN"])
        )
    )
    require(stock.reserved == reserved and line.occupied == occupied, "INVENTORY_INVARIANT")


class FulfillmentService:
    def __init__(self, sessions, clock=utc_now, *, async_delivery=False):
        self.sessions, self.clock = sessions, clock
        self.async_delivery = async_delivery

    def get(self, actor, application_id):
        validate_actor(actor)
        with self.sessions.begin() as s:
            # A consistent read spanning application, fulfillment and audit; no row write lock.
            s.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
            app = s.get(Application, application_id)
            require(app is not None, "NOT_FOUND")
            line = s.get(OrderLine, app.line_id)
            require(line is not None, "INVENTORY_INVARIANT")
            _owned(actor, s.get(Order, line.order_id))
            require(app.action == "exchange", "UNSUPPORTED_APPLICATION")
            fulfillment = s.get(ExchangeFulfillment, app.id)
            require(fulfillment is not None, "INVENTORY_INVARIANT")
            history = s.scalars(
                select(FulfillmentAudit)
                .where(FulfillmentAudit.application_id == app.id)
                .order_by(FulfillmentAudit.to_version)
            )
            result = {
                "application_id": app.id,
                "status": app.status,
                "version": app.version,
                "fulfillment": columns(fulfillment),
                "history": [columns(row) for row in history],
            }
            shipment = s.scalar(
                select(ShipmentRequest).where(ShipmentRequest.application_id == app.id)
            )
            if shipment is not None:
                result["shipment_request"] = shipment_request_view(shipment)
            return result

    def execute(self, actor, application_id, command: FulfillmentCommandInput, *, fault=None):
        validate_actor(actor)
        require(isinstance(command, FulfillmentCommandInput), "INVALID_COMMAND_DATA")
        require(actor.role == ROLES[command.command], "FORBIDDEN")
        require(
            (command.command == "confirm_shipment") == (actor.source == "trusted_adapter"),
            "FORBIDDEN",
        )
        try:
            data = DATA_MODELS[command.command].model_validate(command.data).model_dump()
        except ValidationError:
            raise BusinessError("INVALID_COMMAND_DATA") from None
        if fault:
            require(
                fault == "before_commit"
                and self.sessions.kw["bind"].url.database == "aftercare_test",
                "TEST_FAULT_DENIED",
            )
        request_hash = digest(
            {
                "application_id": application_id,
                "command": command.command,
                "expected_version": command.expected_version,
                "data": data,
            }
        )
        try:
            with self.sessions.begin() as s:
                order, app, line = _resources(s, application_id, actor)
                if command.command == "confirm_shipment":
                    async_request = s.scalar(
                        select(ShipmentRequest.id).where(ShipmentRequest.application_id == app.id)
                    )
                    require(
                        async_request is None or actor.actor_id == "warehouse-outbox",
                        "ASYNC_DELIVERY_REQUIRED",
                    )
                prior = s.scalar(
                    select(FulfillmentCommand).where(
                        FulfillmentCommand.application_id == app.id,
                        FulfillmentCommand.actor_id == actor.actor_id,
                        FulfillmentCommand.actor_role == actor.role,
                        FulfillmentCommand.idempotency_key == command.idempotency_key,
                    )
                )
                if prior:
                    require(prior.request_hash == request_hash, "IDEMPOTENCY_CONFLICT")
                    return prior.receipt
                require(app.version == command.expected_version, "VERSION_CONFLICT")
                allowed, target = TRANSITIONS[command.command]
                require(app.status in allowed, "ILLEGAL_TRANSITION")
                fulfillment, reservation, stock = _stock(s, app)
                _invariants(s, app, line, reservation, stock)
                old_state, old_version = app.status, app.version
                if command.command in {"reject", "withdraw"}:
                    require(
                        line.occupied >= app.quantity and stock.reserved >= app.quantity,
                        "INVENTORY_INVARIANT",
                    )
                    line.occupied -= app.quantity
                    stock.reserved -= app.quantity
                    stock.available += app.quantity
                    stock.version += 1
                    order.version += 1
                    reservation.status = "RELEASED"
                elif command.command == "submit_return":
                    fulfillment.return_carrier = data["carrier"]
                    fulfillment.return_tracking = data["tracking_number"]
                elif command.command == "receive":
                    fulfillment.received_note = data["note"]
                    fulfillment.received_at = self.clock()
                elif command.command == "request_shipment":
                    require(fulfillment.effect_key is None, "INVENTORY_INVARIANT")
                    fulfillment.effect_key = "exchange:" + app.id
                    fulfillment.shipment_status = "PENDING"
                    if self.async_delivery:
                        now = self.clock()
                        payload = {
                            "effect_key": fulfillment.effect_key,
                            "application_id": app.id,
                            "variant_id": app.target_variant,
                            "quantity": app.quantity,
                        }
                        shipment = ShipmentRequest(
                            id=new_id("shipreq"),
                            application_id=app.id,
                            effect_key=fulfillment.effect_key,
                            payload=payload,
                            request_hash=digest(payload),
                            status="PENDING",
                            triggered_by={
                                "actor_id": actor.actor_id,
                                "role": actor.role,
                                "source": actor.source,
                            },
                            created_at=now,
                            updated_at=now,
                        )
                        s.add(shipment)
                        s.flush()
                        s.add(
                            WarehouseOutbox(
                                id=new_id("outbox"),
                                request_id=shipment.id,
                                payload=dict(payload),
                                status="PENDING",
                                next_attempt_at=now,
                                created_at=now,
                                updated_at=now,
                            )
                        )
                elif command.command == "confirm_shipment":
                    require(
                        fulfillment.effect_key is not None
                        and (
                            fulfillment.shipment_status in {"PENDING", "UNKNOWN"}
                            or actor.actor_id == "warehouse-outbox"
                            and fulfillment.shipment_status in {"MANUAL_REVIEW", "FAILED"}
                        ),
                        "INVENTORY_INVARIANT",
                    )
                    fulfillment.external_shipment_id = data["external_shipment_id"]
                    fulfillment.shipment_status = "SUCCEEDED"
                    require(stock.reserved >= app.quantity, "INVENTORY_INVARIANT")
                    stock.reserved -= app.quantity
                    stock.version += 1
                    order.version += 1
                    reservation.status = "CONSUMED"
                app.status, app.version = target, app.version + 1
                fulfillment.updated_at = self.clock()
                receipt = {
                    "command_id": new_id("cmd"),
                    "application_id": app.id,
                    "command": command.command,
                    "status": app.status,
                    "version": app.version,
                }
                s.add(
                    FulfillmentCommand(
                        id=receipt["command_id"],
                        application_id=app.id,
                        actor_id=actor.actor_id,
                        actor_role=actor.role,
                        actor_source=actor.source,
                        idempotency_key=command.idempotency_key,
                        request_hash=request_hash,
                        command=command.command,
                        receipt=receipt,
                        created_at=self.clock(),
                    )
                )
                s.flush()
                _invariants(s, app, line, reservation, stock)
                s.add(
                    FulfillmentAudit(
                        id=new_id("audit"),
                        application_id=app.id,
                        command_id=receipt["command_id"],
                        actor_id=actor.actor_id,
                        actor_role=actor.role,
                        actor_source=actor.source,
                        command=command.command,
                        from_state=old_state,
                        to_state=target,
                        from_version=old_version,
                        to_version=app.version,
                        details={"data": data, "request_hash": request_hash},
                        created_at=self.clock(),
                    )
                )
                s.flush()
                if fault:
                    raise RuntimeError("INJECTED_FULFILLMENT_FAILURE")
                return receipt
        except IntegrityError as error:
            if getattr(getattr(error.orig, "diag", None), "constraint_name", "") == (
                "exchange_fulfillments_external_shipment_id_key"
            ):
                raise BusinessError("SHIPMENT_CONFLICT") from None
            raise
