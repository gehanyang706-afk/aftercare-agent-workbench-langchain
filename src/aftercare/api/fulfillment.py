"""HTTP/session adaptation only; state and inventory rules remain in B1 services."""

import secrets
from dataclasses import dataclass
from typing import Literal

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request, Response
from pydantic import BaseModel, ConfigDict, Field, SecretStr
from sqlalchemy import select
from sqlalchemy.orm import sessionmaker

from aftercare.api.routes import COOKIE as CUSTOMER_COOKIE
from aftercare.api.routes import identity as customer_identity
from aftercare.api.routes import origin_check
from aftercare.domain.contracts import utc_now
from aftercare.domain.fulfillment import ROLES, TRANSITIONS, Actor, FulfillmentCommandInput
from aftercare.domain.rules import require
from aftercare.repositories.models import (
    Application,
    Customer,
    DemoSession,
    ExchangeFulfillment,
    FulfillmentAudit,
    Operation,
    Order,
    OrderLine,
)
from aftercare.services.business import digest
from aftercare.services.fulfillment import FulfillmentService
from aftercare.services.read_pages import ReadPagesService
from aftercare.services.simulated_warehouse import SimulatedWarehouse
from aftercare.services.staff_auth import StaffAuthService

router = APIRouter()
STAFF_COOKIE = "aftercare_staff_session"


class Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class LoginBody(Strict):
    username: str = Field(min_length=1, max_length=100)
    password: SecretStr = Field(min_length=1, max_length=128)


class VersionBody(Strict):
    expected_version: int = Field(ge=1)


class ReviewBody(VersionBody):
    decision: Literal["approve", "reject"]
    note: str = Field(default="", max_length=500)


class ReceiveBody(VersionBody):
    note: str = Field(default="", max_length=500)


class ReturnBody(VersionBody):
    carrier: str = Field(min_length=1, max_length=100)
    tracking_number: str = Field(min_length=1, max_length=100)


@dataclass(frozen=True)
class Identity:
    actor: Actor
    csrf_token: str
    token: str
    kind: str
    display_name: str


def auth_service(request):
    return StaffAuthService(request.app.state.business.sessions)


def csrf_check(request, csrf_token):
    if request.method not in {"GET", "HEAD"}:
        origin_check(request)
        if not secrets.compare_digest(request.headers.get("x-csrf-token", ""), csrf_token):
            raise HTTPException(403, "CSRF_REQUIRED")


def identity(request: Request):
    staff_token = request.cookies.get(STAFF_COOKIE)
    customer_token = request.cookies.get(CUSTOMER_COOKIE)
    if staff_token and customer_token:
        raise HTTPException(401, "AMBIGUOUS_IDENTITY")
    if staff_token:
        result = auth_service(request).authenticate(staff_token)
        who = Identity(
            result["actor"], result["csrf_token"], staff_token, "staff", result["username"]
        )
    elif customer_token:
        result = customer_identity(request)
        with request.app.state.business.sessions() as s:
            customer = s.get(Customer, result["principal"])
            if customer is None:
                raise HTTPException(401, "DEMO_SESSION_EXPIRED")
            name = customer.name
        who = Identity(
            Actor(result["principal"], "customer", "server_session"),
            result["csrf_token"],
            customer_token,
            "customer",
            name,
        )
    else:
        raise HTTPException(401, "SESSION_REQUIRED")
    csrf_check(request, who.csrf_token)
    return who


AUTH = Depends(identity)


def _public_identity(who):
    return {
        "actor_id": who.actor.actor_id,
        "role": who.actor.role,
        "display_name": who.display_name,
        "csrf_token": who.csrf_token,
    }


@router.get("/api/auth/options")
def options(request: Request):
    settings = request.app.state.settings
    return {
        "demo_customer_login": settings.demo_login_enabled,
        "warehouse_simulation": settings.warehouse_simulation_enabled,
    }


@router.post("/api/staff/sessions")
def login(body: LoginBody, request: Request, response: Response):
    origin_check(request)
    if "application/json" not in request.headers.get("content-type", ""):
        raise HTTPException(415, "JSON_REQUIRED")
    result = auth_service(request).login(body.username, body.password.get_secret_value())
    response.delete_cookie(CUSTOMER_COOKIE)
    response.set_cookie(
        STAFF_COOKIE,
        result["token"],
        httponly=True,
        samesite="strict",
        secure=request.url.scheme == "https",
        max_age=max(1, int((result["expires_at"] - utc_now()).total_seconds())),
    )
    return {
        "actor_id": result["actor"].actor_id,
        "role": result["actor"].role,
        "username": result["username"],
        "csrf_token": result["csrf_token"],
        "expires_at": result["expires_at"].isoformat(),
    }


@router.get("/api/staff/session")
def staff_session(who=AUTH):
    if who.kind != "staff":
        raise HTTPException(401, "STAFF_SESSION_REQUIRED")
    return _public_identity(who)


@router.delete("/api/staff/session")
def logout(request: Request, response: Response, who=AUTH):
    if who.kind != "staff":
        raise HTTPException(401, "STAFF_SESSION_REQUIRED")
    auth_service(request).logout(who.token)
    response.delete_cookie(STAFF_COOKIE)
    return {"ok": True}


@router.get("/api/fulfillment/session")
def session_info(who=AUTH):
    return _public_identity(who)


def _item(app, line, order):
    return {
        "application_id": app.id,
        "order_id": order.id,
        "quantity": app.quantity,
        "target_variant": app.target_variant,
        "status": app.status,
        "version": app.version,
    }


@router.get("/api/applications")
def applications(
    request: Request,
    after: str = Query(default="", max_length=100),
    limit: int = Query(default=20, ge=1, le=100),
    who=AUTH,
):
    query = (
        select(Application, OrderLine, Order)
        .join(OrderLine, Application.line_id == OrderLine.id)
        .join(Order, OrderLine.order_id == Order.id)
        .where(Application.action == "exchange", Application.id > after)
        .order_by(Application.id)
        .limit(limit + 1)
    )
    if who.actor.role == "customer":
        query = query.where(Order.customer_id == who.actor.actor_id)
    with request.app.state.business.sessions() as s:
        rows = s.execute(query).all()
        items = [_item(app, line, order) for app, line, order in rows[:limit]]
    return {
        "items": items,
        "next_cursor": items[-1]["application_id"] if len(rows) > limit else None,
    }


def _detail(request, who, application_id):
    sessions = request.app.state.business.sessions
    result = FulfillmentService(sessions).get(who.actor, application_id)
    # Join immutable request metadata and original receipt; do not rewrite that receipt.
    with sessions() as s:
        app, line, order, operation = s.execute(
            select(Application, OrderLine, Order, Operation)
            .join(OrderLine, Application.line_id == OrderLine.id)
            .join(Order, OrderLine.order_id == Order.id)
            .join(Operation, Application.operation_id == Operation.id)
            .where(Application.id == application_id)
        ).one()
        result.update(
            order_id=order.id,
            quantity=app.quantity,
            target_variant=app.target_variant,
            receipt=operation.receipt,
        )
    result["allowed_actions"] = [
        name
        for name, (states, _target) in TRANSITIONS.items()
        if name != "confirm_shipment"
        and ROLES[name] == who.actor.role
        and result["status"] in states
    ]
    if (
        who.actor.role == "warehouse"
        and result["status"] == "REPLACEMENT_PENDING"
        and request.app.state.settings.warehouse_simulation_enabled
        and not result.get("shipment_request")
    ):
        result["allowed_actions"].append("simulate_shipment")
    return result


@router.get("/api/applications/{application_id}")
def detail(application_id: str, request: Request, who=AUTH):
    return _detail(request, who, application_id)


@router.get("/api/applications/{application_id}/events")
def events(
    application_id: str,
    request: Request,
    limit: int | None = Query(default=None, ge=1, le=100),
    after: int | None = Query(default=None, ge=0, le=2**31 - 1),
    who=AUTH,
):
    if limit is not None:
        return ReadPagesService(request.app.state.business.sessions).fulfillment_events(
            who.actor, application_id, after_version=after or 0, limit=limit
        )
    if after is not None:
        raise HTTPException(400, "PAGINATION_LIMIT_REQUIRED")
    return {
        "items": FulfillmentService(request.app.state.business.sessions).get(
            who.actor, application_id
        )["history"]
    }


@router.get("/api/fulfillment-requests/{request_id}")
def shipment_request(request_id: str, request: Request, who=AUTH):
    from aftercare.services.recovery import RecoveryService

    return RecoveryService(request.app.state.business.sessions).get_status(who.actor, request_id)


def _guard_identity(s, request, who):
    if who.kind == "staff":
        current = auth_service(request).guard(s, who.token)
        require(current["actor"] == who.actor, "SESSION_EXPIRED")
    else:
        row = s.get(DemoSession, digest(who.token), with_for_update={"read": True})
        require(
            row is not None
            and row.expires_at > utc_now()
            and row.customer_id == who.actor.actor_id
            and request.app.state.settings.demo_login_enabled,
            "SESSION_EXPIRED",
        )


def execute(request, who, application_id, command, *, simulate=False):
    sessions = request.app.state.business.sessions
    # Hold auth FOR SHARE locks until the business transaction commits. The B1 service
    # joins this outer transaction via a SAVEPOINT; its state/inventory rules stay intact.
    with sessions.begin() as auth_session:
        _guard_identity(auth_session, request, who)
        bound = sessionmaker(
            auth_session.connection(),
            expire_on_commit=False,
            join_transaction_mode="create_savepoint",
        )
        service = FulfillmentService(
            bound, async_delivery=request.app.state.settings.warehouse_delivery_enabled
        )
        actor = who.actor
        if simulate:
            require(actor.role == "warehouse", "FORBIDDEN")
            fulfillment = auth_session.get(ExchangeFulfillment, application_id)
            require(
                fulfillment is not None and fulfillment.effect_key is not None,
                "SHIPMENT_NOT_REQUESTED",
            )
            actor = SimulatedWarehouse.actor
            command = FulfillmentCommandInput(
                command="confirm_shipment",
                expected_version=command.expected_version,
                idempotency_key=command.idempotency_key,
                data={
                    "external_shipment_id": SimulatedWarehouse.shipment_id(fulfillment.effect_key)
                },
            )
        receipt = service.execute(actor, application_id, command)
        if simulate:
            audit = auth_session.scalar(
                select(FulfillmentAudit).where(FulfillmentAudit.command_id == receipt["command_id"])
            )
            if audit is not None and "triggered_by" not in audit.details:
                audit.details = {
                    **audit.details,
                    "triggered_by": {
                        "actor_id": who.actor.actor_id,
                        "role": who.actor.role,
                        "source": who.actor.source,
                    },
                }
        return receipt


def _command(request, who, application_id, body, key, name, data=None):
    if not key or not key.strip() or len(key) > 100:
        raise HTTPException(422, "IDEMPOTENCY_KEY_REQUIRED")
    command = FulfillmentCommandInput(
        command=name,
        expected_version=body.expected_version,
        idempotency_key=key,
        data=data or {},
    )
    return execute(request, who, application_id, command)


@router.post("/api/applications/{application_id}/review")
def review(
    application_id: str,
    body: ReviewBody,
    request: Request,
    idempotency_key: str | None = Header(default=None),
    who=AUTH,
):
    return _command(
        request, who, application_id, body, idempotency_key, body.decision, {"note": body.note}
    )


@router.post("/api/applications/{application_id}/withdraw")
def withdraw(
    application_id: str,
    body: VersionBody,
    request: Request,
    idempotency_key: str | None = Header(default=None),
    who=AUTH,
):
    return _command(request, who, application_id, body, idempotency_key, "withdraw")


@router.post("/api/applications/{application_id}/return-shipment")
def return_shipment(
    application_id: str,
    body: ReturnBody,
    request: Request,
    idempotency_key: str | None = Header(default=None),
    who=AUTH,
):
    return _command(
        request,
        who,
        application_id,
        body,
        idempotency_key,
        "submit_return",
        {"carrier": body.carrier, "tracking_number": body.tracking_number},
    )


@router.post("/api/applications/{application_id}/receive")
def receive(
    application_id: str,
    body: ReceiveBody,
    request: Request,
    idempotency_key: str | None = Header(default=None),
    who=AUTH,
):
    return _command(
        request, who, application_id, body, idempotency_key, "receive", {"note": body.note}
    )


@router.post("/api/applications/{application_id}/replacement-shipments")
def request_shipment(
    application_id: str,
    body: VersionBody,
    request: Request,
    idempotency_key: str | None = Header(default=None),
    who=AUTH,
):
    return _command(request, who, application_id, body, idempotency_key, "request_shipment")


@router.post("/api/applications/{application_id}/complete")
def complete(
    application_id: str,
    body: VersionBody,
    request: Request,
    idempotency_key: str | None = Header(default=None),
    who=AUTH,
):
    return _command(request, who, application_id, body, idempotency_key, "complete")


@router.post("/api/applications/{application_id}/simulate-shipment")
def simulate_shipment(
    application_id: str,
    body: VersionBody,
    request: Request,
    idempotency_key: str | None = Header(default=None),
    who=AUTH,
):
    if not request.app.state.settings.warehouse_simulation_enabled:
        raise HTTPException(404, "NOT_FOUND")
    if not idempotency_key or not idempotency_key.strip() or len(idempotency_key) > 100:
        raise HTTPException(422, "IDEMPOTENCY_KEY_REQUIRED")
    command = FulfillmentCommandInput(
        command="confirm_shipment",
        expected_version=body.expected_version,
        idempotency_key=idempotency_key,
    )
    return execute(request, who, application_id, command, simulate=True)
