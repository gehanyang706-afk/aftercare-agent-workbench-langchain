"""Warehouse callback authentication and support-only recovery command wiring."""

import re
import secrets
import time

from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import Field, ValidationError
from sqlalchemy.orm import sessionmaker
from starlette.concurrency import run_in_threadpool

from aftercare.api.fulfillment import AUTH, Strict, _guard_identity
from aftercare.domain.rules import require
from aftercare.domain.warehouse import PROVIDER_ID, WarehouseEvent, sign_event
from aftercare.services.recovery import RecoveryService

router = APIRouter()
MAX_CALLBACK_BYTES = 16384
CALLBACK_WINDOW_SECONDS = 300


class RecoveryBody(Strict):
    expected_generation: int = Field(ge=0)
    reason: str = Field(min_length=1, max_length=500, pattern=r"\S")


def recovery_service(request, sessions=None):
    settings = request.app.state.settings
    return RecoveryService(
        sessions if sessions is not None else request.app.state.business.sessions,
        lease_seconds=settings.outbox_lease_seconds,
        max_attempts=settings.reconciliation_max_attempts,
        deadline_seconds=settings.reconciliation_deadline_seconds,
        max_cycles=settings.reconciliation_max_cycles,
    )


def _callback_auth(request, raw=None):
    settings = request.app.state.settings
    if not settings.warehouse_callbacks_enabled:
        raise HTTPException(404, "NOT_FOUND")
    source = request.headers.get("x-warehouse-source", "")
    timestamp = request.headers.get("x-warehouse-timestamp", "")
    signature = request.headers.get("x-warehouse-signature", "")
    if (
        source != PROVIDER_ID
        or re.fullmatch(r"[0-9]{1,12}", timestamp) is None
        or abs(time.time() - int(timestamp)) > CALLBACK_WINDOW_SECONDS
        or re.fullmatch(r"v1=[0-9a-f]{64}", signature) is None
    ):
        raise HTTPException(401, "WAREHOUSE_CALLBACK_AUTH_FAILED")
    if raw is not None:
        expected = sign_event(settings.warehouse_callback_secret.get_secret_value(), raw, timestamp)
        if not secrets.compare_digest(signature, expected):
            raise HTTPException(401, "WAREHOUSE_CALLBACK_AUTH_FAILED")


@router.post("/api/integrations/warehouse/events")
async def warehouse_event(request: Request):
    _callback_auth(request)
    length = request.headers.get("content-length")
    if length is not None:
        try:
            too_large = int(length) > MAX_CALLBACK_BYTES
        except ValueError:
            raise HTTPException(422, "INVALID_INPUT") from None
        if too_large:
            raise HTTPException(413, "CALLBACK_BODY_TOO_LARGE")
    chunks, total = [], 0
    async for chunk in request.stream():
        total += len(chunk)
        if total > MAX_CALLBACK_BYTES:
            raise HTTPException(413, "CALLBACK_BODY_TOO_LARGE")
        chunks.append(chunk)
    raw = b"".join(chunks)
    _callback_auth(request, raw)
    try:
        event = WarehouseEvent.model_validate_json(raw).model_dump()
    except ValidationError:
        raise HTTPException(422, "INVALID_INPUT") from None
    result = await run_in_threadpool(recovery_service(request).receive_event, event)
    status = result.get("status")
    http_status = 409 if status == "CONFLICT" else 202 if status in {"PENDING", "RECEIVED"} else 200
    return JSONResponse(status_code=http_status, content=result)


def _execute(request, who, resource_id, body, key, action):
    require(who.actor.role == "support", "FORBIDDEN")
    if key is None or not key.strip() or len(key) > 100:
        raise HTTPException(422, "IDEMPOTENCY_KEY_REQUIRED")
    sessions = request.app.state.business.sessions
    with sessions.begin() as auth_session:
        _guard_identity(auth_session, request, who)
        bound = sessionmaker(
            auth_session.connection(),
            expire_on_commit=False,
            join_transaction_mode="create_savepoint",
        )
        service = recovery_service(request, bound)
        if action == "reconcile":
            return service.manual_reconcile(
                who.actor, resource_id, body.expected_generation, key, body.reason
            )
        return service.redrive(who.actor, resource_id, body.expected_generation, key, body.reason)


@router.post("/api/fulfillment-requests/{request_id}/reconcile", status_code=202)
def reconcile(
    request_id: str,
    body: RecoveryBody,
    request: Request,
    idempotency_key: str | None = Header(default=None),
    who=AUTH,
):
    return _execute(request, who, request_id, body, idempotency_key, "reconcile")


@router.post("/api/outbox/{outbox_id}/redrive", status_code=202)
def redrive(
    outbox_id: str,
    body: RecoveryBody,
    request: Request,
    idempotency_key: str | None = Header(default=None),
    who=AUTH,
):
    return _execute(request, who, outbox_id, body, idempotency_key, "redrive")
