"""Server-owned demo identity, CSRF protected commands, durable queue, replay-only SSE."""

import asyncio
import json
import secrets
from datetime import timedelta
from urllib.parse import urlparse

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select

from aftercare.domain.contracts import BusinessError, utc_now
from aftercare.repositories.models import (
    Conversation,
    Customer,
    DemoSession,
    Event,
    Inventory,
    Job,
)
from aftercare.services.business import columns, digest
from aftercare.services.event_reads import BATCH_SIZE, MAX_EVENT_ID, EventReader
from aftercare.services.read_snapshot import ReadSnapshotService
from aftercare.workers.queue import enqueue

router = APIRouter()
COOKIE = "aftercare_session"


class Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class SessionBody(Strict):
    customer_id: str


class MessageBody(Strict):
    text: str = Field(min_length=1, max_length=6000)
    client_message_id: str = Field(min_length=1, max_length=100)


class ConfirmBody(Strict):
    decision: str


class FaultBody(Strict):
    conversation_id: str
    fault: str
    variant_id: str | None = None
    order_id: str | None = None


def origin_check(request):
    origin = request.headers.get("origin")
    if origin:
        parsed = urlparse(origin)
        if parsed.scheme != request.url.scheme or parsed.netloc != request.url.netloc:
            raise HTTPException(403, "ORIGIN_DENIED")


def identity(request: Request):
    token = request.cookies.get(COOKIE)
    if not token:
        raise HTTPException(401, "DEMO_SESSION_REQUIRED")
    if not request.app.state.settings.demo_login_enabled:
        raise HTTPException(401, "DEMO_LOGIN_DISABLED")
    service = request.app.state.business
    with service.sessions() as s:
        session = s.get(DemoSession, digest(token))
        if not session or session.expires_at <= utc_now():
            raise HTTPException(401, "DEMO_SESSION_EXPIRED")
        if request.method not in ["GET", "HEAD"]:
            origin_check(request)
            if not secrets.compare_digest(
                request.headers.get("x-csrf-token", ""), session.csrf_token
            ):
                raise HTTPException(403, "CSRF_REQUIRED")
        return {"principal": session.customer_id, "csrf_token": session.csrf_token}


AUTH = Depends(identity)


@router.post("/api/demo-sessions")
def demo_session(body: SessionBody, request: Request, response: Response):
    if not request.app.state.settings.demo_login_enabled:
        raise HTTPException(404, "NOT_FOUND")
    origin_check(request)
    if "application/json" not in request.headers.get("content-type", ""):
        raise HTTPException(415, "JSON_REQUIRED")
    if body.customer_id not in ["alice", "bob", "carol"]:
        raise HTTPException(404, "NOT_FOUND")
    service = request.app.state.business
    token, csrf = secrets.token_urlsafe(32), secrets.token_urlsafe(32)
    with service.sessions.begin() as s:
        if not s.get(Customer, body.customer_id):
            raise HTTPException(503, "DEMO_SEED_REQUIRED")
        s.add(
            DemoSession(
                token_hash=digest(token),
                customer_id=body.customer_id,
                csrf_token=csrf,
                expires_at=utc_now() + timedelta(hours=24),
            )
        )
    response.delete_cookie("aftercare_staff_session")
    response.set_cookie(
        COOKIE,
        token,
        httponly=True,
        samesite="strict",
        max_age=86400,
        secure=request.url.scheme == "https",
    )
    return {"principal": body.customer_id, "csrf_token": csrf}


@router.get("/api/session")
def session_info(who=AUTH):
    return who


@router.delete("/api/session")
def logout_customer(request: Request, response: Response, who=AUTH):
    with request.app.state.business.sessions.begin() as s:
        row = s.get(DemoSession, digest(request.cookies[COOKIE]), with_for_update=True)
        if row is not None:
            s.delete(row)
    response.delete_cookie(COOKIE)
    return {"ok": True}


@router.get("/api/orders")
def orders(request: Request, who=AUTH):
    return request.app.state.business.list_orders(who["principal"])


@router.get("/api/orders/{order_id}")
def order(order_id: str, request: Request, who=AUTH):
    return request.app.state.business.get_order(who["principal"], order_id)


@router.get("/api/conversations")
def conversations(request: Request, who=AUTH):
    with request.app.state.business.sessions() as s:
        return [
            columns(c)
            for c in s.scalars(
                select(Conversation)
                .where(Conversation.customer_id == who["principal"])
                .order_by(Conversation.id)
            )
        ]


@router.post("/api/conversations")
def create_conversation(request: Request, who=AUTH):
    return {"id": request.app.state.business.create_conversation(who["principal"])}


@router.get("/api/conversations/{conversation_id}")
def snapshot(conversation_id: str, request: Request, who=AUTH):
    service = request.app.state.business
    return ReadSnapshotService(service).get(who["principal"], conversation_id)


@router.post("/api/conversations/{conversation_id}/messages", status_code=202)
def message(conversation_id: str, body: MessageBody, request: Request, who=AUTH):
    service = request.app.state.business

    def queue(s, msg):
        return enqueue(s, service, conversation_id, msg.revision, "message", message_id=msg.id)

    msg = service.append_message(
        who["principal"], conversation_id, body.text, body.client_message_id, enqueue=queue
    )
    with service.sessions() as s:
        job = s.scalar(select(Job).where(Job.message_id == msg["id"]))
    return {"message_id": msg["id"], "job_id": job.id, "deduplicated": job.status != "PENDING"}


@router.post("/api/previews/{preview_id}/confirm", status_code=202)
def confirm(preview_id: str, body: ConfirmBody, request: Request, who=AUTH):
    if body.decision not in ["accept", "reject"]:
        raise HTTPException(422, "INVALID_DECISION")
    service = request.app.state.business

    def queue(s, conv, p):
        return enqueue(
            s,
            service,
            conv.id,
            conv.revision,
            "confirm" if body.decision == "accept" else "reject",
            preview_id=p.id,
        )

    result = service.confirm(who["principal"], preview_id, body.decision == "accept", enqueue=queue)
    return result


@router.post("/api/jobs/{job_id}/cancel")
def cancel(job_id: str, request: Request, who=AUTH):
    service = request.app.state.business
    with service.sessions.begin() as s:
        initial = s.get(Job, job_id)
        if not initial:
            raise HTTPException(404, "NOT_FOUND")
        service._conversation(s, who["principal"], initial.conversation_id)
        job = s.get(Job, job_id, with_for_update=True)
        if job.status in ["PENDING", "RUNNING"]:
            job.cancel_requested = True
            if job.status == "PENDING":
                job.status = "CANCELLED"
            service.event(s, job.conversation_id, "cancel_requested", {"job_id": job.id})
        return {
            "job_id": job.id,
            "status": job.status,
            "cancel_requested": job.cancel_requested,
            "notice": "已提交的操作以实际回执为准，不自动撤销。",
        }


@router.get("/api/operations/{operation_id}")
def operation(operation_id: str, request: Request, who=AUTH):
    return request.app.state.business.get_operation(who["principal"], operation_id=operation_id)


def load_events(service, principal, conversation_id, after):
    with service.sessions.begin() as s:
        service._conversation(s, principal, conversation_id)
        return [
            columns(e)
            for e in s.scalars(
                select(Event)
                .where(Event.conversation_id == conversation_id, Event.id > after)
                .order_by(Event.id)
                .limit(100)
            )
        ]


@router.get("/api/conversations/{conversation_id}/events")
async def events(
    conversation_id: str,
    request: Request,
    after: int = 0,
    once: bool = False,
    who=AUTH,
):
    service = request.app.state.business
    try:
        cursor = max(0, after, int(request.headers.get("last-event-id", "0")))
        if cursor > MAX_EVENT_ID:
            raise ValueError("EVENT_CURSOR_OUT_OF_RANGE")
    except ValueError:
        raise HTTPException(400, "INVALID_EVENT_CURSOR") from None
    reader = EventReader(service.sessions)
    session_hash = digest(request.cookies.get(COOKIE, ""))
    # Fetch before headers so initial authorization errors retain normal HTTP status.
    # The short transaction finishes before StreamingResponse begins sending rows.
    initial = await asyncio.to_thread(
        reader.read_batch, who["principal"], conversation_id, cursor, session_hash
    )

    async def stream():
        nonlocal cursor
        rows, delay = initial, 0.5
        while True:
            if await request.is_disconnected():
                return
            if rows:
                delay = 0.5
            for event in rows:
                if await request.is_disconnected():
                    return
                cursor = event["id"]
                data = json.dumps(event, ensure_ascii=False)
                yield f"id: {cursor}\nevent: trace\ndata: {data}\n\n"
            if once or await request.is_disconnected():
                return
            if len(rows) == BATCH_SIZE:
                # Bounded batches can catch up without an artificial half-second per batch.
                await asyncio.sleep(0)
            else:
                yield ": heartbeat\n\n"
                await asyncio.sleep(delay)
                if not rows:
                    delay = min(2.0, delay * 2)
            if await request.is_disconnected():
                return
            try:
                rows = await asyncio.to_thread(
                    reader.read_batch, who["principal"], conversation_id, cursor, session_hash
                )
            except BusinessError as error:
                if error.code not in {"SESSION_EXPIRED", "NOT_FOUND"}:
                    raise
                if not await request.is_disconnected():
                    data = json.dumps({"error_code": error.code}, ensure_ascii=False)
                    # No id: revocation must not move the replay cursor.
                    yield f"event: stream_closed\ndata: {data}\n\n"
                return

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@router.get("/api/runs/{run_id}/trace")
def trace(run_id: str, request: Request, who=AUTH):
    service = request.app.state.business
    with service.sessions.begin() as s:
        job = s.get(Job, run_id)
        if not job:
            raise HTTPException(404, "NOT_FOUND")
        service._conversation(s, who["principal"], job.conversation_id)
        conversation_id = job.conversation_id
    return load_events(service, who["principal"], conversation_id, 0)


@router.post("/api/dev/faults")
def fault(body: FaultBody, request: Request, who=AUTH):
    if not request.app.state.settings.dev_faults:
        raise HTTPException(404, "NOT_FOUND")
    service = request.app.state.business
    with service.sessions.begin() as s:
        conv = service._conversation(s, who["principal"], body.conversation_id)
        if body.fault == "inventory_zero":
            stock = s.get(Inventory, body.variant_id, with_for_update=True)
            if not stock:
                raise HTTPException(404, "NOT_FOUND")
            stock.available = 0
            stock.version += 1
        elif body.fault in ["lost_response", "before_commit", "slow"]:
            conv.case = {**conv.case, "_demo_fault_next": body.fault}
        else:
            raise HTTPException(422, "UNKNOWN_FAULT")
        service.event(s, conv.id, "demo_fault_injected", {"fault": body.fault})
    return {"fault": body.fault, "injected": True}
