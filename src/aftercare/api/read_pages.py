"""Opt-in bounded reads; legacy snapshots and streaming remain separate."""

from fastapi import APIRouter, HTTPException, Query, Request

from aftercare.api.routes import AUTH
from aftercare.services.read_pages import PageCursorError, ReadPagesService

router = APIRouter()


@router.get("/api/conversations/{conversation_id}/messages")
def messages(
    conversation_id: str,
    request: Request,
    after: str = Query(default="", max_length=100),
    limit: int = Query(default=50, ge=1, le=100),
    who=AUTH,
):
    try:
        return ReadPagesService(request.app.state.business.sessions).messages(
            who["principal"], conversation_id, after=after, limit=limit
        )
    except PageCursorError:
        raise HTTPException(400, "INVALID_CURSOR") from None
