"""Bounded read pages; no execution locks, snapshot loading or business mutations."""

from sqlalchemy import select, text, tuple_

from aftercare.domain.rules import require
from aftercare.repositories.models import (
    Application,
    Conversation,
    ExchangeFulfillment,
    FulfillmentAudit,
    Message,
    Order,
    OrderLine,
)
from aftercare.services.business import columns
from aftercare.services.fulfillment import _owned, validate_actor


class PageCursorError(ValueError):
    """An anchor is absent or outside the already-authorized resource."""


def _page(rows, limit, cursor):
    items = [columns(row) for row in rows[:limit]]
    return {
        "items": items,
        "next_cursor": cursor(rows[limit - 1]) if len(rows) > limit else None,
    }


def _limit(value):
    if type(value) is not int or not 1 <= value <= 100:
        raise PageCursorError("INVALID_PAGE_LIMIT")


class ReadPagesService:
    def __init__(self, sessions):
        self.sessions = sessions

    def messages(self, principal, conversation_id, *, after="", limit=50):
        _limit(limit)
        with self.sessions.begin() as session:
            session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
            conversation = session.scalar(
                select(Conversation.id).where(
                    Conversation.id == conversation_id,
                    Conversation.customer_id == principal,
                )
            )
            require(conversation is not None, "NOT_FOUND")
            query = select(Message).where(Message.conversation_id == conversation_id)
            if after:
                anchor = session.scalar(
                    select(Message).where(
                        Message.id == after,
                        Message.conversation_id == conversation_id,
                    )
                )
                if anchor is None:
                    raise PageCursorError("INVALID_CURSOR")
                query = query.where(
                    tuple_(Message.revision, Message.created_at, Message.id)
                    > (anchor.revision, anchor.created_at, anchor.id)
                )
            rows = list(
                session.scalars(
                    query.order_by(Message.revision, Message.created_at, Message.id).limit(
                        limit + 1
                    )
                )
            )
            return _page(rows, limit, lambda row: row.id)

    def fulfillment_events(self, actor, application_id, *, after_version=0, limit=50):
        validate_actor(actor)
        _limit(limit)
        if type(after_version) is not int or not 0 <= after_version <= 2**31 - 1:
            raise PageCursorError("INVALID_CURSOR")
        with self.sessions.begin() as session:
            session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
            resource = session.execute(
                select(Application, Order, ExchangeFulfillment.application_id)
                .join(OrderLine, Application.line_id == OrderLine.id)
                .join(Order, OrderLine.order_id == Order.id)
                .outerjoin(
                    ExchangeFulfillment, ExchangeFulfillment.application_id == Application.id
                )
                .where(Application.id == application_id)
            ).one_or_none()
            require(resource is not None, "NOT_FOUND")
            application, order, fulfillment_id = resource
            _owned(actor, order)
            require(application.action == "exchange", "UNSUPPORTED_APPLICATION")
            require(fulfillment_id is not None, "INVENTORY_INVARIANT")
            rows = list(
                session.scalars(
                    select(FulfillmentAudit)
                    .where(
                        FulfillmentAudit.application_id == application_id,
                        FulfillmentAudit.to_version > after_version,
                    )
                    .order_by(FulfillmentAudit.to_version)
                    .limit(limit + 1)
                )
            )
            return _page(rows, limit, lambda row: row.to_version)
