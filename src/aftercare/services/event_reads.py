"""Short, authorized event batches for external SSE, without execution locks.

Supported writers allocate event IDs while holding the conversation's write lock.
With the existing CACHE 1 sequence this makes committed IDs a per-conversation
prefix. Direct SQL, manual IDs and sequence rewinds are outside that protocol.
"""

from sqlalchemy import and_, select, text

from aftercare.domain.contracts import utc_now
from aftercare.domain.rules import require
from aftercare.repositories.models import Conversation, DemoSession, Event
from aftercare.services.business import columns

BATCH_SIZE = 100
MAX_EVENT_ID = 2**63 - 1


class EventReader:
    def __init__(self, sessions, clock=utc_now):
        # Session expiry uses the real identity clock, not the business/fixture clock.
        self.sessions, self.clock = sessions, clock

    def read_batch(self, principal, conversation_id, after, session_hash):
        require(type(after) is int and 0 <= after <= MAX_EVENT_ID, "INVALID_EVENT_CURSOR")
        with self.sessions.begin() as session:
            session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
            identity = session.execute(
                select(DemoSession.customer_id, DemoSession.expires_at, Conversation.id)
                .select_from(DemoSession)
                .outerjoin(
                    Conversation,
                    and_(
                        Conversation.id == conversation_id,
                        Conversation.customer_id == DemoSession.customer_id,
                    ),
                )
                .where(DemoSession.token_hash == session_hash)
            ).one_or_none()
            require(
                identity is not None and identity[0] == principal and identity[1] > self.clock(),
                "SESSION_EXPIRED",
            )
            require(identity[2] is not None, "NOT_FOUND")
            return [
                columns(row)
                for row in session.scalars(
                    select(Event)
                    .where(Event.conversation_id == conversation_id, Event.id > after)
                    .order_by(Event.id)
                    .limit(BATCH_SIZE)
                )
            ]
