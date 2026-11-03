"""HTTP read model; internal guarded snapshots keep their separate execution role."""

from sqlalchemy import select, text

from aftercare.domain.rules import require
from aftercare.repositories.models import Conversation, Event, Job, Message, Operation, Preview
from aftercare.services.business import columns


class ReadSnapshotService:
    def __init__(self, business):
        self.business = business

    def get(self, principal, conversation_id):
        with self.business.sessions.begin() as s:
            s.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"))
            conv = s.scalar(
                select(Conversation).where(
                    Conversation.id == conversation_id,
                    Conversation.customer_id == principal,
                )
            )
            require(conv is not None, "NOT_FOUND")
            # Mirror the guarded snapshot's display rules. These are read facts,
            # never a command authorization or a replacement for run_guard.
            runtime = s.scalar(
                select(Event)
                .where(Event.conversation_id == conv.id, Event.event_type == "runtime_state")
                .order_by(Event.id.desc())
            )
            run_state = (
                runtime.payload
                if runtime and runtime.payload.get("revision") == conv.revision
                else {}
            )
            pending = s.scalar(
                select(Preview).where(
                    Preview.conversation_id == conv.id,
                    Preview.revision == conv.revision,
                    Preview.status == "CONFIRMED",
                )
            )
            task_state = (
                "WAITING_SUBMISSION" if pending else run_state.get("task_state", conv.status)
            )
            operation = s.scalar(
                select(Operation)
                .join(Preview, Operation.preview_id == Preview.id)
                .where(Preview.conversation_id == conv.id, Preview.revision == conv.revision)
            )
            if operation and run_state.get("task_state") != "EXECUTION_UNKNOWN":
                notices = list(
                    s.scalars(
                        select(Event).where(
                            Event.conversation_id == conv.id, Event.event_type == "user_notice"
                        )
                    )
                )
                final = any(
                    e.payload.get("stage") == "final"
                    and e.payload.get("operation_id") == operation.id
                    for e in notices
                )
                task_state = "COMPLETED" if final else "SUBMITTED"
            elif run_state.get("task_state") in {"FAILED", "EXECUTION_UNKNOWN", "REJECTED"}:
                task_state = run_state["task_state"]
            if task_state == "COMPLETED" and operation is None:
                # Neither a graph END nor a legacy status string proves a committed outcome.
                task_state = "FAILED"
            return {
                **columns(conv),
                "task_state": task_state,
                "graph_state": run_state.get("graph_state", "NOT_RUN"),
                "messages": [
                    columns(m)
                    for m in s.scalars(
                        select(Message)
                        .where(Message.conversation_id == conv.id)
                        .order_by(Message.revision, Message.created_at, Message.id)
                    )
                ],
                "previews": self.business._preview_views(s, conv),
                "jobs": [
                    columns(j)
                    for j in s.scalars(select(Job).where(Job.conversation_id == conversation_id))
                ],
                "operations": [
                    op.receipt
                    for op in s.scalars(
                        select(Operation)
                        .join(Preview, Operation.preview_id == Preview.id)
                        .where(Preview.conversation_id == conversation_id)
                    )
                ],
            }
