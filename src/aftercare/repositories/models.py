"""PostgreSQL business facts. All writes go through transactional services."""

from datetime import datetime

from pgvector.sqlalchemy import Vector
from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Customer(Base):
    __tablename__ = "customers"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    name: Mapped[str] = mapped_column(String)


class Policy(Base):
    __tablename__ = "policies"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    rules: Mapped[dict] = mapped_column(JSON)
    content_hash: Mapped[str] = mapped_column(String)
    current: Mapped[bool] = mapped_column(Boolean, default=False)
    __table_args__ = (
        Index(
            "one_current_policy", "current", unique=True, postgresql_where=text('"current" = true')
        ),
    )


class Variant(Base):
    __tablename__ = "variants"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    product_id: Mapped[str] = mapped_column(String)
    title: Mapped[str] = mapped_column(String)
    color: Mapped[str] = mapped_column(String)
    size: Mapped[str] = mapped_column(String)
    price_fen: Mapped[int] = mapped_column(Integer)
    returnable: Mapped[bool] = mapped_column(Boolean, default=True)
    exchangeable: Mapped[bool] = mapped_column(Boolean, default=True)
    category: Mapped[str] = mapped_column(String, default="ordinary")
    __table_args__ = (CheckConstraint("price_fen >= 0"),)


class Inventory(Base):
    __tablename__ = "inventory"
    variant_id: Mapped[str] = mapped_column(ForeignKey("variants.id"), primary_key=True)
    available: Mapped[int] = mapped_column(Integer)
    reserved: Mapped[int] = mapped_column(Integer, default=0)
    version: Mapped[int] = mapped_column(Integer, default=1)
    __table_args__ = (CheckConstraint("available >= 0 AND reserved >= 0"),)


class Order(Base):
    __tablename__ = "orders"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    customer_id: Mapped[str] = mapped_column(ForeignKey("customers.id"), index=True)
    status: Mapped[str] = mapped_column(String)
    paid_fen: Mapped[int] = mapped_column(Integer)
    delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    version: Mapped[int] = mapped_column(Integer, default=1)
    policy_version: Mapped[str] = mapped_column(ForeignKey("policies.id"))
    __table_args__ = (CheckConstraint("paid_fen >= 0"),)


class OrderLine(Base):
    __tablename__ = "order_lines"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    order_id: Mapped[str] = mapped_column(ForeignKey("orders.id"), index=True)
    variant_id: Mapped[str] = mapped_column(ForeignKey("variants.id"))
    quantity: Mapped[int] = mapped_column(Integer)
    occupied: Mapped[int] = mapped_column(Integer, default=0)
    unused: Mapped[bool] = mapped_column(Boolean, default=True)
    __table_args__ = (CheckConstraint("quantity > 0 AND occupied >= 0 AND occupied <= quantity"),)


class Conversation(Base):
    __tablename__ = "conversations"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    customer_id: Mapped[str] = mapped_column(ForeignKey("customers.id"), index=True)
    revision: Mapped[int] = mapped_column(Integer, default=0)
    status: Mapped[str] = mapped_column(String, default="IDLE")
    case: Mapped[dict] = mapped_column(JSON, default=dict)


class Message(Base):
    __tablename__ = "messages"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    conversation_id: Mapped[str] = mapped_column(ForeignKey("conversations.id"), index=True)
    client_message_id: Mapped[str] = mapped_column(String)
    role: Mapped[str] = mapped_column(String, default="user")
    text: Mapped[str] = mapped_column(Text)
    revision: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    __table_args__ = (UniqueConstraint("conversation_id", "client_message_id"),)


class Preview(Base):
    __tablename__ = "action_previews"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    conversation_id: Mapped[str] = mapped_column(ForeignKey("conversations.id"), index=True)
    customer_id: Mapped[str] = mapped_column(ForeignKey("customers.id"))
    order_id: Mapped[str] = mapped_column(ForeignKey("orders.id"))
    payload: Mapped[dict] = mapped_column(JSON)
    parameter_hash: Mapped[str] = mapped_column(String)
    revision: Mapped[int] = mapped_column(Integer)
    order_version: Mapped[int] = mapped_column(Integer)
    eligibility_policy: Mapped[str] = mapped_column(String)
    flow_policy: Mapped[str] = mapped_column(String)
    policy_hash: Mapped[str] = mapped_column(String)
    status: Mapped[str] = mapped_column(String, default="PENDING")
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    __table_args__ = (
        Index(
            "unique_active_preview",
            "conversation_id",
            "revision",
            "parameter_hash",
            unique=True,
            postgresql_where=text("status IN ('PENDING', 'CONFIRMED')"),
        ),
    )


class Approval(Base):
    __tablename__ = "approvals"
    preview_id: Mapped[str] = mapped_column(ForeignKey("action_previews.id"), primary_key=True)
    customer_id: Mapped[str] = mapped_column(ForeignKey("customers.id"))
    parameter_hash: Mapped[str] = mapped_column(String)
    revision: Mapped[int] = mapped_column(Integer)
    confirmed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    method: Mapped[str] = mapped_column(String, default="trusted-api")


class Operation(Base):
    __tablename__ = "operations"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    preview_id: Mapped[str] = mapped_column(ForeignKey("action_previews.id"), unique=True)
    customer_id: Mapped[str] = mapped_column(ForeignKey("customers.id"))
    idempotency_key: Mapped[str] = mapped_column(String)
    request_hash: Mapped[str] = mapped_column(String)
    status: Mapped[str] = mapped_column(String, default="COMMITTED")
    receipt: Mapped[dict] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    __table_args__ = (UniqueConstraint("customer_id", "idempotency_key"),)


class Application(Base):
    __tablename__ = "aftercare_requests"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    operation_id: Mapped[str] = mapped_column(ForeignKey("operations.id"), unique=True)
    line_id: Mapped[str] = mapped_column(ForeignKey("order_lines.id"), index=True)
    action: Mapped[str] = mapped_column(String)
    quantity: Mapped[int] = mapped_column(Integer)
    target_variant: Mapped[str | None] = mapped_column(ForeignKey("variants.id"))
    status: Mapped[str] = mapped_column(String)
    version: Mapped[int] = mapped_column(Integer, default=1, server_default=text("1"))
    __table_args__ = (
        CheckConstraint("quantity > 0"),
        CheckConstraint("version >= 1", name="ck_aftercare_requests_version"),
        CheckConstraint(
            "action != 'exchange' OR status IN ("
            "'EXCHANGE_REQUESTED', 'AWAITING_RETURN', 'RETURN_IN_TRANSIT', "
            "'RETURN_RECEIVED', 'REPLACEMENT_PENDING', 'REPLACEMENT_SHIPPED', "
            "'COMPLETED', 'REJECTED', 'WITHDRAWN')",
            name="ck_aftercare_requests_exchange_state",
        ),
    )


class ExchangeFulfillment(Base):
    __tablename__ = "exchange_fulfillments"
    application_id: Mapped[str] = mapped_column(
        ForeignKey("aftercare_requests.id"), primary_key=True
    )
    origin: Mapped[str] = mapped_column(String, default="native", server_default="native")
    return_carrier: Mapped[str | None] = mapped_column(String)
    return_tracking: Mapped[str | None] = mapped_column(String)
    received_note: Mapped[str | None] = mapped_column(Text)
    received_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    effect_key: Mapped[str | None] = mapped_column(String, unique=True)
    shipment_status: Mapped[str | None] = mapped_column(String)
    external_shipment_id: Mapped[str | None] = mapped_column(String, unique=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    __table_args__ = (
        CheckConstraint("origin IN ('legacy', 'native')", name="ck_exchange_fulfillments_origin"),
        CheckConstraint(
            "shipment_status IN ('PENDING', 'UNKNOWN', 'SUCCEEDED', 'FAILED', 'MANUAL_REVIEW')",
            name="ck_exchange_fulfillments_shipment_status",
        ),
    )


class FulfillmentCommand(Base):
    __tablename__ = "fulfillment_commands"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    application_id: Mapped[str] = mapped_column(ForeignKey("aftercare_requests.id"), index=True)
    actor_id: Mapped[str] = mapped_column(String)
    actor_role: Mapped[str] = mapped_column(String)
    actor_source: Mapped[str] = mapped_column(String)
    idempotency_key: Mapped[str] = mapped_column(String)
    request_hash: Mapped[str] = mapped_column(String)
    command: Mapped[str] = mapped_column(String)
    receipt: Mapped[dict] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    __table_args__ = (
        UniqueConstraint("application_id", "actor_id", "actor_role", "idempotency_key"),
    )


class FulfillmentAudit(Base):
    __tablename__ = "fulfillment_audits"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    application_id: Mapped[str] = mapped_column(ForeignKey("aftercare_requests.id"), index=True)
    command_id: Mapped[str | None] = mapped_column(
        ForeignKey("fulfillment_commands.id"), unique=True
    )
    actor_id: Mapped[str] = mapped_column(String)
    actor_role: Mapped[str] = mapped_column(String)
    actor_source: Mapped[str] = mapped_column(String)
    command: Mapped[str] = mapped_column(String)
    from_state: Mapped[str | None] = mapped_column(String)
    to_state: Mapped[str] = mapped_column(String)
    from_version: Mapped[int | None] = mapped_column(Integer)
    to_version: Mapped[int] = mapped_column(Integer)
    details: Mapped[dict] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    __table_args__ = (UniqueConstraint("application_id", "to_version"),)


class ShipmentRequest(Base):
    __tablename__ = "shipment_requests"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    application_id: Mapped[str] = mapped_column(ForeignKey("aftercare_requests.id"), unique=True)
    effect_key: Mapped[str] = mapped_column(String, unique=True)
    payload: Mapped[dict] = mapped_column(JSON)
    request_hash: Mapped[str] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String, default="PENDING", server_default="PENDING")
    external_shipment_id: Mapped[str | None] = mapped_column(String, unique=True)
    provider_external_id: Mapped[str | None] = mapped_column(String, unique=True)
    last_sequence: Mapped[int] = mapped_column(Integer, default=0, server_default=text("0"))
    triggered_by: Mapped[dict] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    __table_args__ = (
        CheckConstraint(
            "status IN ('PENDING', 'UNKNOWN', 'SUCCEEDED', 'FAILED', 'MANUAL_REVIEW')",
            name="ck_shipment_requests_status",
        ),
        CheckConstraint("length(request_hash) = 64", name="ck_shipment_requests_hash"),
        CheckConstraint("last_sequence >= 0", name="ck_shipment_requests_sequence"),
    )


class WarehouseOutbox(Base):
    __tablename__ = "warehouse_outbox"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    request_id: Mapped[str] = mapped_column(ForeignKey("shipment_requests.id"), unique=True)
    payload: Mapped[dict] = mapped_column(JSON)
    status: Mapped[str] = mapped_column(String, default="PENDING", server_default="PENDING")
    lease_owner: Mapped[str | None] = mapped_column(String)
    generation: Mapped[int] = mapped_column(Integer, default=0, server_default=text("0"))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    attempts: Mapped[int] = mapped_column(Integer, default=0, server_default=text("0"))
    cycle_attempts: Mapped[int] = mapped_column(Integer, default=0, server_default=text("0"))
    redrive_count: Mapped[int] = mapped_column(Integer, default=0, server_default=text("0"))
    next_attempt_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    last_error_code: Mapped[str | None] = mapped_column(String)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    __table_args__ = (
        CheckConstraint(
            "status IN ('PENDING', 'CLAIMED', 'DELIVERED', 'BLOCKED', 'DEAD')",
            name="ck_warehouse_outbox_status",
        ),
        CheckConstraint("generation >= 0 AND attempts >= 0", name="ck_warehouse_outbox_counters"),
        CheckConstraint(
            "cycle_attempts >= 0 AND redrive_count >= 0", name="ck_warehouse_outbox_cycles"
        ),
        CheckConstraint(
            "(status = 'CLAIMED' AND lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL) "
            "OR (status != 'CLAIMED' AND lease_owner IS NULL AND lease_expires_at IS NULL)",
            name="ck_warehouse_outbox_lease",
        ),
        Index("ix_warehouse_outbox_claim", "status", "next_attempt_at", "id"),
    )


class OutboxAttempt(Base):
    __tablename__ = "outbox_attempts"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    outbox_id: Mapped[str] = mapped_column(ForeignKey("warehouse_outbox.id"), index=True)
    generation: Mapped[int] = mapped_column(Integer)
    owner: Mapped[str] = mapped_column(String)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    send_state: Mapped[str] = mapped_column(
        String, default="LEGACY_UNKNOWN", server_default="LEGACY_UNKNOWN"
    )
    send_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    outcome: Mapped[str | None] = mapped_column(String)
    error_code: Mapped[str | None] = mapped_column(String)
    __table_args__ = (
        UniqueConstraint("outbox_id", "generation"),
        CheckConstraint("generation >= 1", name="ck_outbox_attempts_generation"),
        CheckConstraint(
            "send_state IN ('NOT_STARTED', 'STARTED', 'LEGACY_UNKNOWN')",
            name="ck_outbox_attempts_send_state",
        ),
        CheckConstraint(
            "(send_state = 'STARTED' AND send_started_at IS NOT NULL) OR "
            "(send_state != 'STARTED' AND send_started_at IS NULL)",
            name="ck_outbox_attempts_send_started",
        ),
        CheckConstraint(
            "outcome IN ('SUCCEEDED', 'RETRYABLE', 'FAILED', 'UNKNOWN')",
            name="ck_outbox_attempts_outcome",
        ),
    )


class WarehouseInbox(Base):
    __tablename__ = "warehouse_inbox"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    source: Mapped[str] = mapped_column(String)
    event_id: Mapped[str] = mapped_column(String)
    payload: Mapped[dict] = mapped_column(JSON)
    body_hash: Mapped[str] = mapped_column(String(64))
    sequence: Mapped[int] = mapped_column(Integer)
    effect_key: Mapped[str] = mapped_column(String, index=True)
    application_id: Mapped[str] = mapped_column(String)
    external_shipment_id: Mapped[str] = mapped_column(String)
    request_hash: Mapped[str] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String, default="RECEIVED", server_default="RECEIVED")
    error_code: Mapped[str | None] = mapped_column(String)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    __table_args__ = (
        UniqueConstraint("source", "event_id"),
        CheckConstraint("sequence >= 1", name="ck_warehouse_inbox_sequence"),
        CheckConstraint(
            "status IN ('RECEIVED','PENDING','PROCESSED','STALE','CONFLICT')",
            name="ck_warehouse_inbox_status",
        ),
    )


class InboxConflict(Base):
    __tablename__ = "inbox_conflicts"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    inbox_id: Mapped[str] = mapped_column(ForeignKey("warehouse_inbox.id"), index=True)
    original_hash: Mapped[str] = mapped_column(String(64))
    new_hash: Mapped[str] = mapped_column(String(64))
    payload: Mapped[dict] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    __table_args__ = (UniqueConstraint("inbox_id", "new_hash"),)


class ReconciliationTask(Base):
    __tablename__ = "reconciliation_tasks"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    request_id: Mapped[str] = mapped_column(ForeignKey("shipment_requests.id"), unique=True)
    status: Mapped[str] = mapped_column(String, default="PENDING", server_default="PENDING")
    owner: Mapped[str | None] = mapped_column(String)
    generation: Mapped[int] = mapped_column(Integer, default=0, server_default=text("0"))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    attempts: Mapped[int] = mapped_column(Integer, default=0, server_default=text("0"))
    cycle_attempts: Mapped[int] = mapped_column(Integer, default=0, server_default=text("0"))
    cycle: Mapped[int] = mapped_column(Integer, default=1, server_default=text("1"))
    next_attempt_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    deadline_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    last_error_code: Mapped[str | None] = mapped_column(String)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    __table_args__ = (
        CheckConstraint(
            "status IN ('PENDING','CLAIMED','DONE','EXHAUSTED')",
            name="ck_reconciliation_tasks_status",
        ),
        CheckConstraint(
            "generation >= 0 AND attempts >= 0 AND cycle_attempts >= 0 AND cycle >= 1",
            name="ck_reconciliation_tasks_counters",
        ),
        CheckConstraint(
            "(status='CLAIMED' AND owner IS NOT NULL AND lease_expires_at IS NOT NULL) "
            "OR (status!='CLAIMED' AND owner IS NULL AND lease_expires_at IS NULL)",
            name="ck_reconciliation_tasks_lease",
        ),
        Index("ix_reconciliation_tasks_claim", "status", "next_attempt_at", "id"),
    )


class RecoveryCommand(Base):
    __tablename__ = "recovery_commands"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    actor_id: Mapped[str] = mapped_column(String)
    actor_role: Mapped[str] = mapped_column(String)
    actor_source: Mapped[str] = mapped_column(String)
    idempotency_key: Mapped[str] = mapped_column(String)
    action: Mapped[str] = mapped_column(String)
    request_id: Mapped[str] = mapped_column(ForeignKey("shipment_requests.id"), index=True)
    outbox_id: Mapped[str] = mapped_column(ForeignKey("warehouse_outbox.id"))
    expected_generation: Mapped[int] = mapped_column(Integer)
    request_hash: Mapped[str] = mapped_column(String(64))
    receipt: Mapped[dict] = mapped_column(JSON)
    reason: Mapped[str] = mapped_column(String(500))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    __table_args__ = (UniqueConstraint("actor_id", "idempotency_key"),)


class Reservation(Base):
    __tablename__ = "inventory_reservations"
    application_id: Mapped[str] = mapped_column(
        ForeignKey("aftercare_requests.id"), primary_key=True
    )
    variant_id: Mapped[str] = mapped_column(ForeignKey("inventory.variant_id"))
    quantity: Mapped[int] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String, default="RESERVED")
    __table_args__ = (CheckConstraint("quantity > 0"),)


class Refund(Base):
    __tablename__ = "refund_requests"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    operation_id: Mapped[str] = mapped_column(ForeignKey("operations.id"), unique=True)
    amount_fen: Mapped[int] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String, default="SIMULATED_REQUESTED")


class Handoff(Base):
    __tablename__ = "handoff_tickets"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    conversation_id: Mapped[str] = mapped_column(ForeignKey("conversations.id"))
    revision: Mapped[int] = mapped_column(Integer)
    reason_code: Mapped[str] = mapped_column(String)
    summary: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String, default="HANDOFF_PENDING")
    __table_args__ = (UniqueConstraint("conversation_id", "revision", "reason_code"),)


class Event(Base):
    __tablename__ = "events"
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    conversation_id: Mapped[str] = mapped_column(ForeignKey("conversations.id"), index=True)
    event_type: Mapped[str] = mapped_column(String)
    payload: Mapped[dict] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class IdempotencyBinding(Base):
    __tablename__ = "idempotency_bindings"
    customer_id: Mapped[str] = mapped_column(ForeignKey("customers.id"), primary_key=True)
    key: Mapped[str] = mapped_column(String, primary_key=True)
    request_hash: Mapped[str] = mapped_column(String)
    operation_id: Mapped[str] = mapped_column(ForeignKey("operations.id"))


class PolicyChunk(Base):
    __tablename__ = "policy_chunks"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    corpus_hash: Mapped[str] = mapped_column(String)
    model_revision: Mapped[str] = mapped_column(String)
    metadata_json: Mapped[dict] = mapped_column(JSON)
    embedding: Mapped[list[float] | None] = mapped_column(Vector(512))


class Budget(Base):
    __tablename__ = "budgets"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    cap: Mapped[int] = mapped_column(BigInteger)
    settled: Mapped[int] = mapped_column(BigInteger, default=0)
    reserved: Mapped[int] = mapped_column(BigInteger, default=0)
    paused: Mapped[bool] = mapped_column(Boolean, default=False)
    __table_args__ = (CheckConstraint("cap >= 0 AND settled >= 0 AND reserved >= 0"),)


class ModelCall(Base):
    __tablename__ = "model_calls"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    budget_id: Mapped[str] = mapped_column(ForeignKey("budgets.id"), index=True)
    provider: Mapped[str] = mapped_column(String)
    exact_model: Mapped[str] = mapped_column(String)
    batch_id: Mapped[str] = mapped_column(String)
    conversation_id: Mapped[str] = mapped_column(ForeignKey("conversations.id"), index=True)
    run_id: Mapped[str] = mapped_column(String, index=True)
    reserved: Mapped[int] = mapped_column(Integer)
    usage: Mapped[int | None] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String)
    request_id: Mapped[str | None] = mapped_column(String)
    details: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class DemoSession(Base):
    __tablename__ = "demo_sessions"
    token_hash: Mapped[str] = mapped_column(String, primary_key=True)
    customer_id: Mapped[str] = mapped_column(ForeignKey("customers.id"))
    csrf_token: Mapped[str] = mapped_column(String)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class StaffAccount(Base):
    __tablename__ = "staff_accounts"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    username: Mapped[str] = mapped_column(String(100), unique=True)
    password_hash: Mapped[str] = mapped_column(String)
    role: Mapped[str] = mapped_column(String)
    active: Mapped[bool] = mapped_column(Boolean, default=True, server_default=text("true"))
    auth_version: Mapped[int] = mapped_column(Integer, default=1, server_default=text("1"))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    __table_args__ = (
        CheckConstraint("role IN ('support', 'warehouse')", name="ck_staff_accounts_role"),
        CheckConstraint("auth_version >= 1", name="ck_staff_accounts_auth_version"),
    )


class StaffSession(Base):
    __tablename__ = "staff_sessions"
    token_hash: Mapped[str] = mapped_column(String(64), primary_key=True)
    staff_id: Mapped[str] = mapped_column(ForeignKey("staff_accounts.id"), index=True)
    csrf_token: Mapped[str] = mapped_column(String(64))
    auth_version: Mapped[int] = mapped_column(Integer)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class Job(Base):
    __tablename__ = "jobs"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    conversation_id: Mapped[str] = mapped_column(ForeignKey("conversations.id"), index=True)
    message_id: Mapped[str | None] = mapped_column(ForeignKey("messages.id"))
    dedup_key: Mapped[str] = mapped_column(String, unique=True)
    kind: Mapped[str] = mapped_column(String)
    revision: Mapped[int] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String, default="PENDING")
    lease_owner: Mapped[str | None] = mapped_column(String)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    attempt: Mapped[int] = mapped_column(Integer, default=0)
    cancel_requested: Mapped[bool] = mapped_column(Boolean, default=False)
    result: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class ExperimentRun(Base):
    __tablename__ = "experiment_runs"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    plan_hash: Mapped[str] = mapped_column(String)
    config: Mapped[dict] = mapped_column(JSON)
    deadline: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(String, default="READY")


class ExperimentEpisode(Base):
    __tablename__ = "experiment_episodes"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    run_id: Mapped[str] = mapped_column(ForeignKey("experiment_runs.id"))
    conversation_id: Mapped[str] = mapped_column(ForeignKey("conversations.id"))
    config: Mapped[dict] = mapped_column(JSON)
    deadline: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    status: Mapped[str] = mapped_column(String, default="READY")


class SharedModelQuota(Base):
    __tablename__ = "shared_model_quotas"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    confirmed_remaining: Mapped[int] = mapped_column(BigInteger)
    watermark: Mapped[dict] = mapped_column(JSON)
    confirmation: Mapped[dict] = mapped_column(JSON)
    paused: Mapped[bool] = mapped_column(Boolean, default=False)


class ActorBudget(Base):
    __tablename__ = "experiment_actor_budgets"
    id: Mapped[str] = mapped_column(String, primary_key=True)
    run_id: Mapped[str] = mapped_column(ForeignKey("experiment_runs.id"))
    episode_id: Mapped[str | None] = mapped_column(ForeignKey("experiment_episodes.id"))
    role: Mapped[str] = mapped_column(String)
    cap: Mapped[int] = mapped_column(BigInteger)
    request_cap: Mapped[int] = mapped_column(Integer)
    settled: Mapped[int] = mapped_column(BigInteger, default=0)
    reserved: Mapped[int] = mapped_column(BigInteger, default=0)
    requests: Mapped[int] = mapped_column(Integer, default=0)


class ExperimentCall(Base):
    __tablename__ = "experiment_calls"
    call_id: Mapped[str] = mapped_column(ForeignKey("model_calls.id"), primary_key=True)
    quota_id: Mapped[str] = mapped_column(ForeignKey("shared_model_quotas.id"))
    run_id: Mapped[str] = mapped_column(ForeignKey("experiment_runs.id"))
    episode_id: Mapped[str] = mapped_column(ForeignKey("experiment_episodes.id"))
    role: Mapped[str] = mapped_column(String)
    attempt_id: Mapped[str] = mapped_column(String, unique=True)
    dimensions: Mapped[list] = mapped_column(JSON)
    metadata_: Mapped[dict] = mapped_column("metadata", JSON)
