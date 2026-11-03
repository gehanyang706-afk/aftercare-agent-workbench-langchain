"""Trusted principal + explicit confirmation + one PostgreSQL transaction."""

import hashlib
import json
from datetime import timedelta
from uuid import uuid4

from sqlalchemy import select, text

from aftercare.domain.contracts import ActionInput, BusinessError, utc_now
from aftercare.domain.rules import check_window, require
from aftercare.repositories.models import (
    Application,
    Approval,
    Conversation,
    Customer,
    Event,
    ExchangeFulfillment,
    FulfillmentAudit,
    Handoff,
    IdempotencyBinding,
    Inventory,
    Message,
    Operation,
    Order,
    OrderLine,
    Policy,
    Preview,
    Refund,
    Reservation,
    Variant,
)


def new_id(prefix):
    return f"{prefix}_{uuid4().hex}"


def digest(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()


def columns(row):
    return {
        c.name: (v.isoformat() if hasattr(v, "isoformat") else v)
        for c in row.__table__.columns
        if (v := getattr(row, c.name)) is not None
    }


class BusinessService:
    def __init__(self, sessions, clock=utc_now):
        self.sessions, self.clock = sessions, clock

    def event(self, session, conversation_id, kind, payload):
        event = Event(
            conversation_id=conversation_id,
            event_type=kind,
            payload=payload,
            created_at=self.clock(),
        )
        session.add(event)
        return event

    def _conversation(self, s, principal, conversation_id):
        row = s.scalar(
            select(Conversation)
            .where(Conversation.id == conversation_id, Conversation.customer_id == principal)
            .with_for_update()
        )
        require(row is not None, "NOT_FOUND")
        if getattr(self, "run_guard", None):
            self.run_guard(s, row)
        return row

    def create_conversation(self, principal):
        with self.sessions.begin() as s:
            require(s.get(Customer, principal) is not None, "NOT_FOUND")
            row = Conversation(
                id=new_id("conv"), customer_id=principal, revision=0, status="IDLE", case={}
            )
            s.add(row)
            s.flush()
            self.event(s, row.id, "conversation_created", {})
            return row.id

    def append_message(
        self, principal, conversation_id, text_value, client_message_id, enqueue=None
    ):
        require(0 < len(text_value) <= 6000, "INVALID_MESSAGE")
        with self.sessions.begin() as s:
            conv = self._conversation(s, principal, conversation_id)
            existing = s.scalar(
                select(Message).where(
                    Message.conversation_id == conv.id,
                    Message.client_message_id == client_message_id,
                )
            )
            if existing:
                require(existing.text == text_value, "MESSAGE_ID_CONFLICT")
                if enqueue:
                    enqueue(s, existing)
                return columns(existing)
            conv.revision += 1
            for preview in s.scalars(
                select(Preview).where(
                    Preview.conversation_id == conv.id, Preview.status.in_(["PENDING", "CONFIRMED"])
                )
            ):
                preview.status = "INVALIDATED"
            conv.status = "RUNNING"
            row = Message(
                id=new_id("msg"),
                conversation_id=conv.id,
                client_message_id=client_message_id,
                text=text_value,
                role="user",
                revision=conv.revision,
                created_at=self.clock(),
            )
            s.add(row)
            s.flush()
            if enqueue:
                enqueue(s, row)
            self.event(
                s,
                conv.id,
                "user_message",
                {"message_id": row.id, "revision": conv.revision, "text": text_value},
            )
            return columns(row)

    def list_orders(self, principal, status_filter=None):
        with self.sessions() as s:
            q = select(Order).where(Order.customer_id == principal).order_by(Order.id).limit(30)
            if status_filter:
                q = q.where(Order.status == status_filter)
            return [columns(row) for row in s.scalars(q)]

    def get_order(self, principal, order_id):
        with self.sessions() as s:
            order = s.scalar(
                select(Order).where(Order.id == order_id, Order.customer_id == principal)
            )
            require(order is not None, "NOT_FOUND")
            lines = list(s.scalars(select(OrderLine).where(OrderLine.order_id == order_id)))
            variant_ids = {line.variant_id for line in lines}
            variants = (
                {
                    variant.id: variant
                    for variant in s.scalars(select(Variant).where(Variant.id.in_(variant_ids)))
                }
                if variant_ids
                else {}
            )
            return {
                **columns(order),
                "lines": [
                    {**columns(line), "variant": columns(variants.get(line.variant_id))}
                    for line in lines
                ],
            }

    def inventory(self, product_id, variant_filters=None):
        with self.sessions() as s:
            rows = []
            for var in s.scalars(select(Variant).where(Variant.product_id == product_id)):
                if variant_filters and any(
                    getattr(var, k, None) != v for k, v in variant_filters.items()
                ):
                    continue
                stock = s.get(Inventory, var.id)
                rows.append({**columns(var), "available": stock.available if stock else 0})
            return rows

    def _eligibility(self, s, order, payload, now):
        eligibility = s.get(Policy, order.policy_version)
        flow = s.scalar(select(Policy).where(Policy.current.is_(True)))
        require(eligibility is not None and flow is not None, "POLICY_UNAVAILABLE")
        check_window(payload["action"], order.status, order.delivered_at, now, eligibility.rules)
        info = {
            "eligibility_policy": eligibility.id,
            "flow_policy": flow.id,
            "policy_hash": digest([eligibility.content_hash, flow.content_hash]),
        }
        if payload["action"] == "cancel":
            occupied = s.scalar(
                select(OrderLine.id).where(OrderLine.order_id == order.id, OrderLine.occupied > 0)
            )
            require(occupied is None, "ACTIVE_AFTERCARE")
            info.update(
                refund_fen=order.paid_fen,
                shipping_fee_fen=0,
                result="CANCELLED",
                notice="仅记录模拟退款请求，不代表退款到账",
            )
            return info
        line = s.scalar(
            select(OrderLine)
            .where(OrderLine.id == payload["line_id"], OrderLine.order_id == order.id)
            .with_for_update()
        )
        require(line is not None, "NOT_FOUND")
        variant = s.get(Variant, line.variant_id)
        require(line.unused and getattr(variant, payload["action"] + "able"), "ITEM_INELIGIBLE")
        require(line.occupied + payload["quantity"] <= line.quantity, "QUANTITY_UNAVAILABLE")
        if payload["action"] == "exchange":
            target = s.get(Variant, payload["target_variant"])
            require(
                target is not None
                and target.product_id == variant.product_id
                and target.price_fen == variant.price_fen
                and target.id != variant.id,
                "INVALID_EXCHANGE_VARIANT",
            )
            stock = s.scalar(
                select(Inventory).where(Inventory.variant_id == target.id).with_for_update()
            )
            require(stock is not None and stock.available >= payload["quantity"], "OUT_OF_STOCK")
        fee = flow.rules["shipping"]["normal_" + payload["action"]]
        info.update(
            shipping_fee_fen=fee["quoted_fee_fen"],
            shipping_payer=fee["payer"],
            result=payload["action"].upper() + "_REQUESTED",
            notice="仅提交售后申请，尚未退款或寄出换货；运费仅模拟报价",
        )
        return info

    def preview(self, principal, conversation_id, action: ActionInput, *, require_case=False):
        payload = action.model_dump()
        if action.action == "cancel":
            require(action.line_id is None and action.target_variant is None, "INVALID_PARAMETERS")
        else:
            require(action.line_id is not None, "MISSING_LINE")
            require(
                action.action == "exchange" or action.target_variant is None, "INVALID_PARAMETERS"
            )
        parameter_hash = digest(payload)
        with self.sessions.begin() as s:
            conv = self._conversation(s, principal, conversation_id)
            if require_case:
                from aftercare.services.intent_guard import validate_case_for_preview

                validate_case_for_preview(s, conv, action)
            existing = s.scalar(
                select(Preview).where(
                    Preview.conversation_id == conv.id,
                    Preview.revision == conv.revision,
                    Preview.parameter_hash == parameter_hash,
                    Preview.status.in_(["PENDING", "CONFIRMED", "COMMITTED"]),
                )
            )
            if existing and existing.status == "COMMITTED":
                return columns(existing)
            order = s.scalar(
                select(Order)
                .where(Order.id == action.order_id, Order.customer_id == principal)
                .with_for_update()
            )
            require(order is not None, "NOT_FOUND")
            constraints = conv.case.get("facts", {}).get("constraints", {}).get("value", {})
            require(
                action.action == "cancel" or constraints.get("unused") is not False,
                "ITEM_INELIGIBLE",
            )
            info = self._eligibility(s, order, payload, self.clock())
            if (
                existing
                and existing.expires_at >= self.clock()
                and existing.order_version == order.version
                and existing.policy_hash == info["policy_hash"]
            ):
                return columns(existing)
            for row in s.scalars(
                select(Preview).where(
                    Preview.conversation_id == conv.id, Preview.status.in_(["PENDING", "CONFIRMED"])
                )
            ):
                row.status = "INVALIDATED"
            s.flush()
            row = Preview(
                id=new_id("prev"),
                conversation_id=conv.id,
                customer_id=principal,
                order_id=order.id,
                payload={**payload, "computed": info},
                parameter_hash=parameter_hash,
                revision=conv.revision,
                order_version=order.version,
                eligibility_policy=info["eligibility_policy"],
                flow_policy=info["flow_policy"],
                policy_hash=info["policy_hash"],
                status="PENDING",
                expires_at=self.clock() + timedelta(minutes=10),
            )
            s.add(row)
            conv.status = "WAITING_CONFIRMATION"
            s.flush()
            self.event(
                s, conv.id, "preview_created", {"preview_id": row.id, "payload": row.payload}
            )
            return columns(row)

    def _preview(self, s, principal, preview_id):
        initial = s.get(Preview, preview_id)
        require(initial is not None and initial.customer_id == principal, "NOT_FOUND")
        conv = self._conversation(s, principal, initial.conversation_id)
        row = s.scalar(
            select(Preview)
            .where(Preview.id == preview_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        return conv, row

    def _valid_preview(self, conv, row):
        require(row.status in {"PENDING", "CONFIRMED"}, "PREVIEW_INVALIDATED")
        require(row.expires_at >= self.clock(), "PREVIEW_EXPIRED")
        require(row.revision == conv.revision, "INTENT_CHANGED")

    def confirm(self, principal, preview_id, accept: bool, enqueue=None):
        with self.sessions.begin() as s:
            conv, row = self._preview(s, principal, preview_id)
            if row.status == "COMMITTED":
                return {"preview_id": row.id, "status": row.status}
            self._valid_preview(conv, row)
            if accept:
                order = s.scalar(select(Order).where(Order.id == row.order_id).with_for_update())
                require(order is not None and order.version == row.order_version, "STATE_CHANGED")
                info = self._eligibility(s, order, row.payload, self.clock())
                require(info["policy_hash"] == row.policy_hash, "POLICY_CHANGED")
            if not accept:
                row.status, conv.status = "REJECTED", "WAITING_USER"
            elif row.status != "CONFIRMED":
                row.status = "CONFIRMED"
                s.add(
                    Approval(
                        preview_id=row.id,
                        customer_id=principal,
                        parameter_hash=row.parameter_hash,
                        revision=row.revision,
                        confirmed_at=self.clock(),
                        method="trusted-api",
                    )
                )
            binding = {
                "preview_id": row.id,
                "principal": principal,
                "parameter_hash": row.parameter_hash,
                "intent_revision": row.revision,
                "decision": "approve" if accept else "reject",
            }
            event = self.event(
                s,
                conv.id,
                "preview_confirmed" if accept else "preview_rejected",
                {**binding, "revision": conv.revision},
            )
            s.flush()
            binding["event_id"] = event.id
            row.confirmation_event = binding
            conv.status = "WAITING_SUBMISSION" if accept else "WAITING_USER"
            if enqueue:
                enqueue(s, conv, row)
            return {"preview_id": row.id, "status": row.status, "confirmation": binding}

    def commit(self, principal, preview_id, idempotency_key=None, fault=None):
        fault = fault or getattr(self, "commit_fault", None)
        probe = getattr(self, "commit_probe", None)
        if probe:
            if self.sessions.kw["bind"].url.database != "aftercare_test":
                raise RuntimeError("RELIABILITY_TEST_DATABASE_REQUIRED")
            probe.hit("before_transaction", preview_id=preview_id)
        key = idempotency_key or preview_id
        request_hash = digest({"preview_id": preview_id})
        with self.sessions.begin() as s:
            s.execute(
                text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
                {"key": principal + ":" + key},
            )
            conv, row = self._preview(s, principal, preview_id)
            prior_key = s.get(IdempotencyBinding, (principal, key))
            if prior_key:
                require(prior_key.request_hash == request_hash, "IDEMPOTENCY_CONFLICT")
                return s.get(Operation, prior_key.operation_id).receipt
            prior = s.scalar(select(Operation).where(Operation.preview_id == preview_id))
            if prior:
                s.add(
                    IdempotencyBinding(
                        customer_id=principal,
                        key=key,
                        request_hash=request_hash,
                        operation_id=prior.id,
                    )
                )
                return prior.receipt
            self._valid_preview(conv, row)
            approval = s.get(Approval, preview_id)
            require(
                row.status == "CONFIRMED"
                and approval is not None
                and approval.customer_id == principal
                and approval.revision == conv.revision
                and approval.parameter_hash == row.parameter_hash,
                "APPROVAL_REQUIRED",
            )
            payload = {k: v for k, v in row.payload.items() if k != "computed"}
            require(digest(payload) == row.parameter_hash, "PARAMETER_HASH_MISMATCH")
            order = s.scalar(
                select(Order)
                .where(Order.id == row.order_id, Order.customer_id == principal)
                .with_for_update()
            )
            require(order is not None and order.version == row.order_version, "STATE_CHANGED")
            try:
                info = self._eligibility(s, order, payload, self.clock())
            except BusinessError as error:
                if error.code in {"OUT_OF_STOCK", "INELIGIBLE_STATUS", "QUANTITY_UNAVAILABLE"}:
                    raise BusinessError("STATE_CHANGED", error.code) from None
                raise
            require(
                info["policy_hash"] == row.policy_hash and info["flow_policy"] == row.flow_policy,
                "POLICY_CHANGED",
            )
            op_id, application_id = new_id("op"), None
            if payload["action"] == "cancel":
                order.status = "CANCELLED"
                terminal = "CANCELLED"
            else:
                application_id = new_id("app")
                terminal = payload["action"].upper() + "_REQUESTED"
                line = s.get(OrderLine, payload["line_id"])
                line.occupied += payload["quantity"]
            receipt = {
                "operation_id": op_id,
                "preview_id": row.id,
                "status": "COMMITTED",
                "result": terminal,
                "order_id": order.id,
                "application_id": application_id,
                "notice": info["notice"],
            }
            s.add(
                Operation(
                    id=op_id,
                    preview_id=row.id,
                    customer_id=principal,
                    idempotency_key=key,
                    request_hash=request_hash,
                    receipt=receipt,
                    created_at=self.clock(),
                )
            )
            s.flush()
            s.add(
                IdempotencyBinding(
                    customer_id=principal, key=key, request_hash=request_hash, operation_id=op_id
                )
            )
            if payload["action"] == "cancel":
                s.add(Refund(id=new_id("refund"), operation_id=op_id, amount_fen=order.paid_fen))
            else:
                s.add(
                    Application(
                        id=application_id,
                        operation_id=op_id,
                        line_id=payload["line_id"],
                        action=payload["action"],
                        quantity=payload["quantity"],
                        target_variant=payload["target_variant"],
                        status=terminal,
                    )
                )
                s.flush()
                if payload["action"] == "exchange":
                    stock = s.get(Inventory, payload["target_variant"])
                    stock.available -= payload["quantity"]
                    stock.reserved += payload["quantity"]
                    stock.version += 1
                    s.add(
                        Reservation(
                            application_id=application_id,
                            variant_id=stock.variant_id,
                            quantity=payload["quantity"],
                        )
                    )
                    s.add(
                        ExchangeFulfillment(
                            application_id=application_id,
                            origin="native",
                            created_at=self.clock(),
                            updated_at=self.clock(),
                        )
                    )
                    s.add(
                        FulfillmentAudit(
                            id=new_id("audit"),
                            application_id=application_id,
                            command_id=None,
                            actor_id=principal,
                            actor_role="customer",
                            actor_source="trusted_confirmation",
                            command="application_created",
                            from_state=None,
                            to_state=terminal,
                            from_version=None,
                            to_version=1,
                            details={
                                "operation_id": op_id,
                                "preview_id": preview_id,
                                "request_hash": request_hash,
                            },
                            created_at=self.clock(),
                        )
                    )
            order.version += 1
            row.status, conv.status = "COMMITTED", "COMPLETED"
            self.event(s, conv.id, "operation_committed", receipt)
            s.flush()
            if fault == "before_commit":
                raise RuntimeError("INJECTED_TRANSACTION_FAILURE")
        if probe:
            probe.hit("after_commit", preview_id=preview_id, operation_id=op_id)
        if fault == "lost_response":
            with self.sessions.begin() as s:
                self._conversation(s, principal, row.conversation_id)
                self.event(
                    s,
                    row.conversation_id,
                    "fault_occurred",
                    {
                        "kind": "lost_response",
                        "preview_id": preview_id,
                        "operation_id": op_id,
                        "after_commit": True,
                    },
                )
            raise BusinessError("WRITE_RESULT_UNKNOWN", "结果查询 operation/preview 回执")
        return receipt

    def get_operation(self, principal, operation_id=None, preview_id=None):
        with self.sessions() as s:
            q = select(Operation).where(Operation.customer_id == principal)
            q = (
                q.where(Operation.id == operation_id)
                if operation_id
                else q.where(Operation.preview_id == preview_id)
            )
            row = s.scalar(q)
            require(row is not None, "NOT_FOUND")
            return row.receipt

    def handoff(self, principal, conversation_id, reason_code, summary):
        with self.sessions.begin() as s:
            conv = self._conversation(s, principal, conversation_id)
            existing = s.scalar(
                select(Handoff).where(
                    Handoff.conversation_id == conv.id,
                    Handoff.revision == conv.revision,
                    Handoff.reason_code == reason_code,
                )
            )
            if existing:
                return columns(existing)
            row = Handoff(
                id=new_id("ticket"),
                conversation_id=conv.id,
                revision=conv.revision,
                reason_code=reason_code[:80],
                summary=summary[:1500],
                status="HANDOFF_PENDING",
            )
            s.add(row)
            conv.status = "HANDOFF_PENDING"
            s.flush()
            self.event(s, conv.id, "handoff_created", {"ticket_id": row.id, "reason": reason_code})
            return columns(row)

    def _preview_views(self, s, conv):
        previews = list(s.scalars(select(Preview).where(Preview.conversation_id == conv.id)))
        target_ids = {
            p.payload["target_variant"] for p in previews if p.payload.get("target_variant")
        }
        variants = (
            {
                variant.id: variant
                for variant in s.scalars(select(Variant).where(Variant.id.in_(target_ids)))
            }
            if target_ids
            else {}
        )
        return [
            {
                **columns(p),
                "target_label": (
                    f"{variants.get(p.payload['target_variant']).color} "
                    f"{variants.get(p.payload['target_variant']).size} 码"
                    if p.payload.get("target_variant")
                    else None
                ),
                "confirmation_available": (
                    p.status in {"PENDING", "CONFIRMED"}
                    and p.revision == conv.revision
                    and p.expires_at >= self.clock()
                ),
            }
            for p in previews
        ]

    def snapshot(self, principal, conversation_id):
        with self.sessions.begin() as s:
            conv = self._conversation(s, principal, conversation_id)
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
                "previews": self._preview_views(s, conv),
            }
