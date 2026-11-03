"""Explicit demo/test-only fixtures and simulation events; never Agent tools."""

from datetime import timedelta

import yaml
from sqlalchemy import select, text
from sqlalchemy.engine import make_url

from aftercare.config import PROJECT_ROOT
from aftercare.domain.contracts import utc_now
from aftercare.domain.rules import require
from aftercare.repositories.models import (
    Application,
    Customer,
    ExchangeFulfillment,
    FulfillmentAudit,
    Inventory,
    Order,
    OrderLine,
    Policy,
    Reservation,
    Variant,
)
from aftercare.services.business import digest


def assert_dedicated(sessions):
    with sessions() as s:
        url = make_url(str(s.bind.url))
        require(url.database in {"aftercare_demo", "aftercare_test"}, "DEDICATED_DATABASE_REQUIRED")
        require(s.scalar(text("SELECT current_user")) == "aftercare", "DEDICATED_ROLE_REQUIRED")
        require(
            s.scalar(text("SELECT to_regclass('action_previews')")) is not None,
            "PROJECT_SCHEMA_REQUIRED",
        )


def seed(sessions, clock=utc_now):
    assert_dedicated(sessions)
    rules = yaml.safe_load((PROJECT_ROOT / "configs/business-rules.v1.yaml").read_text())
    with sessions.begin() as s:
        if s.get(Customer, "alice"):
            return {"status": "already_seeded", "customers": ["alice", "bob", "carol"]}
        s.add(Policy(id="v1", rules=rules, content_hash=digest(rules), current=True))
        s.add_all(
            [
                Customer(id=k, name=v)
                for k, v in [("alice", "小林"), ("bob", "小陈"), ("carol", "小周")]
            ]
        )
        s.flush()
        for variant_id, product, title, color, size, price, ordinary in [
            ("tee-white-M", "tee", "基础纯棉短袖", "白色", "M", 9900, True),
            ("tee-white-L", "tee", "基础纯棉短袖", "白色", "L", 9900, True),
            ("tee-black-M", "tee", "基础纯棉短袖", "黑色", "M", 9900, True),
            ("tee-white-XL", "tee", "基础纯棉短袖", "白色", "XL", 9900, True),
            ("shoe-black-40", "shoe", "轻便运动鞋", "黑色", "40", 29900, True),
            ("shoe-black-41", "shoe", "轻便运动鞋", "黑色", "41", 29900, True),
            ("custom-M", "custom", "定制印字短袖", "白色", "M", 12900, False),
        ]:
            s.add(
                Variant(
                    id=variant_id,
                    product_id=product,
                    title=title,
                    color=color,
                    size=size,
                    price_fen=price,
                    returnable=ordinary,
                    exchangeable=ordinary,
                    category="ordinary" if ordinary else "custom",
                )
            )
        s.flush()
        s.add_all(
            [
                Inventory(variant_id=v, available=n, reserved=0, version=1)
                for v, n in [
                    ("tee-white-M", 5),
                    ("tee-white-L", 1),
                    ("tee-black-M", 3),
                    ("tee-white-XL", 0),
                    ("shoe-black-40", 4),
                    ("shoe-black-41", 2),
                    ("custom-M", 1),
                ]
            ]
        )
        for oid, customer, state, age, variant, quantity in [
            ("S1001", "alice", "PAID_UNSHIPPED", None, "tee-white-M", 2),
            ("S1002", "alice", "DELIVERED", 2, "tee-white-M", 2),
            ("S1003", "alice", "DELIVERED", 10, "shoe-black-40", 1),
            ("S1004", "alice", "SHIPPED", None, "tee-white-M", 1),
            ("S1005", "alice", "DELIVERED", 20, "tee-white-M", 1),
            ("S2001", "bob", "DELIVERED", 2, "tee-white-M", 1),
            ("S3001", "carol", "DELIVERED", 1, "custom-M", 1),
        ]:
            price = s.get(Variant, variant).price_fen
            s.add(
                Order(
                    id=oid,
                    customer_id=customer,
                    status=state,
                    paid_fen=price * quantity,
                    delivered_at=clock() - timedelta(days=age) if age else None,
                    version=1,
                    policy_version="v1",
                )
            )
            s.flush()
            s.add(
                OrderLine(
                    id=oid + "-1",
                    order_id=oid,
                    variant_id=variant,
                    quantity=quantity,
                    occupied=0,
                    unused=True,
                )
            )
    return {"status": "seeded", "customers": ["alice", "bob", "carol"]}


def ship_order(sessions, order_id):
    assert_dedicated(sessions)
    with sessions.begin() as s:
        order = s.scalar(select(Order).where(Order.id == order_id).with_for_update())
        require(order is not None and order.status == "PAID_UNSHIPPED", "INELIGIBLE_STATUS")
        order.status, order.version = "SHIPPED", order.version + 1


def settle_application(sessions, application_id, decision):
    assert_dedicated(sessions)
    require(decision in {"REJECTED", "WITHDRAWN", "COMPLETED"}, "INVALID_DECISION")
    with sessions.begin() as s:
        initial = s.get(Application, application_id)
        require(initial is not None, "NOT_FOUND")
        line_info = s.get(OrderLine, initial.line_id)
        s.scalar(select(Order).where(Order.id == line_info.order_id).with_for_update())
        app = s.scalar(
            select(Application)
            .where(Application.id == application_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if app.status == decision:
            return {"application_id": app.id, "status": app.status}
        require(app.status in {"RETURN_REQUESTED", "EXCHANGE_REQUESTED"}, "APPLICATION_CLOSED")
        line = s.scalar(select(OrderLine).where(OrderLine.id == app.line_id).with_for_update())
        if decision in {"REJECTED", "WITHDRAWN"}:
            line.occupied -= app.quantity
        reservation = s.get(Reservation, app.id)
        if reservation and reservation.status == "RESERVED":
            stock = s.scalar(
                select(Inventory)
                .where(Inventory.variant_id == reservation.variant_id)
                .with_for_update()
            )
            stock.reserved -= app.quantity
            if decision != "COMPLETED":
                stock.available += app.quantity
            stock.version += 1
            reservation.status = "CONSUMED" if decision == "COMPLETED" else "RELEASED"
        app.status = decision
        if app.action == "exchange":
            # Legacy test/demo shortcut, never a formal fulfillment command or Agent tool.
            # Keep its bypass explicit in audit and do not fabricate warehouse evidence.
            from aftercare.services.business import new_id

            fulfillment = s.get(ExchangeFulfillment, app.id, with_for_update=True)
            require(fulfillment is not None, "INVENTORY_INVARIANT")
            prior_version = app.version
            app.version += 1
            fulfillment.updated_at = utc_now()
            s.add(
                FulfillmentAudit(
                    id=new_id("audit"),
                    application_id=app.id,
                    command_id=None,
                    actor_id="demo-helper",
                    actor_role="system",
                    actor_source="demo_helper",
                    command="demo_settle",
                    from_state="EXCHANGE_REQUESTED",
                    to_state=decision,
                    from_version=prior_version,
                    to_version=app.version,
                    details={"demo_only": True, "bypasses_fulfillment": True},
                    created_at=utc_now(),
                )
            )
        return {"application_id": app.id, "status": app.status}


def run_case(service, case):
    from concurrent.futures import ThreadPoolExecutor

    from aftercare.domain.contracts import ActionInput, BusinessError

    principal = "alice"
    conversation = service.create_conversation(principal)
    service.append_message(principal, conversation, "演示换大一码", "demo-start")
    action = ActionInput(
        action="exchange", order_id="S1002", line_id="S1002-1", target_variant="tee-white-L"
    )
    preview = service.preview(principal, conversation, action)
    if case == "revise-before-confirm":
        service.append_message(principal, conversation, "不换了，改退货", "demo-revise")
        try:
            service.confirm(principal, preview["id"], True)
        except BusinessError as e:
            return {"old_preview": preview["id"], "blocked": e.code}
        raise AssertionError("old confirmation accepted")
    service.confirm(principal, preview["id"], True)
    if case == "lost-submit-response":
        try:
            service.commit(principal, preview["id"], fault="lost_response")
        except BusinessError as e:
            require(e.code == "WRITE_RESULT_UNKNOWN", "UNEXPECTED_ERROR")
        return service.get_operation(principal, preview_id=preview["id"])
    if case == "concurrent-stock":
        other = service.create_conversation("bob")
        other_preview = service.preview(
            "bob",
            other,
            ActionInput(
                action="exchange", order_id="S2001", line_id="S2001-1", target_variant="tee-white-L"
            ),
        )
        service.confirm("bob", other_preview["id"], True)

        def execute(pair):
            try:
                return service.commit(*pair)
            except BusinessError as e:
                return {"blocked": e.code}

        with ThreadPoolExecutor(max_workers=2) as pool:
            return list(
                pool.map(execute, [(principal, preview["id"]), ("bob", other_preview["id"])])
            )
    receipt = service.commit(principal, preview["id"])
    replay = service.commit(principal, preview["id"])
    require(receipt == replay, "DUPLICATE_RECEIPT")
    return receipt


def reset_demo(sessions, confirmation):
    from sqlalchemy import text

    from aftercare.repositories.models import Base

    assert_dedicated(sessions)
    require(confirmation == "demo-only", "EXPLICIT_RESET_REQUIRED")
    require(sessions.kw["bind"].url.database == "aftercare_demo", "DEMO_DATABASE_REQUIRED")
    from aftercare.repositories.models import Job, ModelCall

    with sessions() as s:
        require(
            s.scalar(select(ModelCall.id).where(ModelCall.batch_id != "offline")) is None,
            "REAL_LEDGER_RESET_FORBIDDEN",
            "此演示库含已封存真实批次账本，禁止全库重置；请保留账本和业务数据。",
        )
        require(
            s.scalar(select(Job.id).where(Job.status.in_(["RUNNING", "PENDING"]))) is None,
            "STOP_OR_FINISH_ACTIVE_JOBS_BEFORE_RESET",
        )
    names = ", ".join('"' + table.name + '"' for table in Base.metadata.sorted_tables)
    with sessions.begin() as s:
        s.execute(text(f"TRUNCATE {names} RESTART IDENTITY CASCADE"))
    return seed(sessions)
