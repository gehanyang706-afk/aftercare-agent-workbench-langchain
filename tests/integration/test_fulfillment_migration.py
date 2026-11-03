"""Migrate historical facts inside a rolled-back private schema, never downgrade public."""

import importlib.util
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import MetaData, Table, inspect, select, text
from sqlalchemy.exc import IntegrityError

from aftercare.config import PROJECT_ROOT

NOW = datetime(2026, 10, 27, 4, tzinfo=UTC)
LEGACY_STATES = ("EXCHANGE_REQUESTED", "REJECTED", "WITHDRAWN", "COMPLETED")


def migration(revision):
    path = PROJECT_ROOT / "migrations/versions" / f"{revision}.py"
    spec = importlib.util.spec_from_file_location(f"migration_test_{revision}", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def upgrade(connection, revision):
    with Operations.context(MigrationContext.configure(connection)):
        migration(revision).upgrade()


def add_application(connection, tables, suffix, action="exchange", status="EXCHANGE_REQUESTED"):
    line, preview, operation, application = (
        f"{name}-{suffix}" for name in ("line", "pv", "op", "app")
    )
    connection.execute(
        tables["order_lines"].insert(),
        {
            "id": line,
            "order_id": "legacy-order",
            "variant_id": "legacy-old",
            "quantity": 1,
            "occupied": 0 if status in {"REJECTED", "WITHDRAWN"} else 1,
            "unused": True,
        },
    )
    connection.execute(
        tables["action_previews"].insert(),
        {
            "id": preview,
            "conversation_id": "legacy-conv",
            "customer_id": "legacy-customer",
            "order_id": "legacy-order",
            "payload": {"action": action, "quantity": 1, "preserve": [1, "旧申请"]},
            "parameter_hash": f"params-{suffix}",
            "revision": 1,
            "order_version": 1,
            "eligibility_policy": "legacy-policy",
            "flow_policy": "v1",
            "policy_hash": "frozen-policy",
            "status": "COMMITTED",
            "expires_at": NOW + timedelta(minutes=5),
        },
    )
    connection.execute(
        tables["approvals"].insert(),
        {
            "preview_id": preview,
            "customer_id": "legacy-customer",
            "parameter_hash": f"params-{suffix}",
            "revision": 1,
            "confirmed_at": NOW,
            "method": "trusted-api",
        },
    )
    connection.execute(
        tables["operations"].insert(),
        {
            "id": operation,
            "preview_id": preview,
            "customer_id": "legacy-customer",
            "idempotency_key": f"key-{suffix}",
            "request_hash": f"hash-{suffix}",
            "status": "COMMITTED",
            "receipt": {"result": status, "preserve": {"amount_fen": 123, "unknown": None}},
            "created_at": NOW,
        },
    )
    connection.execute(
        tables["idempotency_bindings"].insert(),
        {
            "customer_id": "legacy-customer",
            "key": f"key-{suffix}",
            "request_hash": f"hash-{suffix}",
            "operation_id": operation,
        },
    )
    connection.execute(
        tables["aftercare_requests"].insert(),
        {
            "id": application,
            "operation_id": operation,
            "line_id": line,
            "action": action,
            "quantity": 1,
            "target_variant": "legacy-new" if action == "exchange" else None,
            "status": status,
        },
    )
    if action == "exchange":
        connection.execute(
            tables["inventory_reservations"].insert(),
            {
                "application_id": application,
                "variant_id": "legacy-new",
                "quantity": 1,
                "status": (
                    "CONSUMED"
                    if status == "COMPLETED"
                    else "RELEASED"
                    if status in {"REJECTED", "WITHDRAWN"}
                    else "RESERVED"
                ),
            },
        )
    return application


def seed_history(connection, tables):
    rows = {
        "customers": {"id": "legacy-customer", "name": "历史客户"},
        "policies": {
            "id": "legacy-policy",
            "rules": {"frozen": True},
            "content_hash": "frozen-policy",
            "current": True,
        },
        "orders": {
            "id": "legacy-order",
            "customer_id": "legacy-customer",
            "status": "DELIVERED",
            "paid_fen": 500,
            "delivered_at": NOW - timedelta(days=1),
            "version": 7,
            "policy_version": "legacy-policy",
        },
        "conversations": {
            "id": "legacy-conv",
            "customer_id": "legacy-customer",
            "revision": 1,
            "status": "COMPLETED",
            "case": {"frozen": ["历史", 7]},
        },
        "budgets": {
            "id": "legacy-budget",
            "cap": 1000,
            "settled": 42,
            "reserved": 99,
            "paused": True,
        },
        "model_calls": {
            "id": "legacy-call",
            "budget_id": "legacy-budget",
            "provider": "fake-real-no-transport",
            "exact_model": "historical-offline-fixture",
            "batch_id": "legacy-batch",
            "conversation_id": "legacy-conv",
            "run_id": "legacy-run",
            "reserved": 99,
            "usage": None,
            "status": "UNKNOWN",
            "request_id": None,
            "details": {"preserve_unknown_reservation": True},
            "created_at": NOW,
        },
        "experiment_runs": {
            "id": "legacy-run",
            "plan_hash": "frozen-plan",
            "config": {"approved": "historical-only"},
            "deadline": NOW,
            "status": "PAUSED",
        },
        "shared_model_quotas": {
            "id": "legacy-quota",
            "confirmed_remaining": 777,
            "watermark": {"calls": 1},
            "confirmation": {"frozen": True},
            "paused": True,
        },
        "experiment_episodes": {
            "id": "legacy-episode",
            "run_id": "legacy-run",
            "conversation_id": "legacy-conv",
            "config": {"preserve": True},
            "deadline": NOW,
            "status": "PAUSED",
        },
        "experiment_actor_budgets": {
            "id": "legacy-actor-budget",
            "run_id": "legacy-run",
            "episode_id": "legacy-episode",
            "role": "agent",
            "cap": 1000,
            "request_cap": 10,
            "settled": 42,
            "reserved": 99,
            "requests": 1,
        },
        "experiment_calls": {
            "call_id": "legacy-call",
            "quota_id": "legacy-quota",
            "run_id": "legacy-run",
            "episode_id": "legacy-episode",
            "role": "agent",
            "attempt_id": "legacy-attempt",
            "dimensions": ["legacy-budget", "legacy-actor-budget"],
            "metadata": {"preserve": [1, None, "历史"]},
        },
    }
    for name, values in rows.items():
        connection.execute(tables[name].insert(), values)
    for name in ("legacy-old", "legacy-new"):
        connection.execute(
            tables["variants"].insert(),
            {
                "id": name,
                "product_id": "legacy-product",
                "title": name,
                "color": "white",
                "size": "M",
                "price_fen": 100,
                "returnable": True,
                "exchangeable": True,
                "category": "ordinary",
            },
        )
    connection.execute(
        tables["inventory"].insert(),
        {"variant_id": "legacy-new", "available": 10, "reserved": 1, "version": 4},
    )
    for state in LEGACY_STATES:
        add_application(connection, tables, state, status=state)
    add_application(connection, tables, "return", action="return", status="RETURN_REQUESTED")


@pytest.fixture
def legacy_schema(database_sessions):
    engine = database_sessions.kw["bind"]
    url = engine.url
    if (url.host, url.port, url.database, url.username) != (
        "127.0.0.1",
        55434,
        "aftercare_test",
        "aftercare",
    ):
        raise RuntimeError("ISOLATED_MIGRATION_TEST_DATABASE_REQUIRED")
    schema = "b1_migration_" + uuid.uuid4().hex
    with engine.connect() as connection:
        transaction = connection.begin()
        try:
            connection.execute(text(f'CREATE SCHEMA "{schema}"'))
            connection.execute(text(f'SET LOCAL search_path TO "{schema}"'))
            # Rebuild only the relevant frozen 0006-era tables. No public downgrade or truncation.
            for revision in ("0002_business", "0004_budget", "0005_web_jobs", "0006_experiments"):
                upgrade(connection, revision)
            metadata = MetaData()
            metadata.reflect(bind=connection)
            seed_history(connection, metadata.tables)
            yield connection, metadata.tables
        finally:
            transaction.rollback()


def snapshot_rows(connection, tables):
    return {
        name: [
            dict(row)
            for row in connection.execute(select(table).order_by(*table.primary_key)).mappings()
        ]
        for name, table in sorted(tables.items())
    }


def test_incremental_fulfillment_migration_preserves_history_and_unknown_ledgers(legacy_schema):
    connection, old_tables = legacy_schema
    before = snapshot_rows(connection, old_tables)
    upgrade(connection, "0007_exchange_fulfillment")
    assert snapshot_rows(connection, old_tables) == before
    assert set(connection.scalars(text("SELECT version FROM aftercare_requests"))) == {1}

    fulfillments = (
        connection.execute(text("SELECT * FROM exchange_fulfillments ORDER BY application_id"))
        .mappings()
        .all()
    )
    assert {row["application_id"] for row in fulfillments} == {f"app-{s}" for s in LEGACY_STATES}
    for row in fulfillments:
        assert row["origin"] == "legacy"
        for column in (
            "return_carrier",
            "return_tracking",
            "received_note",
            "received_at",
            "effect_key",
            "shipment_status",
            "external_shipment_id",
        ):
            assert row[column] is None
        assert row["created_at"] is not None and row["updated_at"] is not None

    audits = connection.execute(text("SELECT * FROM fulfillment_audits")).mappings().all()
    assert len(audits) == len(LEGACY_STATES)
    for row in audits:
        assert row["application_id"] == "app-" + row["to_state"]
        assert (row["actor_id"], row["actor_role"], row["actor_source"], row["command"]) == (
            "migration:0007",
            "system",
            "migration",
            "legacy_import",
        )
        assert row["command_id"] is None
        assert row["from_state"] is None and row["from_version"] is None
        assert row["to_version"] == 1 and row["details"] == {"legacy_import": True}
    assert connection.scalar(text("SELECT count(*) FROM fulfillment_commands")) == 0
    with pytest.raises(RuntimeError, match="FULFILLMENT_DOWNGRADE_FORBIDDEN"):
        migration("0007_exchange_fulfillment").downgrade()
    assert snapshot_rows(connection, old_tables) == before
    assert connection.scalar(text("SELECT count(*) FROM fulfillment_audits")) == len(LEGACY_STATES)


def test_fulfillment_migration_defaults_and_exchange_only_state_constraint(legacy_schema):
    connection, old_tables = legacy_schema
    upgrade(connection, "0007_exchange_fulfillment")
    application = add_application(connection, old_tables, "new-default")
    assert (
        connection.scalar(
            text("SELECT version FROM aftercare_requests WHERE id=:id"), {"id": application}
        )
        == 1
    )
    fulfillment = Table("exchange_fulfillments", MetaData(), autoload_with=connection)
    connection.execute(
        fulfillment.insert(), {"application_id": application, "created_at": NOW, "updated_at": NOW}
    )
    assert (
        connection.scalar(
            select(fulfillment.c.origin).where(fulfillment.c.application_id == application)
        )
        == "native"
    )
    for assignment in ("version=0", "status='NOT_AN_EXCHANGE_STATE'"):
        with pytest.raises(IntegrityError):
            with connection.begin_nested():
                connection.execute(
                    text(f"UPDATE aftercare_requests SET {assignment} WHERE id=:id"),
                    {"id": application},
                )
    connection.execute(
        text("UPDATE aftercare_requests SET status='LEGACY_RETURN_STATE' WHERE id='app-return'")
    )
    assert (
        connection.scalar(text("SELECT status FROM aftercare_requests WHERE id='app-return'"))
        == "LEGACY_RETURN_STATE"
    )


def test_unknown_historical_exchange_state_blocks_migration_without_rewriting(legacy_schema):
    connection, old_tables = legacy_schema
    add_application(connection, old_tables, "unknown", status="UNRECOGNIZED_OLD_EXCHANGE_STATE")
    before = snapshot_rows(connection, old_tables)
    with pytest.raises(IntegrityError):
        with connection.begin_nested():
            upgrade(connection, "0007_exchange_fulfillment")
    assert snapshot_rows(connection, old_tables) == before
    assert "version" not in {
        c["name"] for c in inspect(connection).get_columns("aftercare_requests")
    }
    assert "exchange_fulfillments" not in inspect(connection).get_table_names()
