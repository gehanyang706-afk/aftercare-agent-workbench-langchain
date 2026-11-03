"""Trusted callers can migrate a private schema without closing their transaction."""

from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text

from aftercare.config import PROJECT_ROOT, Settings

pytestmark = pytest.mark.integration


def test_external_connection_keeps_private_version_and_caller_rollback(database_sessions):
    engine = database_sessions.kw["bind"]
    schema = "d4_migration_" + uuid4().hex
    with engine.connect() as connection:
        public_before = connection.scalar(text("SELECT version_num FROM public.alembic_version"))
        connection.rollback()
        transaction = connection.begin()
        try:
            connection.execute(text(f'CREATE SCHEMA "{schema}"'))
            connection.execute(text(f'SET LOCAL search_path TO "{schema}",public'))
            config = Config(str(PROJECT_ROOT / "alembic.ini"))
            config.attributes.update(
                settings=Settings(
                    _env_file=None,
                    database_url=engine.url.render_as_string(hide_password=False),
                ),
                connection=connection,
                version_table_schema=schema,
            )
            command.upgrade(config, "0006_experiments")
            connection.execute(
                text(f'INSERT INTO "{schema}".customers(id,name) VALUES (:id,:name)'),
                {"id": "d4-caller-owned", "name": "迁移事务"},
            )
            command.upgrade(config, "head")
            assert not connection.closed and transaction.is_active
            assert (
                connection.scalar(text(f'SELECT version_num FROM "{schema}".alembic_version'))
                == "0011_dispatch_intent"
            )
            assert (
                connection.scalar(
                    text(f'SELECT name FROM "{schema}".customers WHERE id=:id'),
                    {"id": "d4-caller-owned"},
                )
                == "迁移事务"
            )
            assert (
                connection.scalar(text("SELECT version_num FROM public.alembic_version"))
                == public_before
            )
        finally:
            transaction.rollback()
        assert not connection.closed
        assert (
            connection.scalar(
                text("SELECT count(*) FROM pg_namespace WHERE nspname=:schema"), {"schema": schema}
            )
            == 0
        )
        assert (
            connection.scalar(text("SELECT version_num FROM public.alembic_version"))
            == public_before
        )
