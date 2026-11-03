from alembic import context
from sqlalchemy import create_engine

from aftercare.config import Settings

config = context.config
settings = config.attributes.get("settings") or Settings()
target_metadata = None

if context.is_offline_mode():
    context.configure(
        url=settings.database_url.get_secret_value(),
        target_metadata=target_metadata,
        literal_binds=True,
    )
    with context.begin_transaction():
        context.run_migrations()
else:

    def run_with_connection(connection):
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            version_table_schema=config.attributes.get("version_table_schema"),
        )
        with context.begin_transaction():
            context.run_migrations()

    supplied_connection = config.attributes.get("connection")
    if supplied_connection is not None:
        # Trusted migration callers may keep a private-schema transaction open.
        # The caller owns its connection and commit/rollback; never close it here.
        run_with_connection(supplied_connection)
    else:
        engine = create_engine(
            settings.database_url.get_secret_value(),
            connect_args={"connect_timeout": 2, "options": "-c statement_timeout=5000"},
        )
        try:
            with engine.connect() as connection:
                run_with_connection(connection)
        finally:
            engine.dispose()
