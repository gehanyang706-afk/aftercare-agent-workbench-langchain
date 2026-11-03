"""Small, bounded readiness probe. Neither import nor app creation connects."""

from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

from aftercare.config import HealthResult, Settings
from aftercare.offline import is_loopback

BASELINE_REVISION = "0011_dispatch_intent"


def is_local_database(url) -> bool:
    if "hostaddr" in url.query:
        return False
    if "host" in url.query:
        return url.host is None and url.query["host"] == "/var/run/postgresql"
    return is_loopback(url.host)


def database_health(settings: Settings) -> HealthResult:
    url = make_url(settings.database_url.get_secret_value())
    if settings.offline and not is_local_database(url):
        return HealthResult(status="not_ready", code="OFFLINE_DATABASE_HOST_DENIED")
    engine = create_engine(
        url,
        connect_args={"connect_timeout": 2, "options": "-c statement_timeout=2000"},
        pool_pre_ping=True,
    )
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
            vector = connection.scalar(
                text("SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname='vector')")
            )
            if not vector:
                return HealthResult(status="not_ready", code="PGVECTOR_MISSING")
            revision_table = connection.scalar(text("SELECT to_regclass('alembic_version')"))
            if not revision_table:
                return HealthResult(status="not_ready", code="MIGRATIONS_REQUIRED")
            revisions = set(connection.scalars(text("SELECT version_num FROM alembic_version")))
            if revisions != {BASELINE_REVISION}:
                return HealthResult(status="not_ready", code="SCHEMA_REVISION_MISMATCH")
        return HealthResult(status="ready", code="OK")
    except Exception:
        # SQLAlchemy/psycopg exceptions may contain credentials/DSNs. Never echo them.
        return HealthResult(status="not_ready", code="DATABASE_UNAVAILABLE")
    finally:
        engine.dispose()


def session_factory(settings: Settings):
    from sqlalchemy.orm import sessionmaker

    url = make_url(settings.database_url.get_secret_value())
    if settings.offline and not is_local_database(url):
        raise ValueError("OFFLINE_DATABASE_HOST_DENIED")
    engine = create_engine(
        url,
        pool_pre_ping=True,
        connect_args={
            "connect_timeout": 2,
            "options": "-c statement_timeout=10000 -c lock_timeout=5000",
        },
    )
    return sessionmaker(engine, expire_on_commit=False)
