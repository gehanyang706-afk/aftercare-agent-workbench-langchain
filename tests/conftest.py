import os
from datetime import UTC, datetime

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text

from aftercare.config import PROJECT_ROOT, Settings
from aftercare.repositories.database import session_factory
from aftercare.repositories.models import Base
from aftercare.services.business import BusinessService
from aftercare.services.demo import seed

TEST_URL = "postgresql+psycopg://aftercare:aftercare_local_only@127.0.0.1:55434/aftercare_test"
FIXED = datetime(2026, 10, 17, 4, 0, tzinfo=UTC)


@pytest.fixture(scope="session")
def database_sessions():
    settings = Settings(
        _env_file=None, database_url=os.getenv("AFTERCARE_TEST_DATABASE_URL", TEST_URL)
    )
    sessions = session_factory(settings)
    if sessions.kw["bind"].url.database != "aftercare_test":
        raise RuntimeError("TEST_DATABASE_MUST_BE_AFTERCARE_TEST")
    try:
        with sessions() as s:
            s.execute(text("SELECT 1"))
    except Exception:
        pytest.skip("Dedicated PostgreSQL aftercare_test must be started")
    config = Config(str(PROJECT_ROOT / "alembic.ini"))
    config.attributes["settings"] = settings
    command.upgrade(config, "head")
    # The metadata migration is frozen in the next phase; phase-1 dev schema adds its own tables.
    yield sessions
    sessions.kw["bind"].dispose()


@pytest.fixture
def service(database_sessions):
    names = ", ".join('"' + t.name + '"' for t in Base.metadata.sorted_tables)
    with database_sessions.begin() as s:
        s.execute(text(f"TRUNCATE {names} RESTART IDENTITY CASCADE"))
    seed(database_sessions, lambda: FIXED)
    return BusinessService(database_sessions, lambda: FIXED)
