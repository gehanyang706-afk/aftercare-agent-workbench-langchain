"""Apply original Alembic revisions only inside a new D4 acceptance schema.

Public must already be current. No downgrade, reset, drop or database creation.
The normal container startup uses the existing `aftercare db migrate` command.
"""

import argparse
import json
import os
import time

from alembic import command
from alembic.config import Config
from sqlalchemy import text

from aftercare.config import PROJECT_ROOT, Settings
from aftercare.offline import install_offline_guard
from aftercare.repositories.database import session_factory
from scripts.backend_d4_acceptance import (
    DATABASE,
    check_schema,
    require,
    runtime_path,
    validate_environment,
    write_new,
)

REVISIONS = ["0006_experiments", "0010_warehouse_recovery", "0011_dispatch_intent"]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--revision", choices=REVISIONS, required=True)
    parser.add_argument("--create", action="store_true")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    validate_environment(os.environ)
    check_schema(args.schema)
    output = runtime_path(args.output)
    require(not output.exists(), "EVIDENCE_EXISTS")
    require(output.parent.is_dir(), "EVIDENCE_PARENT_REQUIRED")
    require(not args.create or args.revision == REVISIONS[0], "CREATE_ONLY_AT_FIRST_REVISION")
    install_offline_guard()
    settings = Settings(_env_file=None, database_url=DATABASE, allow_real_model_calls=False)
    sessions = session_factory(settings)
    report = {"status": "RUNNING", "schema": args.schema, "target": args.revision}
    started = time.monotonic()
    try:
        with sessions.kw["bind"].begin() as connection:
            public = connection.scalar(text("SELECT version_num FROM public.alembic_version"))
            require(public == REVISIONS[-1], "PUBLIC_MUST_ALREADY_BE_CURRENT")
            exists = connection.scalar(
                text("SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname=:name)"),
                {"name": args.schema},
            )
            require(not exists if args.create else exists, "SCHEMA_CREATE_OR_EXISTENCE_MISMATCH")
            if args.create:
                connection.execute(text(f'CREATE SCHEMA "{args.schema}"'))
            else:
                prior = connection.scalar(
                    text(f'SELECT version_num FROM "{args.schema}".alembic_version')
                )
                require(REVISIONS.index(prior) < REVISIONS.index(args.revision), "FORWARD_ONLY")
                report["before_revision"] = prior
            connection.execute(text(f'SET LOCAL search_path TO "{args.schema}",public'))
            config = Config(str(PROJECT_ROOT / "alembic.ini"))
            config.attributes.update(
                settings=settings, connection=connection, version_table_schema=args.schema
            )
            command.upgrade(config, args.revision)
            require(
                connection.scalar(text(f'SELECT version_num FROM "{args.schema}".alembic_version'))
                == args.revision,
                "PRIVATE_REVISION_MISMATCH",
            )
            require(
                connection.scalar(text("SELECT version_num FROM public.alembic_version")) == public,
                "PUBLIC_REVISION_CHANGED",
            )
        report.update(status="PASSED", public_revision_unchanged=True)
    except Exception as error:
        report.update(status="FAILED", error_code=type(error).__name__)
    finally:
        sessions.kw["bind"].dispose()
        report["elapsed_seconds"] = time.monotonic() - started
        write_new(output, report)
    print(json.dumps(report))
    return 0 if report["status"] == "PASSED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
