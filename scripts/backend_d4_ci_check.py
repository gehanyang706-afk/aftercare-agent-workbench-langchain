"""Fail closed when offline CI lacks its isolated PostgreSQL or regression coverage.

Dependency installation is a separate online preparation step. These checks and
the regression do not permit model calls. This script never migrates or resets a
database; the workflow validates the target before its existing migration CLI.
"""

import argparse
import hashlib
import json
import os
import platform
import xml.etree.ElementTree as ET
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.engine import make_url

from aftercare.config import PROJECT_ROOT, Settings
from aftercare.offline import install_offline_guard
from aftercare.repositories.database import BASELINE_REVISION, database_health, session_factory

TEST_URL = "postgresql+psycopg://aftercare:aftercare_local_only@127.0.0.1:55434/aftercare_test"
OPTIONAL_MODULES = {
    "tests.benchmark_contract.test_bridge",
    "tests.benchmark_contract.test_official_protocol",
    "tests.benchmark_contract.test_tool_visibility",
    "tests.benchmark_contract.test_visibility_integration",
}
REQUIRED_MODULES = {
    "tests.integration.test_fulfillment",
    "tests.integration.test_fulfillment_migration",
    "tests.integration.test_staff_auth",
    "tests.integration.test_outbox",
    "tests.integration.test_warehouse_recovery",
    "tests.integration.test_dispatch_intent",
    "tests.integration.test_worker_concurrency",
    "tests.integration.test_read_queries",
    "tests.e2e.test_fulfillment_api",
    "tests.e2e.test_outbox_api",
    "tests.e2e.test_warehouse_recovery_api",
    "tests.e2e.test_read_snapshot",
    "tests.e2e.test_sse_reads",
    "tests.unit.test_worker_observation",
    "tests.unit.test_ci_contract",
    "tests.unit.test_backend_runtime_lock",
    "tests.unit.test_backend_d4_acceptance",
    "tests.integration.test_migration_connection",
}


class CheckFailed(ValueError):
    pass


def require(condition, code):
    if not condition:
        raise CheckFailed(code)


def validate_environment(environ):
    """No permissive fallbacks: check both inputs before a caller may migrate."""
    require(environ.get("ALLOW_REAL_MODEL_CALLS") == "false", "EXPLICIT_MODEL_OFF_REQUIRED")
    for name in ("DATABASE_URL", "AFTERCARE_TEST_DATABASE_URL"):
        raw = environ.get(name)
        require(raw == TEST_URL, "EXACT_ISOLATED_TEST_DATABASE_REQUIRED")
        # The literal equality also rejects query/hostaddr overrides and aliases.
        url = make_url(raw)
        require(
            url.drivername == "postgresql+psycopg"
            and url.host == "127.0.0.1"
            and url.port == 55434
            and url.database == "aftercare_test"
            and url.username == "aftercare"
            and not url.query,
            "EXACT_ISOLATED_TEST_DATABASE_REQUIRED",
        )
    return {
        "status": "PASSED",
        "database": "127.0.0.1:55434/aftercare_test",
        "real_model_calls_allowed": False,
    }


def probe_database():
    """Read only the dedicated test database; no skip-on-error behavior."""
    settings = Settings(
        _env_file=None,
        database_url=TEST_URL,
        allow_real_model_calls=False,
        offline=True,
        warehouse_delivery_enabled=False,
        warehouse_simulation_enabled=False,
        warehouse_callbacks_enabled=False,
    )
    require(database_health(settings).status == "ready", "CI_DATABASE_NOT_READY")
    sessions = session_factory(settings)
    try:
        with sessions() as session:
            session.execute(text("SET TRANSACTION READ ONLY"))
            identity = session.execute(text("SELECT current_database(),current_user")).one()
            require(tuple(identity) == ("aftercare_test", "aftercare"), "CI_DATABASE_IDENTITY_BAD")
            version_num = int(session.scalar(text("SHOW server_version_num")))
            require(170000 <= version_num < 180000, "CI_POSTGRESQL_17_REQUIRED")
            revision = session.scalar(text("SELECT version_num FROM alembic_version"))
            require(revision == BASELINE_REVISION, "CI_SCHEMA_REVISION_BAD")
            require(
                session.scalar(text("SELECT count(*) FROM model_calls")) == 0,
                "CI_NONEMPTY_MODEL_CALL_LEDGER_REFUSED",
            )
            return {
                "status": "PASSED",
                "database": "127.0.0.1:55434/aftercare_test",
                "schema_revision": revision,
                "postgresql_version": session.scalar(text("SHOW server_version")),
                "role": identity[1],
                "real_model_call_records": 0,
                "python": platform.python_version(),
                "requirements_sha256": hashlib.sha256(
                    (PROJECT_ROOT / "requirements.lock").read_bytes()
                ).hexdigest(),
            }
    finally:
        sessions.kw["bind"].dispose()


def audit_junit(path, minimum_passed=842):
    """A green unit-only run or database-wide skip cannot satisfy full CI."""
    require(type(minimum_passed) is int and minimum_passed >= 842, "CI_MINIMUM_CANNOT_BE_LOWERED")
    try:
        root = ET.parse(path).getroot()
    except (OSError, ET.ParseError):
        raise CheckFailed("CI_JUNIT_MISSING_OR_INVALID") from None
    require(root.tag in {"testsuites", "testsuite"}, "CI_JUNIT_ROOT_INVALID")
    cases = list(root.iter("testcase"))
    require(bool(cases), "CI_EMPTY_REGRESSION_REFUSED")
    seen, modules, skips = set(), set(), []
    failures = errors = passed = 0
    for case in cases:
        module, name = case.get("classname", ""), case.get("name", "")
        identity = (module, name)
        require(identity not in seen, "CI_DUPLICATE_TESTCASE_REFUSED")
        seen.add(identity)
        failures += case.find("failure") is not None
        errors += case.find("error") is not None
        skip = case.find("skipped")
        if skip is not None:
            reason = (skip.get("message", "") + " " + (skip.text or "")).strip()
            require(
                module == ""
                and name in OPTIONAL_MODULES
                and "could not import 'tau2'" in reason
                and "No module named 'tau2'" in reason,
                "CI_UNEXPECTED_SKIP_REFUSED",
            )
            skips.append(name)
        elif case.find("failure") is None and case.find("error") is None:
            passed += 1
            modules.add(module)
    require(not failures and not errors, "CI_REGRESSION_FAILED")
    require(passed >= minimum_passed, "CI_REGRESSION_COUNT_TOO_SMALL")
    require(REQUIRED_MODULES <= modules, "CI_REQUIRED_REGRESSION_COVERAGE_MISSING")
    return {
        "status": "PASSED",
        "tests": len(cases),
        "passed": passed,
        "failures": failures,
        "errors": errors,
        "optional_tau2_collection_skips": sorted(skips),
        "required_modules_present": sorted(REQUIRED_MODULES),
        "minimum_passed": minimum_passed,
        "junit_sha256": hashlib.sha256(Path(path).read_bytes()).hexdigest(),
    }


def write_once(path, report):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x") as output:
        json.dump(report, output, ensure_ascii=False, indent=2)
        output.write("\n")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("target", "preflight", "audit"))
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--junit", type=Path)
    args = parser.parse_args(argv)
    try:
        validate_environment(os.environ)
        if args.action == "audit":
            require(args.junit is not None, "CI_JUNIT_ARGUMENT_REQUIRED")
            report = audit_junit(args.junit)
        else:
            install_offline_guard()
            report = (
                validate_environment(os.environ) if args.action == "target" else probe_database()
            )
    except CheckFailed as error:
        report = {"status": "FAILED", "error_code": str(error)}
    except Exception:
        # Connector exceptions can contain a DSN. Keep the diagnostic secret-safe.
        report = {"status": "FAILED", "error_code": "CI_CHECK_FAILED"}
    try:
        write_once(args.output, report)
    except OSError:
        print(json.dumps({"status": "FAILED", "error_code": "CI_EVIDENCE_WRITE_REFUSED"}))
        return 1
    print(json.dumps(report, ensure_ascii=False))
    return 0 if report["status"] == "PASSED" else 1


if __name__ == "__main__":
    raise SystemExit(main())
