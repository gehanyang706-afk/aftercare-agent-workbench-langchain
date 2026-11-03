"""CI must fail when PostgreSQL coverage is skipped, narrowed or incomplete."""

import json
import xml.etree.ElementTree as ET

import pytest

from scripts import backend_d4_ci_check as ci


def environment():
    return {
        "ALLOW_REAL_MODEL_CALLS": "false",
        "DATABASE_URL": ci.TEST_URL,
        "AFTERCARE_TEST_DATABASE_URL": ci.TEST_URL,
    }


def report(tmp_path, *, count=842, all_modules=True, skipped=(), failed=None):
    root = ET.Element("testsuites")
    suite = ET.SubElement(root, "testsuite")
    modules = sorted(ci.REQUIRED_MODULES) if all_modules else ["tests.unit.only"]
    for index in range(count):
        case = ET.SubElement(
            suite,
            "testcase",
            classname=modules[index % len(modules)],
            name="test_case_" + str(index),
        )
        if index == 0 and failed:
            ET.SubElement(case, failed, message="regression failed")
    for module, reason in skipped:
        case = ET.SubElement(suite, "testcase", classname="", name=module)
        ET.SubElement(case, "skipped", message="collection skipped").text = reason
    path = tmp_path / "junit.xml"
    ET.ElementTree(root).write(path)
    return path


def test_exact_test_environment_is_accepted_without_connecting():
    result = ci.validate_environment(environment())
    assert result["status"] == "PASSED"
    assert "password" not in json.dumps(result) and "aftercare_local_only" not in json.dumps(result)


@pytest.mark.parametrize(
    "url",
    [
        ci.TEST_URL.replace("55434", "55432"),
        ci.TEST_URL.replace("55434", "55433"),
        ci.TEST_URL.replace("aftercare_test", "aftercare_demo"),
        ci.TEST_URL.replace("127.0.0.1", "localhost"),
        ci.TEST_URL + "?hostaddr=192.0.2.1",
        ci.TEST_URL + "?host=/var/run/postgresql",
        None,
    ],
)
@pytest.mark.parametrize("key", ["DATABASE_URL", "AFTERCARE_TEST_DATABASE_URL"])
def test_both_database_inputs_must_be_the_exact_isolated_target(url, key):
    env = environment()
    env[key] = url
    with pytest.raises(ci.CheckFailed, match="EXACT_ISOLATED_TEST_DATABASE_REQUIRED"):
        ci.validate_environment(env)


@pytest.mark.parametrize("value", [None, "true", "False", "0"])
def test_model_disable_must_be_explicit(value):
    env = environment()
    env["ALLOW_REAL_MODEL_CALLS"] = value
    with pytest.raises(ci.CheckFailed, match="EXPLICIT_MODEL_OFF_REQUIRED"):
        ci.validate_environment(env)


def test_full_junit_accepts_only_the_existing_optional_tau2_collection_skips(tmp_path):
    path = report(
        tmp_path,
        skipped=[
            (module, "Skipped: could not import 'tau2': No module named 'tau2'")
            for module in ci.OPTIONAL_MODULES
        ],
    )
    result = ci.audit_junit(path)
    assert result["passed"] == 842 and result["tests"] == 846
    assert set(result["optional_tau2_collection_skips"]) == ci.OPTIONAL_MODULES
    assert set(result["required_modules_present"]) == ci.REQUIRED_MODULES


@pytest.mark.parametrize("kind", ["failure", "error"])
def test_any_failed_test_stays_failed_even_with_the_full_count(tmp_path, kind):
    with pytest.raises(ci.CheckFailed, match="CI_REGRESSION_FAILED"):
        ci.audit_junit(report(tmp_path, count=843, failed=kind))


@pytest.mark.parametrize(
    "module,reason",
    [
        ("tests.integration.test_outbox", "Dedicated PostgreSQL aftercare_test must be started"),
        ("tests.integration.test_fulfillment", "temporary unavailable"),
        ("tests.benchmark_contract.test_bridge", "could not import 'torch'"),
        ("tests.new_optional", "could not import 'tau2': No module named 'tau2'"),
    ],
)
def test_database_or_new_optional_skips_cannot_turn_ci_green(tmp_path, module, reason):
    with pytest.raises(ci.CheckFailed, match="CI_UNEXPECTED_SKIP_REFUSED"):
        ci.audit_junit(report(tmp_path, skipped=[(module, reason)]))


def test_unit_only_run_cannot_replace_required_transaction_and_http_coverage(tmp_path):
    with pytest.raises(ci.CheckFailed, match="CI_REQUIRED_REGRESSION_COVERAGE_MISSING"):
        ci.audit_junit(report(tmp_path, all_modules=False))


def test_small_subset_cannot_replace_the_previous_full_baseline(tmp_path):
    with pytest.raises(ci.CheckFailed, match="CI_REGRESSION_COUNT_TOO_SMALL"):
        ci.audit_junit(report(tmp_path, count=841))


def test_ci_count_floor_cannot_be_lowered_to_allow_a_subset(tmp_path):
    with pytest.raises(ci.CheckFailed, match="CI_MINIMUM_CANNOT_BE_LOWERED"):
        ci.audit_junit(report(tmp_path), minimum_passed=1)


def test_missing_or_empty_junit_is_not_success(tmp_path):
    with pytest.raises(ci.CheckFailed, match="CI_JUNIT_MISSING_OR_INVALID"):
        ci.audit_junit(tmp_path / "missing.xml")
    with pytest.raises(ci.CheckFailed, match="CI_EMPTY_REGRESSION_REFUSED"):
        ci.audit_junit(report(tmp_path, count=0))


def test_duplicate_cases_cannot_inflate_passed_count(tmp_path):
    path = report(tmp_path)
    tree = ET.parse(path)
    case = tree.getroot().find("testsuite/testcase")
    tree.getroot().find("testsuite").append(case)
    tree.write(path)
    with pytest.raises(ci.CheckFailed, match="CI_DUPLICATE_TESTCASE_REFUSED"):
        ci.audit_junit(path)


def test_failed_preflight_records_a_secret_safe_failure(tmp_path, monkeypatch, capsys):
    secret = "should-never-appear-provider-token"
    for key, value in environment().items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(ci, "install_offline_guard", lambda: None)

    def fail():
        raise RuntimeError("postgresql://user:" + secret + "@unavailable")

    monkeypatch.setattr(ci, "probe_database", fail)
    path = tmp_path / "preflight.json"
    assert ci.main(["preflight", "--output", str(path)]) == 1
    saved = path.read_text()
    assert json.loads(saved) == {"status": "FAILED", "error_code": "CI_CHECK_FAILED"}
    assert secret not in saved + capsys.readouterr().out


def test_existing_first_evidence_is_never_overwritten(tmp_path, monkeypatch, capsys):
    for key, value in environment().items():
        monkeypatch.setenv(key, value)
    monkeypatch.setattr(ci, "install_offline_guard", lambda: None)
    path = tmp_path / "target-first.json"
    args = ["target", "--output", str(path)]
    assert ci.main(args) == 0
    original = path.read_bytes()
    assert ci.main(args) == 1 and path.read_bytes() == original
    assert "CI_EVIDENCE_WRITE_REFUSED" in capsys.readouterr().out


def test_wrong_target_never_calls_the_database_probe(tmp_path, monkeypatch):
    for key, value in environment().items():
        monkeypatch.setenv(key, value)
    monkeypatch.setenv("DATABASE_URL", ci.TEST_URL.replace("aftercare_test", "aftercare_demo"))

    def forbidden():
        pytest.fail("Target rejection must happen before a database connection")

    monkeypatch.setattr(ci, "probe_database", forbidden)
    path = tmp_path / "failure.json"
    assert ci.main(["preflight", "--output", str(path)]) == 1
    assert json.loads(path.read_text())["error_code"] == "EXACT_ISOLATED_TEST_DATABASE_REQUIRED"
