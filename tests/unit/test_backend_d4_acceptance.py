"""D4 evidence tooling must not accept an old DB or erase migration history."""

import base64
import json
import subprocess
from copy import deepcopy
from types import SimpleNamespace

import httpx
import pytest

from scripts import backend_d4_acceptance as acceptance
from scripts.backend_d4_acceptance import (
    DATABASE,
    assert_legacy_preserved,
    check_schema,
    load_state,
    validate_environment,
    write_new,
)


@pytest.mark.parametrize(
    "database",
    [
        DATABASE.replace("55436", "55434"),
        DATABASE.replace("55436", "55432"),
        DATABASE.replace("55436", "55433"),
        DATABASE.replace("aftercare_test", "aftercare_demo"),
        DATABASE + "?options=-csearch_path=public",
        DATABASE.replace("127.0.0.1", "localhost"),
        DATABASE.replace("aftercare:aftercare_local_only", "postgres:other"),
    ],
)
def test_old_or_ambiguous_database_is_rejected_before_connecting(database):
    with pytest.raises(RuntimeError, match="EXACT_D4_TEST_DATABASE_REQUIRED"):
        validate_environment({"DATABASE_URL": database, "ALLOW_REAL_MODEL_CALLS": "false"})


def test_exact_d4_target_also_requires_explicit_model_off():
    for value in ["true", "False", "0", None]:
        with pytest.raises(RuntimeError, match="EXPLICIT_OFFLINE_REQUIRED"):
            validate_environment({"DATABASE_URL": DATABASE, "ALLOW_REAL_MODEL_CALLS": value})
    validate_environment({"DATABASE_URL": DATABASE, "ALLOW_REAL_MODEL_CALLS": "false"})


@pytest.mark.parametrize(
    "schema", ["public", "d4_migration_", "d4_migration_abc;DROP", "other_12345678"]
)
def test_migration_cannot_fall_back_to_public_or_interpolate_arbitrary_schema(schema):
    with pytest.raises(RuntimeError, match="PRIVATE_MIGRATION_SCHEMA_REQUIRED"):
        check_schema(schema)
    assert check_schema("d4_migration_1234abcd") == "d4_migration_1234abcd"


def test_existing_first_evidence_is_not_overwritten(tmp_path):
    target = tmp_path / "result.json"
    write_new(target, {"status": "FAILED", "first_evidence": True})
    original = target.read_bytes()
    with pytest.raises(FileExistsError):
        write_new(target, {"status": "PASSED"})
    assert target.read_bytes() == original
    assert target.stat().st_mode & 0o777 == 0o600


def test_legacy_unknown_budget_or_original_receipt_change_blocks_migration_acceptance():
    rows = {
        "budgets": [{"settled": 42, "reserved": 99, "paused": True}],
        "operations": [{"receipt": {"preserve": ["历史", None, 7]}}],
    }
    attempt = {"send_state": "LEGACY_UNKNOWN", "send_started_at": None}
    assert_legacy_preserved({"old_rows": rows}, deepcopy(rows), attempt)
    for field, value in [("reserved", 0), ("paused", False)]:
        changed = deepcopy(rows)
        changed["budgets"][0][field] = value
        with pytest.raises(RuntimeError, match="MIGRATION_REWROTE_PRIOR_FACTS"):
            assert_legacy_preserved({"old_rows": rows}, changed, attempt)
    changed = deepcopy(rows)
    changed["operations"][0]["receipt"]["preserve"][0] = "rewritten"
    with pytest.raises(RuntimeError, match="MIGRATION_REWROTE_PRIOR_FACTS"):
        assert_legacy_preserved({"old_rows": rows}, changed, attempt)


@pytest.mark.parametrize(
    "attempt",
    [
        {"send_state": "NOT_STARTED", "send_started_at": None},
        {"send_state": "STARTED", "send_started_at": "2026-10-30T00:00:00+00:00"},
        {"send_state": "LEGACY_UNKNOWN", "send_started_at": "2026-10-30T00:00:00+00:00"},
    ],
)
def test_migration_must_not_invent_old_execution_send_permission(attempt):
    with pytest.raises(RuntimeError, match="MIGRATION_INFERRED_LEGACY_SEND_PERMISSION"):
        assert_legacy_preserved({"old_rows": {}}, {}, attempt)


def test_state_cannot_be_loaded_from_outside_runtime(tmp_path):
    with pytest.raises(RuntimeError, match="OUTPUT_MUST_BE_RUNTIME"):
        load_state(tmp_path / "other-project-state.json")


def test_provider_exec_preserves_http_error_body_and_sends_secret_only_on_stdin(monkeypatch):
    calls = []
    secret = "never-put-this-provider-token-in-argv-or-env"
    body = b'{"code":"NOT_FOUND","accepted":false}'
    monkeypatch.setenv("WAREHOUSE_TOKEN", secret)
    monkeypatch.setenv("UNRELATED_MODEL_KEY", secret)

    def run(argv, **kwargs):
        calls.append((argv, kwargs))
        packet = json.loads(kwargs["input"])
        assert packet["authorization"] == "Bearer " + secret
        assert packet["path"] == "/mock-warehouse/shipments/by-key/exchange%3Aapp-test"
        assert all(secret not in value for value in argv)
        assert all(secret not in value for value in kwargs["env"].values())
        return SimpleNamespace(
            returncode=0,
            stdout=json.dumps(
                {
                    "status": 404,
                    "headers": {"Content-Type": "application/json"},
                    "body_b64": base64.b64encode(body).decode(),
                }
            ).encode(),
            stderr=b"",
        )

    client = httpx.Client(
        base_url="http://127.0.0.1:8152",
        transport=acceptance.DockerLoopbackTransport(run=run),
        headers={"Authorization": "Bearer " + secret},
    )
    response = client.get("/mock-warehouse/shipments/by-key/exchange%3Aapp-test")
    assert response.status_code == 404 and response.content == body
    argv, kwargs = calls[0]
    assert argv[:5] == ["docker", "exec", "-i", "aftercare-d4-test-warehouse-1", "python"]
    assert 0 < kwargs["timeout"] <= 8 and not kwargs.get("shell", False)


def test_provider_exec_preserves_post_body_without_changing_the_wire_hash():
    wire = {"effect_key": "exchange:app-test", "quantity": 1, "request_hash": "a" * 64}

    def run(argv, **kwargs):
        packet = json.loads(kwargs["input"])
        assert (
            packet["method"] == "POST" and json.loads(base64.b64decode(packet["body_b64"])) == wire
        )
        return SimpleNamespace(
            returncode=0, stdout=b'{"status":200,"headers":{},"body_b64":"e30="}', stderr=b""
        )

    with httpx.Client(transport=acceptance.DockerLoopbackTransport(run=run)) as client:
        result = client.post(
            "http://127.0.0.1:8152/mock-warehouse/shipments",
            json=wire,
            headers={"Authorization": "Bearer " + "x" * 40},
        )
    assert result.status_code == 200 and result.json() == {}


@pytest.mark.parametrize(
    "url,method",
    [
        ("http://192.0.2.1:8152/mock-warehouse/shipments/by-key/a", "GET"),
        ("http://warehouse:8152/mock-warehouse/shipments/by-key/a", "GET"),
        ("https://127.0.0.1:8152/mock-warehouse/shipments", "POST"),
        ("http://127.0.0.1:8153/mock-warehouse/shipments", "POST"),
        ("http://127.0.0.1:8152/__test/faults", "POST"),
        ("http://127.0.0.1:8152/mock-warehouse/shipments", "DELETE"),
        ("http://127.0.0.1:8152/mock-warehouse/shipments?redirect=1", "POST"),
    ],
)
def test_provider_exec_refuses_nonloopback_or_unapproved_operations_before_spawning(url, method):
    def forbidden(*args, **kwargs):
        pytest.fail("Rejected provider request must never execute Docker")

    with httpx.Client(transport=acceptance.DockerLoopbackTransport(run=forbidden)) as client:
        with pytest.raises(httpx.TransportError, match="D4_PROVIDER_REQUEST_DENIED"):
            client.request(method, url, headers={"Authorization": "Bearer " + "x" * 40})


def test_provider_exec_timeout_is_bounded_and_does_not_echo_exception_output():
    secret = "never-echo-provider-secret"

    def timeout(argv, **kwargs):
        raise subprocess.TimeoutExpired(argv, kwargs["timeout"], output=secret, stderr=secret)

    with httpx.Client(transport=acceptance.DockerLoopbackTransport(run=timeout)) as client:
        with pytest.raises(httpx.ReadTimeout, match="D4_PROVIDER_DOCKER_TIMEOUT") as caught:
            client.get(
                "http://127.0.0.1:8152/mock-warehouse/shipments/by-key/a",
                headers={"Authorization": "Bearer " + "x" * 40},
            )
    assert secret not in str(caught.value)
