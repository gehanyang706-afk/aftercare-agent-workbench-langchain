import json
import os
import subprocess
import sys

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from aftercare.api.app import create_app
from aftercare.config import Settings
from aftercare.llm.gateway import RealModelCallsDisabled, create_real_client


def test_live_has_no_database_or_inference_dependency(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("live must not connect to dependencies")

    monkeypatch.setattr("aftercare.api.app.database_health", forbidden)
    with TestClient(create_app(Settings(_env_file=None))) as client:
        assert client.get("/health/live").json()["status"] == "alive"


def test_unavailable_database_is_503_and_redacted():
    settings = Settings(
        _env_file=None,
        database_url="postgresql+psycopg://person:private-secret@127.0.0.1:1/aftercare_test",
        dashscope_api_key="key-secret",
    )
    with TestClient(create_app(settings)) as client:
        response = client.get("/health/ready")
    assert response.status_code == 503
    assert response.json()["code"] == "DATABASE_UNAVAILABLE"
    assert "private-secret" not in response.text
    assert "key-secret" not in response.text


@pytest.mark.parametrize(
    "url",
    [
        "postgresql+psycopg://u:p@remote.test/db",
        "postgresql+psycopg://u:p@127.0.0.1/db?host=remote.test",
        "postgresql+psycopg://u:p@127.0.0.1/db?hostaddr=1.1.1.1",
    ],
)
def test_remote_database_refused_before_any_connection(monkeypatch, url):
    def forbidden(*args, **kwargs):
        raise AssertionError("remote connection attempted")

    monkeypatch.setattr("aftercare.repositories.database.create_engine", forbidden)
    settings = Settings(_env_file=None, database_url=url)
    with TestClient(create_app(settings)) as client:
        assert client.get("/health/ready").json()["code"] == "OFFLINE_DATABASE_HOST_DENIED"


@pytest.mark.parametrize("requested_flag", [True, False])
def test_real_model_denied_even_when_flag_is_true(requested_flag):
    settings = Settings(_env_file=None, allow_real_model_calls=requested_flag)
    with pytest.raises(RealModelCallsDisabled):
        create_real_client(settings)
    assert settings.public_summary()["real_model_calls_allowed"] is False


def test_invalid_settings_do_not_echo_credentials():
    with pytest.raises(ValidationError) as error:
        Settings(_env_file=None, database_url="sqlite:///sensitive-data.db")
    assert "sensitive-data" not in str(error.value)


def test_diagnostics_are_secret_safe():
    env = {
        **os.environ,
        "DATABASE_URL": "postgresql+psycopg://u:DB_SECRET@127.0.0.1:1/db",
        "DASHSCOPE_API_KEY": "API_SECRET",
        "DASHSCOPE_BASE_URL": "https://SECRET_HOST/v1",
    }
    result = subprocess.run(
        [sys.executable, "-m", "aftercare", "doctor", "--offline"],
        env=env,
        capture_output=True,
        text=True,
        timeout=15,
    )
    assert result.returncode == 1
    assert json.loads(result.stdout)["settings"]["api_key_configured"] is True
    assert all(
        secret not in result.stdout + result.stderr
        for secret in ("DB_SECRET", "API_SECRET", "SECRET_HOST")
    )


def test_offline_audit_guard_blocks_egress_in_child_process():
    code = """
import socket
from aftercare.offline import install_offline_guard
install_offline_guard()
checks = [lambda: socket.getaddrinfo("example.com", 443),
          lambda: socket.create_connection(("1.1.1.1", 443), timeout=1),
          lambda: socket.socket(socket.AF_INET, socket.SOCK_DGRAM).sendto(b"x", ("1.1.1.1", 53))]
for check in checks:
    try:
        check()
    except PermissionError as error:
        assert str(error) == "OFFLINE_EGRESS_DENIED"
    else:
        raise AssertionError("egress escaped guard")
print("all blocked")
"""
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, timeout=10
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "all blocked"


def test_imports_and_startup_do_not_open_network():
    code = """
import sys
attempts = []
def audit(event, args):
    if event in ("socket.connect", "socket.getaddrinfo", "socket.sendto"):
        attempts.append(event)
        raise AssertionError("startup network attempt")
sys.addaudithook(audit)
import aftercare
import aftercare.cli
from langchain.agents import create_agent
from langgraph.checkpoint.postgres import PostgresSaver
from aftercare.api.app import create_app
from aftercare.config import Settings
create_app(Settings(_env_file=None))
assert not attempts
print("zero network attempts")
"""
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True, timeout=10
    )
    assert result.returncode == 0, result.stderr
    assert "zero network attempts" in result.stdout


def test_migration_cli_refuses_unknown_database():
    result = subprocess.run(
        [sys.executable, "-m", "aftercare", "db", "migrate"],
        env={**os.environ, "DATABASE_URL": "postgresql+psycopg://u:p@127.0.0.1/production"},
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert result.returncode == 2
    assert "DEDICATED_LOCAL_DATABASE_REQUIRED" in result.stdout
