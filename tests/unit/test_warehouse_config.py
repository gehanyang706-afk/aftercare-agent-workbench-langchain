"""C1's local HTTP endpoint and bounded credential/lease configuration."""

import pytest
from pydantic import ValidationError

from aftercare.config import Settings

TOKEN = "C1-local-warehouse-test-token-123456789"


def configured(**overrides):
    values = {
        "warehouse_delivery_enabled": True,
        "warehouse_simulation_enabled": False,
        "warehouse_token": TOKEN,
    }
    values.update(overrides)
    return Settings(_env_file=None, **values)


def test_delivery_is_disabled_by_default_and_public_summary_does_not_expose_token():
    default = Settings(_env_file=None)
    assert default.warehouse_delivery_enabled is False
    assert default.warehouse_simulation_enabled is False
    enabled = configured()
    assert enabled.public_summary()["warehouse_delivery_enabled"] is True
    assert "warehouse_token" not in enabled.public_summary()
    assert TOKEN not in repr(enabled)
    assert TOKEN not in str(enabled.public_summary())


@pytest.mark.parametrize(
    "url",
    [
        "http://127.0.0.1:8140",
        "http://127.2.3.4:8140",
        "http://[::1]:8140",
    ],
)
def test_only_literal_loopback_http_endpoints_are_accepted(url):
    assert configured(warehouse_base_url=url).warehouse_base_url == url


@pytest.mark.parametrize(
    "url",
    [
        "https://127.0.0.1:8140",
        "http://localhost:8140",
        "http://example.com:8140",
        "http://192.168.1.1:8140",
        "http://0.0.0.0:8140",
        "http://[::]:8140",
        "http://2130706433:8140",
        "http://user@127.0.0.1:8140",
        "http://user:password@127.0.0.1:8140",
        "http://127.0.0.1:8140/",
        "http://127.0.0.1:8140/shipments",
        "http://127.0.0.1:8140?token=secret",
        "http://127.0.0.1:8140#fragment",
        "http://127.0.0.1:8140?",
        "http://127.0.0.1:8140#",
        "http://127.0.0.1",
        "http://127.0.0.1:80",
        "http://127.0.0.1:65536",
        " http://127.0.0.1:8140",
        "http://127.0.0.1:8140\n",
    ],
)
def test_nonlocal_or_ambiguous_delivery_endpoints_are_rejected(url):
    with pytest.raises(ValidationError):
        configured(warehouse_base_url=url)


def test_delivery_and_in_process_simulation_are_mutually_exclusive():
    with pytest.raises(ValidationError, match="WAREHOUSE_DELIVERY_MODES_CONFLICT"):
        configured(warehouse_simulation_enabled=True)


@pytest.mark.parametrize(
    "token",
    [
        "",
        "x" * 31,
        "x" * 257,
        "x" * 31 + " ",
        "x" * 31 + "\n",
        "x" * 31 + "中",
    ],
)
def test_delivery_requires_a_bounded_nonblank_visible_ascii_token(token):
    with pytest.raises(ValidationError, match="WAREHOUSE_TOKEN_REQUIRED") as error:
        configured(warehouse_token=token)
    if token:
        assert token not in str(error.value)


@pytest.mark.parametrize(
    "values",
    [
        {"warehouse_timeout_seconds": 0.09},
        {"warehouse_timeout_seconds": 5.01},
        {"outbox_lease_seconds": 2},
        {"outbox_lease_seconds": 301},
        {"outbox_max_attempts": 0},
        {"outbox_max_attempts": 11},
        {"warehouse_timeout_seconds": 2.0, "outbox_lease_seconds": 5},
    ],
)
def test_timeout_lease_and_attempt_limits_are_bounded(values):
    with pytest.raises(ValidationError):
        configured(**values)


def test_lease_can_equal_three_times_the_http_timeout():
    settings = configured(warehouse_timeout_seconds=2.0, outbox_lease_seconds=6)
    assert settings.outbox_lease_seconds == 3 * settings.warehouse_timeout_seconds
