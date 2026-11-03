"""Validated, secret-safe configuration; loading settings creates no clients."""

import ipaddress
from pathlib import Path
from typing import Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import make_url

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def validate_warehouse_url(value: str) -> str:
    """C1 permits literal loopback HTTP only, with no proxy or redirect target."""
    try:
        parsed = urlsplit(value)
        address = ipaddress.ip_address(parsed.hostname or "")
        valid = (
            parsed.scheme == "http"
            and address.is_loopback
            and parsed.username is None
            and parsed.password is None
            and parsed.path == ""
            and not parsed.query
            and not parsed.fragment
            and "?" not in value
            and "#" not in value
            and parsed.port is not None
            and 1024 <= parsed.port <= 65535
            and not any(c.isspace() for c in value)
        )
    except (ValueError, TypeError):
        valid = False
    if not valid:
        raise ValueError("WAREHOUSE_URL_MUST_BE_LITERAL_LOOPBACK_HTTP")
    return value


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=PROJECT_ROOT / ".env", extra="ignore", hide_input_in_errors=True
    )

    app_env: Literal["local", "test"] = "local"
    allow_real_model_calls: bool = False
    dashscope_api_key: SecretStr = SecretStr("")
    dashscope_base_url: SecretStr = SecretStr("")
    database_url: SecretStr = SecretStr(
        "postgresql+psycopg://aftercare:aftercare_local_only@127.0.0.1:55435/aftercare_demo"
    )
    offline: bool = True
    dev_faults: bool = False
    demo_login_enabled: bool = False
    warehouse_simulation_enabled: bool = False
    warehouse_delivery_enabled: bool = False
    warehouse_base_url: str = "http://127.0.0.1:8140"
    warehouse_token: SecretStr = SecretStr("")
    warehouse_timeout_seconds: float = Field(default=2.0, ge=0.1, le=5.0)
    outbox_lease_seconds: int = Field(default=15, ge=3, le=300)
    outbox_max_attempts: int = Field(default=3, ge=1, le=10)
    warehouse_callbacks_enabled: bool = False
    warehouse_callback_secret: SecretStr = SecretStr("")
    reconciliation_max_attempts: int = Field(default=3, ge=1, le=10)
    reconciliation_deadline_seconds: int = Field(default=60, ge=5, le=600)
    reconciliation_max_cycles: int = Field(default=3, ge=1, le=10)

    @field_validator("warehouse_base_url")
    @classmethod
    def local_warehouse(cls, value: str) -> str:
        return validate_warehouse_url(value)

    @model_validator(mode="after")
    def warehouse_mode(self):
        if self.warehouse_delivery_enabled:
            if self.warehouse_simulation_enabled:
                raise ValueError("WAREHOUSE_DELIVERY_MODES_CONFLICT")
            token = self.warehouse_token.get_secret_value()
            if not 32 <= len(token) <= 256 or any(not 33 <= ord(c) <= 126 for c in token):
                raise ValueError("WAREHOUSE_TOKEN_REQUIRED")
            if self.outbox_lease_seconds < 3 * self.warehouse_timeout_seconds:
                raise ValueError("OUTBOX_LEASE_MUST_EXCEED_HTTP_TIMEOUT")
        if self.warehouse_callbacks_enabled:
            secret = self.warehouse_callback_secret.get_secret_value()
            if not 32 <= len(secret) <= 256 or any(not 33 <= ord(c) <= 126 for c in secret):
                raise ValueError("WAREHOUSE_CALLBACK_SECRET_REQUIRED")
            if (
                self.warehouse_delivery_enabled
                and secret == self.warehouse_token.get_secret_value()
            ):
                raise ValueError("WAREHOUSE_CALLBACK_SECRET_MUST_DIFFER")
        return self

    @field_validator("database_url")
    @classmethod
    def validate_database_url(cls, value: SecretStr) -> SecretStr:
        try:
            url = make_url(value.get_secret_value())
        except Exception:
            raise ValueError("DATABASE_URL must be a PostgreSQL psycopg URL") from None
        if url.drivername != "postgresql+psycopg" or not url.database:
            raise ValueError("DATABASE_URL must use postgresql+psycopg and name a database")
        return value

    def public_summary(self) -> dict:
        return {
            "app_env": self.app_env,
            "offline": self.offline,
            "real_model_calls_allowed": False,
            "requested_real_model_flag": self.allow_real_model_calls,
            "api_key_configured": bool(self.dashscope_api_key.get_secret_value()),
            "base_url_configured": bool(self.dashscope_base_url.get_secret_value()),
            "database_configured": bool(self.database_url.get_secret_value()),
            "phase": 5,
            "demo_login_enabled": self.demo_login_enabled,
            "warehouse_simulation_enabled": self.warehouse_simulation_enabled,
            "warehouse_delivery_enabled": self.warehouse_delivery_enabled,
            "warehouse_callbacks_enabled": self.warehouse_callbacks_enabled,
        }


class HealthResult(BaseModel):
    model_config = ConfigDict(extra="forbid")
    status: str
    code: str
