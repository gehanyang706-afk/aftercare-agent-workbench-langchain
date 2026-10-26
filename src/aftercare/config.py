"""Validated, secret-safe configuration; loading settings creates no clients."""

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, SecretStr, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import make_url

PROJECT_ROOT = Path(__file__).resolve().parents[2]


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
        }


class HealthResult(BaseModel):
    model_config = ConfigDict(extra="forbid")
    status: str
    code: str
