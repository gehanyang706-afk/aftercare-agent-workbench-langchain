"""Strict warehouse wire types and the product-compatible canonical request digest."""

import hashlib
import hmac
import json
import re
import secrets
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

PROVIDER_ID = "mock-warehouse-v1"


def shipment_request_hash(
    effect_key: str, application_id: str, variant_id: str, quantity: int
) -> str:
    payload = {
        "effect_key": effect_key,
        "application_id": application_id,
        "variant_id": variant_id,
        "quantity": quantity,
    }
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()
    ).hexdigest()


class ShipmentInput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    effect_key: str = Field(min_length=1, max_length=200, pattern=r"\S")
    application_id: str = Field(min_length=1, max_length=100, pattern=r"\S")
    variant_id: str = Field(min_length=1, max_length=100, pattern=r"\S")
    quantity: int = Field(ge=1, le=1000)
    request_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    def request_payload(self) -> dict:
        return self.model_dump(exclude={"request_hash"})


class ShipmentResult(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    effect_key: str = Field(min_length=1, max_length=200)
    application_id: str = Field(min_length=1, max_length=100)
    request_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    external_shipment_id: str = Field(min_length=1, max_length=100, pattern=r"\S")
    status: Literal["SUCCEEDED"]


def verify_shipment_hash(shipment: ShipmentInput) -> bool:
    expected = shipment_request_hash(**shipment.request_payload())
    return secrets.compare_digest(shipment.request_hash, expected)


class WarehouseEvent(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    event_id: str = Field(min_length=1, max_length=200, pattern=r"\S")
    effect_key: str = Field(min_length=1, max_length=200, pattern=r"\S")
    application_id: str = Field(min_length=1, max_length=100, pattern=r"\S")
    request_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    external_shipment_id: str = Field(min_length=1, max_length=100, pattern=r"\S")
    sequence: int = Field(ge=1, le=2147483647)
    status: Literal["ACCEPTED", "SUCCEEDED"]


def validate_callback_secret(secret: str) -> str:
    if not (
        isinstance(secret, str)
        and 32 <= len(secret) <= 256
        and all(33 <= ord(character) <= 126 for character in secret)
    ):
        raise ValueError("WAREHOUSE_CALLBACK_SECRET_REQUIRED")
    return secret


def sign_event(secret: str, raw_body: bytes, timestamp: str | int) -> str:
    validate_callback_secret(secret)
    if (
        not isinstance(raw_body, bytes)
        or type(timestamp) not in {str, int}
        or re.fullmatch(r"[0-9]{1,20}", str(timestamp)) is None
    ):
        raise ValueError("INVALID_SIGNING_INPUT")
    content = str(timestamp).encode("ascii") + b"." + raw_body
    return "v1=" + hmac.new(secret.encode("ascii"), content, hashlib.sha256).hexdigest()


def verify_event_signature(
    secret: str, raw_body: bytes, timestamp: str | int, signature: str
) -> bool:
    if not isinstance(signature, str) or re.fullmatch(r"v1=[0-9a-f]{64}", signature) is None:
        return False
    try:
        expected = sign_event(secret, raw_body, timestamp)
    except ValueError:
        return False
    return secrets.compare_digest(signature, expected)
