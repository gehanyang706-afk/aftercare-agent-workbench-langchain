"""Domain command contract; Actor is supplied by trusted application code, not a DTO."""

from dataclasses import dataclass
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


@dataclass(frozen=True)
class Actor:
    actor_id: str
    role: str
    source: str


class FulfillmentCommandInput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    command: Literal[
        "approve",
        "reject",
        "withdraw",
        "submit_return",
        "receive",
        "request_shipment",
        "confirm_shipment",
        "complete",
    ]
    expected_version: int = Field(ge=1)
    idempotency_key: str = Field(min_length=1, max_length=100, pattern=r"\S")
    data: dict = Field(default_factory=dict)


class CommandData(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, str_strip_whitespace=True)


class NoteData(CommandData):
    note: str = Field(default="", max_length=500)


class ReturnData(CommandData):
    carrier: str = Field(min_length=1, max_length=100)
    tracking_number: str = Field(min_length=1, max_length=100)


class ShipmentData(CommandData):
    external_shipment_id: str = Field(min_length=1, max_length=100)


DATA_MODELS = {
    "approve": NoteData,
    "reject": NoteData,
    "withdraw": CommandData,
    "submit_return": ReturnData,
    "receive": NoteData,
    "request_shipment": CommandData,
    "confirm_shipment": ShipmentData,
    "complete": CommandData,
}

# Role permission and state permission are intentionally separate checks.
ROLES = {
    "approve": "support",
    "reject": "support",
    "withdraw": "customer",
    "submit_return": "customer",
    "receive": "warehouse",
    "request_shipment": "warehouse",
    "confirm_shipment": "warehouse",
    "complete": "customer",
}
TRANSITIONS = {
    "approve": ({"EXCHANGE_REQUESTED"}, "AWAITING_RETURN"),
    "reject": ({"EXCHANGE_REQUESTED"}, "REJECTED"),
    "withdraw": ({"EXCHANGE_REQUESTED", "AWAITING_RETURN"}, "WITHDRAWN"),
    "submit_return": ({"AWAITING_RETURN"}, "RETURN_IN_TRANSIT"),
    "receive": ({"RETURN_IN_TRANSIT"}, "RETURN_RECEIVED"),
    "request_shipment": ({"RETURN_RECEIVED"}, "REPLACEMENT_PENDING"),
    "confirm_shipment": ({"REPLACEMENT_PENDING"}, "REPLACEMENT_SHIPPED"),
    "complete": ({"REPLACEMENT_SHIPPED"}, "COMPLETED"),
}
