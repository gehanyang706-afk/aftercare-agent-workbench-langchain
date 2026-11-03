"""Authenticated local HTTP provider, independent of product PostgreSQL and sessions."""

import secrets
import sqlite3
import threading
import time
from typing import Annotated, Literal

from fastapi import Depends, FastAPI, Header
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field, model_validator

from aftercare.domain.warehouse import (
    PROVIDER_ID,
    ShipmentInput,
    validate_callback_secret,
    verify_shipment_hash,
)
from aftercare.mock_warehouse.callbacks import CallbackSender, validate_callback_url
from aftercare.mock_warehouse.store import (
    WarehouseConflict,
    WarehouseInvalidRequest,
    WarehouseStateError,
    WarehouseStore,
)


class ProviderRejection(Exception):
    def __init__(self, retryable):
        self.retryable = retryable


class ProviderUnauthorized(Exception):
    pass


class FaultInput(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    effect_key: str = Field(min_length=1, max_length=200, pattern=r"\S")
    mode: Literal[
        "retryable_reject_once",
        "permanent_reject",
        "error_after_accept_once",
        "query_unavailable",
        "delay_after_accept_once",
        "clear",
    ]
    delay_seconds: float = Field(default=1.0, ge=0, le=5, allow_inf_nan=False)

    @model_validator(mode="after")
    def delay_only_for_delay_fault(self):
        if "delay_seconds" in self.model_fields_set and self.mode != "delay_after_accept_once":
            raise ValueError("INVALID_FAULT_DELAY")
        return self


class Faults:
    def __init__(self):
        self.lock = threading.Lock()
        self.modes = {}

    def configure(self, effect_key, mode, delay_seconds=1.0):
        with self.lock:
            if mode == "clear":
                self.modes.pop(effect_key, None)
            else:
                self.modes[effect_key] = (mode, delay_seconds)

    def consume(self, effect_key):
        with self.lock:
            configured = self.modes.get(effect_key)
            mode = configured[0] if configured else None
            if mode not in {"permanent_reject", "query_unavailable"}:
                self.modes.pop(effect_key, None)
            return configured or (None, 0)

    def query_unavailable(self, effect_key):
        with self.lock:
            configured = self.modes.get(effect_key)
            return configured is not None and configured[0] == "query_unavailable"


def problem(status, code, *, accepted=None, retryable=False):
    payload = {"provider": PROVIDER_ID, "code": code, "retryable": retryable}
    if accepted is not None:
        payload["accepted"] = accepted
    return JSONResponse(status_code=status, content=payload)


def create_app(state_path, token, *, test_mode=False, callback_base_url=None, callback_secret=None):
    if not isinstance(test_mode, bool):
        raise ValueError("WAREHOUSE_TEST_MODE_INVALID")
    if not (
        isinstance(token, str)
        and 32 <= len(token) <= 256
        and all(33 <= ord(character) <= 126 for character in token)
    ):
        raise ValueError("WAREHOUSE_TOKEN_REQUIRED")
    callbacks_configured = callback_base_url is not None or callback_secret is not None
    if callbacks_configured:
        validate_callback_url(callback_base_url)
        validate_callback_secret(callback_secret)
        if secrets.compare_digest(token, callback_secret):
            raise ValueError("WAREHOUSE_CALLBACK_SECRET_MUST_DIFFER")
    store = WarehouseStore(state_path)
    sender = (
        CallbackSender(state_path, callback_base_url, callback_secret, test_mode=test_mode)
        if callbacks_configured
        else None
    )
    expected = ("Bearer " + token).encode()
    faults = Faults()
    app = FastAPI(title="Local mock warehouse", docs_url=None, redoc_url=None, openapi_url=None)
    app.state.store = store
    app.state.callback_sender = sender

    def authorized(authorization: Annotated[str | None, Header()] = None):
        if not secrets.compare_digest((authorization or "").encode(), expected):
            raise ProviderUnauthorized()
        return True

    auth = Depends(authorized)

    @app.exception_handler(ProviderUnauthorized)
    def unauthorized(request, error):
        return problem(401, "UNAUTHORIZED", accepted=False)

    @app.exception_handler(RequestValidationError)
    def invalid_input(request, error):
        # Do not echo headers, secret tokens, request values or state paths.
        return problem(422, "INVALID_INPUT", accepted=False)

    @app.get("/health/ready")
    def ready():
        try:
            if store.ready():
                return {"status": "ready"}
        except (OSError, sqlite3.Error, WarehouseStateError):
            pass
        return JSONResponse(status_code=503, content={"status": "not_ready"})

    @app.post("/mock-warehouse/shipments")
    def shipment(body: ShipmentInput, allowed=auth):
        if not allowed:
            return problem(401, "UNAUTHORIZED", accepted=False)
        if not verify_shipment_hash(body):
            return problem(400, "INVALID_REQUEST_HASH", accepted=False)
        mode = None
        delay_seconds = 0

        def before_accept():
            nonlocal mode, delay_seconds
            mode, delay_seconds = faults.consume(body.effect_key)
            if mode in {"retryable_reject_once", "permanent_reject"}:
                raise ProviderRejection(mode == "retryable_reject_once")

        try:
            result, created = store.accept(body, before_accept=before_accept)
        except ProviderRejection as error:
            return problem(
                503 if error.retryable else 422,
                "TEMPORARY_UNAVAILABLE" if error.retryable else "PERMANENT_REJECTED",
                accepted=False,
                retryable=error.retryable,
            )
        except WarehouseConflict as error:
            return problem(409, error.code, accepted=False)
        except WarehouseInvalidRequest:
            return problem(400, "INVALID_REQUEST_HASH", accepted=False)
        except (OSError, sqlite3.Error, WarehouseStateError):
            # No accepted:false proof after an unexpected storage failure.
            return problem(500, "STATE_ERROR")
        if sender is not None:
            # Shipment facts and both immutable events have committed. Failed or
            # slow callbacks must never undo that acceptance or create a new one.
            for _ in range(2):
                try:
                    sender.work_once(effect_key=body.effect_key)
                except Exception:
                    pass
        if created and mode == "delay_after_accept_once":
            time.sleep(delay_seconds)
        if created and mode == "error_after_accept_once":
            return problem(500, "RESPONSE_LOST_AFTER_ACCEPT")
        return result.model_dump()

    @app.get("/mock-warehouse/shipments/by-key/{effect_key:path}")
    def by_key(effect_key: str, allowed=auth):
        if not allowed:
            return problem(401, "UNAUTHORIZED", accepted=False)
        if faults.query_unavailable(effect_key):
            # This is an availability failure, with no not-accepted proof.
            return problem(503, "QUERY_UNAVAILABLE", retryable=True)
        try:
            result = store.get(effect_key)
        except (OSError, sqlite3.Error, WarehouseStateError):
            return problem(500, "STATE_ERROR")
        if result is None:
            return problem(404, "NOT_FOUND", accepted=False)
        return JSONResponse(content=result.model_dump(), headers={"X-Warehouse-Sequence": "2"})

    if test_mode:

        @app.post("/__test/faults")
        def configure_fault(body: FaultInput, allowed=auth):
            if not allowed:
                return problem(401, "UNAUTHORIZED", accepted=False)
            try:
                if (
                    body.mode not in {"query_unavailable", "clear"}
                    and store.get(body.effect_key) is not None
                ):
                    return problem(409, "ALREADY_ACCEPTED")
            except (OSError, sqlite3.Error, WarehouseStateError):
                return problem(500, "STATE_ERROR")
            faults.configure(body.effect_key, body.mode, body.delay_seconds)
            return {"configured": True}

    return app
