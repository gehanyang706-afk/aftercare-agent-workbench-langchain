"""Separate warehouse consumer. Existing Agent job execution remains unchanged."""

import time
from uuid import uuid4

from aftercare.domain.contracts import BusinessError
from aftercare.repositories.database import database_health, session_factory
from aftercare.services.outbox import OutboxService
from aftercare.services.warehouse_attempt import BoundedWarehouseClient
from aftercare.services.worker_observation import WorkerObservation, json_worker_sink


class OutboxWorker:
    def __init__(self, service, client, *, owner=None, observation=None):
        self.service, self.client, self.owner = service, client, owner or "outbox-" + uuid4().hex
        self.observation = observation

    def run_once(self):
        observe = WorkerObservation("outbox", self.owner, self.observation)
        with observe.stage("cycle") as cycle:
            with observe.stage("maintenance"):
                expired = self.service.expire_leases()
            with observe.stage("claim") as step:
                claim = self.service.claim(self.owner)
                if claim is not None:
                    observe.bind_claim(claim)
                step.result(outcome="IDLE" if claim is None else "CLAIMED")
            if claim is None:
                cycle.result(outcome="IDLE")
                return {"status": "IDLE", "expired_leases": expired}
            try:
                with observe.stage("permission"):
                    self.service.begin_delivery(claim)
                with observe.stage("transport") as step:
                    result = self.client.deliver(claim.payload, claim.request_hash)
                    step.result(outcome=result.outcome, error_code=result.error_code)
                with observe.stage("finish") as step:
                    receipt = self.service.finish(
                        claim,
                        result.outcome,
                        external_shipment_id=result.external_shipment_id,
                        error_code=result.error_code,
                    )
                    step.receipt(receipt)
                cycle.receipt(receipt)
                return receipt
            except BusinessError as error:
                if error.code not in {"OUTBOX_LEASE_LOST", "DELIVERY_ALREADY_STARTED"}:
                    raise
                receipt = {
                    "status": "LEASE_LOST"
                    if error.code == "OUTBOX_LEASE_LOST"
                    else "DELIVERY_ALREADY_STARTED",
                    "outbox_id": claim.outbox_id,
                    "error_code": error.code,
                }
                cycle.receipt(receipt)
                return receipt


def work(settings, *, once=False, owner=None, emit=None, log_json=False):
    if not settings.warehouse_delivery_enabled:
        raise ValueError("WAREHOUSE_DELIVERY_DISABLED")
    if database_health(settings).status != "ready":
        raise ValueError("OUTBOX_DATABASE_NOT_READY")
    sessions = session_factory(settings)
    sink = None
    try:
        service = OutboxService(
            sessions,
            lease_seconds=settings.outbox_lease_seconds,
            max_attempts=settings.outbox_max_attempts,
        )
        client = BoundedWarehouseClient(
            settings.warehouse_base_url,
            settings.warehouse_token.get_secret_value(),
            timeout=settings.warehouse_timeout_seconds,
        )
        if log_json:
            sink = json_worker_sink(sensitive_values=(settings.warehouse_token.get_secret_value(),))
        worker = OutboxWorker(service, client, owner=owner, observation=sink)
        while True:
            result = worker.run_once()
            if emit is not None and (once or result.get("status") != "IDLE"):
                emit(result)
            if once:
                return result
            time.sleep(0.5)
    finally:
        try:
            sessions.kw["bind"].dispose()
        finally:
            if sink is not None:
                sink.close(timeout=0.25)
