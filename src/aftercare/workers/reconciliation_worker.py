"""Bounded read-only provider queries with fenced, transactional result merging."""

import time
from uuid import uuid4

from aftercare.domain.contracts import BusinessError
from aftercare.repositories.database import database_health, session_factory
from aftercare.services.outbox import OutboxService
from aftercare.services.recovery import RecoveryService
from aftercare.services.warehouse_attempt import BoundedWarehouseClient
from aftercare.services.worker_observation import WorkerObservation, json_worker_sink


class ReconciliationWorker:
    def __init__(self, service, client, *, outbox=None, owner=None, observation=None):
        self.service, self.client = service, client
        self.outbox, self.owner = outbox, owner or "reconcile-" + uuid4().hex
        self.observation = observation

    def run_once(self):
        observe = WorkerObservation("reconciliation", self.owner, self.observation)
        with observe.stage("cycle") as cycle:
            with observe.stage("maintenance"):
                if self.outbox is not None:
                    self.outbox.expire_leases()
                self.service.ensure_pending()
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
                    self.service.check_claim(claim)
                with observe.stage("transport") as step:
                    result = self.client.query(
                        claim.effect_key, claim.application_id, claim.request_hash
                    )
                    step.result(outcome=result.outcome, error_code=result.error_code)
                with observe.stage("finish") as step:
                    receipt = self.service.finish(
                        claim,
                        result.outcome,
                        external_shipment_id=result.external_shipment_id,
                        provider_sequence=result.provider_sequence,
                        error_code=result.error_code,
                    )
                    step.receipt(receipt)
                cycle.receipt(receipt)
                return receipt
            except BusinessError as error:
                if error.code not in {"RECONCILIATION_LEASE_LOST", "OUTBOX_LEASE_LOST"}:
                    raise
                receipt = {
                    "status": "LEASE_LOST",
                    "task_id": claim.task_id,
                    "error_code": error.code,
                }
                cycle.receipt(receipt)
                return receipt


def work(settings, *, once=False, owner=None, emit=None, log_json=False):
    if not settings.warehouse_delivery_enabled:
        raise ValueError("WAREHOUSE_DELIVERY_DISABLED")
    if database_health(settings).status != "ready":
        raise ValueError("RECONCILIATION_DATABASE_NOT_READY")
    sessions = session_factory(settings)
    sink = None
    try:
        service = RecoveryService(
            sessions,
            lease_seconds=settings.outbox_lease_seconds,
            max_attempts=settings.reconciliation_max_attempts,
            deadline_seconds=settings.reconciliation_deadline_seconds,
            max_cycles=settings.reconciliation_max_cycles,
        )
        client = BoundedWarehouseClient(
            settings.warehouse_base_url,
            settings.warehouse_token.get_secret_value(),
            timeout=settings.warehouse_timeout_seconds,
        )
        if log_json:
            sink = json_worker_sink(sensitive_values=(settings.warehouse_token.get_secret_value(),))
        worker = ReconciliationWorker(
            service, client, outbox=OutboxService(sessions), owner=owner, observation=sink
        )
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
