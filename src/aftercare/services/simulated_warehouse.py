"""Deterministic in-process B1 adapter; no HTTP or provider reliability claims."""

from hashlib import sha256

from aftercare.domain.fulfillment import Actor, FulfillmentCommandInput
from aftercare.domain.rules import require


class SimulatedWarehouse:
    actor = Actor("warehouse-simulator", "warehouse", "trusted_adapter")

    @staticmethod
    def shipment_id(effect_key):
        return "SIM-" + sha256(effect_key.encode()).hexdigest()[:24]

    def confirm(self, service, application_id, *, expected_version, idempotency_key):
        snapshot = service.get(self.actor, application_id)
        effect_key = snapshot["fulfillment"].get("effect_key")
        require(effect_key is not None, "SHIPMENT_NOT_REQUESTED")
        external_id = self.shipment_id(effect_key)
        return service.execute(
            self.actor,
            application_id,
            FulfillmentCommandInput(
                command="confirm_shipment",
                expected_version=expected_version,
                idempotency_key=idempotency_key,
                data={"external_shipment_id": external_id},
            ),
        )
