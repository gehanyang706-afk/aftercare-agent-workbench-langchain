"""Add durable recovery evidence and bounded query tasks; preserve C1 business facts."""

from alembic import op

revision = "0010_warehouse_recovery"
down_revision = "0009_warehouse_outbox"
branch_labels = depends_on = None


def upgrade():
    op.execute("""ALTER TABLE shipment_requests ADD COLUMN last_sequence INTEGER NOT
NULL DEFAULT 0""")
    op.execute("""ALTER TABLE shipment_requests ADD CONSTRAINT
ck_shipment_requests_sequence CHECK(last_sequence >= 0)""")
    op.execute("""ALTER TABLE shipment_requests ADD COLUMN provider_external_id VARCHAR
UNIQUE""")
    op.execute("""ALTER TABLE warehouse_outbox ADD COLUMN cycle_attempts INTEGER NOT
NULL DEFAULT 0""")
    op.execute("""ALTER TABLE warehouse_outbox ADD COLUMN redrive_count INTEGER NOT NULL
DEFAULT 0""")
    op.execute("""UPDATE warehouse_outbox SET cycle_attempts = attempts""")
    op.execute("""ALTER TABLE warehouse_outbox ADD CONSTRAINT ck_warehouse_outbox_cycles
CHECK(cycle_attempts >= 0 AND redrive_count >= 0)""")
    op.execute("""CREATE TABLE warehouse_inbox ( id VARCHAR NOT NULL, source VARCHAR NOT
NULL, event_id VARCHAR NOT NULL, payload JSON NOT NULL, body_hash
VARCHAR(64) NOT NULL, sequence INTEGER NOT NULL, effect_key VARCHAR
NOT NULL, application_id VARCHAR NOT NULL, external_shipment_id
VARCHAR NOT NULL, request_hash VARCHAR(64) NOT NULL, status VARCHAR
DEFAULT 'RECEIVED' NOT NULL, error_code VARCHAR, created_at TIMESTAMP
WITH TIME ZONE NOT NULL, updated_at TIMESTAMP WITH TIME ZONE NOT NULL,
PRIMARY KEY (id), UNIQUE (source, event_id), CONSTRAINT
ck_warehouse_inbox_sequence CHECK (sequence >= 1), CONSTRAINT
ck_warehouse_inbox_status CHECK (status IN
('RECEIVED','PENDING','PROCESSED','STALE','CONFLICT')) )""")
    op.execute("""CREATE INDEX ix_warehouse_inbox_effect_key ON warehouse_inbox
(effect_key)""")
    op.execute("""CREATE TABLE inbox_conflicts ( id VARCHAR NOT NULL, inbox_id VARCHAR
NOT NULL, original_hash VARCHAR(64) NOT NULL, new_hash VARCHAR(64) NOT
NULL, payload JSON NOT NULL, created_at TIMESTAMP WITH TIME ZONE NOT
NULL, PRIMARY KEY (id), UNIQUE (inbox_id, new_hash), FOREIGN
KEY(inbox_id) REFERENCES warehouse_inbox (id) )""")
    op.execute("""CREATE INDEX ix_inbox_conflicts_inbox_id ON inbox_conflicts (inbox_id)""")
    op.execute("""CREATE TABLE reconciliation_tasks ( id VARCHAR NOT NULL, request_id
VARCHAR NOT NULL, status VARCHAR DEFAULT 'PENDING' NOT NULL, owner
VARCHAR, generation INTEGER DEFAULT 0 NOT NULL, lease_expires_at
TIMESTAMP WITH TIME ZONE, attempts INTEGER DEFAULT 0 NOT NULL,
cycle_attempts INTEGER DEFAULT 0 NOT NULL, cycle INTEGER DEFAULT 1 NOT
NULL, next_attempt_at TIMESTAMP WITH TIME ZONE NOT NULL, deadline_at
TIMESTAMP WITH TIME ZONE NOT NULL, last_error_code VARCHAR, created_at
TIMESTAMP WITH TIME ZONE NOT NULL, updated_at TIMESTAMP WITH TIME ZONE
NOT NULL, PRIMARY KEY (id), CONSTRAINT ck_reconciliation_tasks_status
CHECK (status IN ('PENDING','CLAIMED','DONE','EXHAUSTED')), CONSTRAINT
ck_reconciliation_tasks_counters CHECK (generation >= 0 AND attempts
>= 0 AND cycle_attempts >= 0 AND cycle >= 1), CONSTRAINT
ck_reconciliation_tasks_lease CHECK ((status='CLAIMED' AND owner IS
NOT NULL AND lease_expires_at IS NOT NULL) OR (status!='CLAIMED' AND
owner IS NULL AND lease_expires_at IS NULL)), UNIQUE (request_id),
FOREIGN KEY(request_id) REFERENCES shipment_requests (id) )""")
    op.execute("""CREATE INDEX ix_reconciliation_tasks_claim ON reconciliation_tasks
(status, next_attempt_at, id)""")
    op.execute("""CREATE TABLE recovery_commands ( id VARCHAR NOT NULL, actor_id VARCHAR
NOT NULL, actor_role VARCHAR NOT NULL, actor_source VARCHAR NOT NULL,
idempotency_key VARCHAR NOT NULL, action VARCHAR NOT NULL, request_id
VARCHAR NOT NULL, outbox_id VARCHAR NOT NULL, expected_generation
INTEGER NOT NULL, request_hash VARCHAR(64) NOT NULL, receipt JSON NOT
NULL, reason VARCHAR(500) NOT NULL, created_at TIMESTAMP WITH TIME
ZONE NOT NULL, PRIMARY KEY (id), UNIQUE (actor_id, idempotency_key),
FOREIGN KEY(request_id) REFERENCES shipment_requests (id), FOREIGN
KEY(outbox_id) REFERENCES warehouse_outbox (id) )""")
    op.execute("""CREATE INDEX ix_recovery_commands_request_id ON recovery_commands
(request_id)""")


def downgrade():
    raise RuntimeError("WAREHOUSE_RECOVERY_DOWNGRADE_FORBIDDEN")
