"""Distinguish new unsent delivery claims without inventing legacy send evidence."""

from alembic import op

revision = "0011_dispatch_intent"
down_revision = "0010_warehouse_recovery"
branch_labels = depends_on = None


def upgrade():
    op.execute(
        "ALTER TABLE outbox_attempts ADD COLUMN send_state VARCHAR NOT NULL "
        "DEFAULT 'LEGACY_UNKNOWN'"
    )
    op.execute("ALTER TABLE outbox_attempts ADD COLUMN send_started_at TIMESTAMP WITH TIME ZONE")
    op.execute(
        "ALTER TABLE outbox_attempts ADD CONSTRAINT ck_outbox_attempts_send_state "
        "CHECK (send_state IN ('NOT_STARTED', 'STARTED', 'LEGACY_UNKNOWN'))"
    )
    op.execute(
        "ALTER TABLE outbox_attempts ADD CONSTRAINT ck_outbox_attempts_send_started "
        "CHECK ((send_state = 'STARTED' AND send_started_at IS NOT NULL) OR "
        "(send_state != 'STARTED' AND send_started_at IS NULL))"
    )


def downgrade():
    raise RuntimeError("DISPATCH_INTENT_DOWNGRADE_FORBIDDEN")
