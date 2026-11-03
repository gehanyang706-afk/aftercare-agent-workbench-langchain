"""Add opt-in shipment requests and fenced outbox; never backfill or dispatch legacy rows."""

import sqlalchemy as sa
from alembic import op

revision = "0009_warehouse_outbox"
down_revision = "0008_staff_auth"
branch_labels = depends_on = None


def upgrade():
    op.create_table(
        "shipment_requests",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column(
            "application_id",
            sa.String(),
            sa.ForeignKey("aftercare_requests.id"),
            nullable=False,
            unique=True,
        ),
        sa.Column("effect_key", sa.String(), nullable=False, unique=True),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("status", sa.String(), server_default="PENDING", nullable=False),
        sa.Column("external_shipment_id", sa.String(), unique=True),
        sa.Column("triggered_by", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "status IN ('PENDING', 'UNKNOWN', 'SUCCEEDED', 'FAILED', 'MANUAL_REVIEW')",
            name="ck_shipment_requests_status",
        ),
        sa.CheckConstraint("length(request_hash) = 64", name="ck_shipment_requests_hash"),
    )
    op.create_table(
        "warehouse_outbox",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column(
            "request_id",
            sa.String(),
            sa.ForeignKey("shipment_requests.id"),
            nullable=False,
            unique=True,
        ),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(), server_default="PENDING", nullable=False),
        sa.Column("lease_owner", sa.String()),
        sa.Column("generation", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True)),
        sa.Column("attempts", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_error_code", sa.String()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "status IN ('PENDING', 'CLAIMED', 'DELIVERED', 'BLOCKED', 'DEAD')",
            name="ck_warehouse_outbox_status",
        ),
        sa.CheckConstraint(
            "generation >= 0 AND attempts >= 0", name="ck_warehouse_outbox_counters"
        ),
        sa.CheckConstraint(
            "(status = 'CLAIMED' AND lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL) "
            "OR (status != 'CLAIMED' AND lease_owner IS NULL AND lease_expires_at IS NULL)",
            name="ck_warehouse_outbox_lease",
        ),
    )
    op.create_index(
        "ix_warehouse_outbox_claim", "warehouse_outbox", ["status", "next_attempt_at", "id"]
    )
    op.create_table(
        "outbox_attempts",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("outbox_id", sa.String(), sa.ForeignKey("warehouse_outbox.id"), nullable=False),
        sa.Column("generation", sa.Integer(), nullable=False),
        sa.Column("owner", sa.String(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.Column("outcome", sa.String()),
        sa.Column("error_code", sa.String()),
        sa.UniqueConstraint("outbox_id", "generation"),
        sa.CheckConstraint("generation >= 1", name="ck_outbox_attempts_generation"),
        sa.CheckConstraint(
            "outcome IN ('SUCCEEDED', 'RETRYABLE', 'FAILED', 'UNKNOWN')",
            name="ck_outbox_attempts_outcome",
        ),
    )
    op.create_index("ix_outbox_attempts_outbox_id", "outbox_attempts", ["outbox_id"])
    op.execute("""CREATE FUNCTION warehouse_outbox_immutable()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF OLD.request_id IS DISTINCT FROM NEW.request_id
             OR OLD.payload::jsonb IS DISTINCT FROM NEW.payload::jsonb THEN
            RAISE EXCEPTION 'OUTBOX_SNAPSHOT_IMMUTABLE' USING ERRCODE = '23514';
          END IF;
          RETURN NEW;
        END $$""")
    op.execute("""CREATE TRIGGER warehouse_outbox_immutable BEFORE UPDATE ON warehouse_outbox
        FOR EACH ROW EXECUTE FUNCTION warehouse_outbox_immutable()""")
    op.execute("""CREATE FUNCTION shipment_request_immutable()
        RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
          IF OLD.application_id IS DISTINCT FROM NEW.application_id
             OR OLD.effect_key IS DISTINCT FROM NEW.effect_key
             OR OLD.payload::jsonb IS DISTINCT FROM NEW.payload::jsonb
             OR OLD.request_hash IS DISTINCT FROM NEW.request_hash
             OR OLD.triggered_by::jsonb IS DISTINCT FROM NEW.triggered_by::jsonb THEN
            RAISE EXCEPTION 'SHIPMENT_REQUEST_IMMUTABLE' USING ERRCODE = '23514';
          END IF;
          RETURN NEW;
        END $$""")
    op.execute("""CREATE TRIGGER shipment_request_immutable BEFORE UPDATE ON shipment_requests
        FOR EACH ROW EXECUTE FUNCTION shipment_request_immutable()""")


def downgrade():
    raise RuntimeError("WAREHOUSE_OUTBOX_DOWNGRADE_FORBIDDEN")
