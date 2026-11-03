"""Add exchange fulfillment facts without rewriting legacy requests or ledgers."""

import sqlalchemy as sa
from alembic import op

revision = "0007_exchange_fulfillment"
down_revision = "0006_experiments"
branch_labels = depends_on = None


def upgrade():
    op.add_column(
        "aftercare_requests",
        sa.Column("version", sa.Integer(), server_default=sa.text("1"), nullable=False),
    )
    op.create_check_constraint(
        "ck_aftercare_requests_version", "aftercare_requests", "version >= 1"
    )
    op.create_check_constraint(
        "ck_aftercare_requests_exchange_state",
        "aftercare_requests",
        "action != 'exchange' OR status IN ("
        "'EXCHANGE_REQUESTED', 'AWAITING_RETURN', 'RETURN_IN_TRANSIT', "
        "'RETURN_RECEIVED', 'REPLACEMENT_PENDING', 'REPLACEMENT_SHIPPED', "
        "'COMPLETED', 'REJECTED', 'WITHDRAWN')",
    )
    op.create_table(
        "exchange_fulfillments",
        sa.Column(
            "application_id", sa.String(), sa.ForeignKey("aftercare_requests.id"), primary_key=True
        ),
        sa.Column("origin", sa.String(), server_default="native", nullable=False),
        sa.Column("return_carrier", sa.String(), nullable=True),
        sa.Column("return_tracking", sa.String(), nullable=True),
        sa.Column("received_note", sa.Text(), nullable=True),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("effect_key", sa.String(), nullable=True, unique=True),
        sa.Column("shipment_status", sa.String(), nullable=True),
        sa.Column("external_shipment_id", sa.String(), nullable=True, unique=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint(
            "origin IN ('legacy', 'native')", name="ck_exchange_fulfillments_origin"
        ),
        sa.CheckConstraint(
            "shipment_status IN ('PENDING', 'UNKNOWN', 'SUCCEEDED', 'FAILED', 'MANUAL_REVIEW')",
            name="ck_exchange_fulfillments_shipment_status",
        ),
    )
    op.create_table(
        "fulfillment_commands",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column(
            "application_id", sa.String(), sa.ForeignKey("aftercare_requests.id"), nullable=False
        ),
        sa.Column("actor_id", sa.String(), nullable=False),
        sa.Column("actor_role", sa.String(), nullable=False),
        sa.Column("actor_source", sa.String(), nullable=False),
        sa.Column("idempotency_key", sa.String(), nullable=False),
        sa.Column("request_hash", sa.String(), nullable=False),
        sa.Column("command", sa.String(), nullable=False),
        sa.Column("receipt", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("application_id", "actor_id", "actor_role", "idempotency_key"),
    )
    op.create_index(
        "ix_fulfillment_commands_application_id", "fulfillment_commands", ["application_id"]
    )
    op.create_table(
        "fulfillment_audits",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column(
            "application_id", sa.String(), sa.ForeignKey("aftercare_requests.id"), nullable=False
        ),
        sa.Column(
            "command_id",
            sa.String(),
            sa.ForeignKey("fulfillment_commands.id"),
            nullable=True,
            unique=True,
        ),
        sa.Column("actor_id", sa.String(), nullable=False),
        sa.Column("actor_role", sa.String(), nullable=False),
        sa.Column("actor_source", sa.String(), nullable=False),
        sa.Column("command", sa.String(), nullable=False),
        sa.Column("from_state", sa.String(), nullable=True),
        sa.Column("to_state", sa.String(), nullable=False),
        sa.Column("from_version", sa.Integer(), nullable=True),
        sa.Column("to_version", sa.Integer(), nullable=False),
        sa.Column("details", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.UniqueConstraint("application_id", "to_version"),
    )
    op.create_index(
        "ix_fulfillment_audits_application_id", "fulfillment_audits", ["application_id"]
    )
    op.execute("""INSERT INTO exchange_fulfillments
        (application_id, origin, created_at, updated_at)
        SELECT id, 'legacy', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
        FROM aftercare_requests WHERE action = 'exchange'""")
    op.execute("""INSERT INTO fulfillment_audits
        (id, application_id, command_id, actor_id, actor_role, actor_source, command,
         from_state, to_state, from_version, to_version, details, created_at)
        SELECT 'legacy_import:' || id, id, NULL, 'migration:0007', 'system', 'migration',
               'legacy_import', NULL, status, NULL, version,
               '{"legacy_import": true}'::json, CURRENT_TIMESTAMP
        FROM aftercare_requests WHERE action = 'exchange'""")


def downgrade():
    raise RuntimeError("FULFILLMENT_DOWNGRADE_FORBIDDEN")
