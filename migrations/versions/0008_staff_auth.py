"""Add staff identities and revocable sessions; retain every existing business row."""

import sqlalchemy as sa
from alembic import op

revision = "0008_staff_auth"
down_revision = "0007_exchange_fulfillment"
branch_labels = depends_on = None


def upgrade():
    op.create_table(
        "staff_accounts",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("username", sa.String(100), nullable=False, unique=True),
        sa.Column("password_hash", sa.String(), nullable=False),
        sa.Column("role", sa.String(), nullable=False),
        sa.Column("active", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        sa.Column("auth_version", sa.Integer(), server_default=sa.text("1"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("role IN ('support', 'warehouse')", name="ck_staff_accounts_role"),
        sa.CheckConstraint("auth_version >= 1", name="ck_staff_accounts_auth_version"),
    )
    op.create_table(
        "staff_sessions",
        sa.Column("token_hash", sa.String(64), primary_key=True),
        sa.Column("staff_id", sa.String(), sa.ForeignKey("staff_accounts.id"), nullable=False),
        sa.Column("csrf_token", sa.String(64), nullable=False),
        sa.Column("auth_version", sa.Integer(), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_staff_sessions_staff_id", "staff_sessions", ["staff_id"])


def downgrade():
    raise RuntimeError("STAFF_AUTH_DOWNGRADE_FORBIDDEN")
