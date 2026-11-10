"""initial inventory schema

Revision ID: 0001
Revises:
Create Date: 2026-01-01

Hand-reviewed after autogenerate. Note the explicit CHECK constraints and the
partial index — autogenerate produces a reasonable draft, but the constraints
that actually protect your data are usually ones you add deliberately.
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "products",
        sa.Column("sku", sa.String(64), primary_key=True),
        sa.Column("name", sa.String(255), nullable=False),
        sa.Column("unit_price", sa.Numeric(12, 2), nullable=False),
        sa.Column("quantity_on_hand", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("quantity_reserved", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("version", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.CheckConstraint("quantity_on_hand >= 0", name="ck_on_hand_non_negative"),
        sa.CheckConstraint("quantity_reserved >= 0", name="ck_reserved_non_negative"),
        sa.CheckConstraint("quantity_reserved <= quantity_on_hand",
                           name="ck_reserved_not_exceeding_stock"),
    )
    op.create_index("idx_product_name", "products", ["name"])

    op.create_table(
        "reservations",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("order_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("sku", sa.String(64), sa.ForeignKey("products.sku"), nullable=False),
        sa.Column("quantity", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False, server_default="HELD"),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.UniqueConstraint("order_id", "sku", name="uq_reservation_order_sku"),
    )
    op.create_index("ix_reservations_order_id", "reservations", ["order_id"])
    op.create_index("ix_reservations_sku", "reservations", ["sku"])
    op.create_index("idx_reservation_expiry", "reservations", ["status", "expires_at"])

    op.create_table(
        "outbox_messages",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("event_id", postgresql.UUID(as_uuid=True), nullable=False, unique=True),
        sa.Column("event_type", sa.String(100), nullable=False),
        sa.Column("aggregate_id", sa.String(64), nullable=False),
        sa.Column("topic", sa.String(100), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("correlation_id", sa.String(64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_error", sa.String(2000), nullable=False, server_default=""),
    )
    op.create_index("ix_outbox_aggregate_id", "outbox_messages", ["aggregate_id"])
    # The partial index: only unpublished rows. Stays small forever even as the
    # table grows to millions of published rows.
    op.create_index("idx_inv_outbox_unpublished", "outbox_messages", ["created_at"],
                    postgresql_where=sa.text("published_at IS NULL"))

    op.create_table(
        "processed_events",
        sa.Column("event_id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("event_type", sa.String(100), nullable=False),
        sa.Column("processed_at", sa.DateTime(timezone=True), server_default=sa.func.now()),
    )
    op.create_index("ix_processed_events_processed_at", "processed_events",
                    ["processed_at"])


def downgrade() -> None:
    """
    Write the downgrade. Yes, even though you will probably never run it.

    Two reasons. First, writing it forces you to notice when a migration is
    irreversible — dropping a column destroys data, and realising that while
    writing `downgrade()` is much cheaper than realising it during a rollback.
    Second, it is the only way to test a migration both ways locally.
    """
    op.drop_table("processed_events")
    op.drop_table("outbox_messages")
    op.drop_table("reservations")
    op.drop_table("products")
