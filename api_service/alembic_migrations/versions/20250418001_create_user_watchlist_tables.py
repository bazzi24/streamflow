"""create_user_watchlist_tables

Initial migration - create user and watchlist tables in api database.

Revision ID: 20250418001
Revises:
Create Date: 2025-04-18
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import mysql

# revision identifiers, used by Alembic.
revision = "20250418001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Create user table
    op.create_table(
        "user",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("email", sa.String(255), nullable=False, unique=True, index=True),
        sa.Column("username", sa.String(100), nullable=False),
        sa.Column("password_hash", sa.String(255), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime,
            server_default=sa.func.current_timestamp(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime,
            server_default=sa.func.current_timestamp(),
            onupdate=sa.func.current_timestamp(),
            nullable=False,
        ),
        sa.Column("is_active", sa.Boolean, default=True, nullable=False),
        schema="api",
        mysql_engine="InnoDB",
        mysql_charset="utf8mb4",
        mysql_collate="utf8mb4_unicode_ci",
    )

    # Create watchlist table
    op.create_table(
        "watchlist",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column(
            "user_id",
            sa.BigInteger,
            sa.ForeignKey("api.user.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("symbol", sa.String(20), nullable=False),
        sa.Column("position", sa.Integer, default=0, nullable=False),
        sa.Column(
            "added_at",
            sa.DateTime,
            server_default=sa.func.current_timestamp(),
            nullable=False,
        ),
        sa.UniqueConstraint("user_id", "symbol", name="unique_user_symbol"),
        sa.Index("idx_user_id", "user_id"),
        schema="api",
        mysql_engine="InnoDB",
        mysql_charset="utf8mb4",
        mysql_collate="utf8mb4_unicode_ci",
    )


def downgrade() -> None:
    op.drop_table("watchlist", schema="api")
    op.drop_table("user", schema="api")
