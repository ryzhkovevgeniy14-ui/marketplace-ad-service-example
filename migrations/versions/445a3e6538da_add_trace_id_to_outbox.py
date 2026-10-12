"""add_trace_id_to_outbox

Revision ID: 445a3e6538da
Revises: 0002
Create Date: 2026-10-08 14:13:32.843765

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = '445a3e6538da'
down_revision: Union[str, Sequence[str], None] = '0002'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column("outbox", sa.Column("trace_id", sa.String(36), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_column("outbox", "trace_id")
