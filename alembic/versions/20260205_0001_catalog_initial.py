"""Initial Catalog schema baseline."""

from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "20260205_0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Apply initial Catalog schema."""
    from heber.catalog.db import Base

    bind = op.get_bind()
    Base.metadata.create_all(bind=bind)


def downgrade() -> None:
    """Drop initial Catalog schema."""
    from heber.catalog.db import Base

    bind = op.get_bind()
    Base.metadata.drop_all(bind=bind)
