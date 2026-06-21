"""rename limit to default_limit and drop period column

Revision ID: c4a1e5f7d832
Revises: b3590faf4eaf
Create Date: 2026-06-20 12:19:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'c4a1e5f7d832'
down_revision = 'b3590faf4eaf'
branch_labels = None
depends_on = None


def upgrade():
    # Rename 'limit' column to 'default_limit'
    op.alter_column('quotas', 'limit', new_column_name='default_limit')
    # Drop the 'period' column (no longer in the model)
    op.drop_column('quotas', 'period')
    # Fix nullability to match the model
    op.alter_column('quotas', 'org_id', existing_type=sa.String(), nullable=False)
    op.alter_column('quotas', 'feature', existing_type=sa.String(), nullable=False)
    op.alter_column('quotas', 'default_limit', existing_type=sa.Integer(), nullable=False)


def downgrade():
    # Restore nullability
    op.alter_column('quotas', 'feature', existing_type=sa.String(), nullable=True)
    op.alter_column('quotas', 'org_id', existing_type=sa.String(), nullable=True)
    op.alter_column('quotas', 'default_limit', existing_type=sa.Integer(), nullable=True)
    # Add 'period' column back
    op.add_column('quotas', sa.Column('period', sa.String(), nullable=True))
    # Rename 'default_limit' back to 'limit'
    op.alter_column('quotas', 'default_limit', new_column_name='limit')
