import uuid

from flask_sqlalchemy import SQLAlchemy
from sqlalchemy.orm import Mapped

db = SQLAlchemy()


class Quota(db.Model):
    __tablename__ = "quotas"

    id: Mapped[uuid.UUID] = db.Column(
        db.UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    org_id: Mapped[str] = db.Column(db.String, nullable=False)
    feature: Mapped[str] = db.Column(db.String, nullable=False)
    default_limit: Mapped[int] = db.Column(db.Integer, nullable=False)

    def to_dict(self) -> dict[str, uuid.UUID | str | int]:
        return {
            "id": self.id,
            "org_id": self.org_id,
            "feature": self.feature,
            "default_limit": self.default_limit,
        }

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
