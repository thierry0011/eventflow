"""
Notification models.

The interesting one is `Notification`, which doubles as an AUDIT LOG. In a
system that sends things to humans, "did we actually send it, when, and after
how many attempts" is a question you will be asked, usually by someone angry.
Store the answer.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

from sqlalchemy import DateTime, Index, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from .extensions import db


class NotificationStatus:
    PENDING = "PENDING"
    SENT = "SENT"
    FAILED = "FAILED"
    DEAD = "DEAD"        # exhausted retries; a human must look


class Notification(db.Model):
    __tablename__ = "notifications"

    id: Mapped[str] = mapped_column(String(36), primary_key=True,
                                    default=lambda: str(uuid.uuid4()))
    event_id: Mapped[str] = mapped_column(String(36), index=True)
    event_type: Mapped[str] = mapped_column(String(100), index=True)
    order_id: Mapped[str] = mapped_column(String(36), index=True)
    recipient: Mapped[str] = mapped_column(String(255))
    channel: Mapped[str] = mapped_column(String(20), default="email")
    subject: Mapped[str] = mapped_column(String(255))
    body: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(16),
                                        default=NotificationStatus.PENDING,
                                        index=True)
    attempts: Mapped[int] = mapped_column(default=0)
    last_error: Mapped[str] = mapped_column(Text, default="")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True),
                                                 server_default=func.now())
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True),
                                                     nullable=True)

    __table_args__ = (
        Index("idx_notification_order_created", "order_id", "created_at"),
        Index("idx_notification_status", "status", "created_at"),
    )

    def to_dict(self) -> dict:
        return {
            "id": self.id, "event_type": self.event_type, "order_id": self.order_id,
            "recipient": self.recipient, "channel": self.channel,
            "subject": self.subject, "status": self.status,
            "attempts": self.attempts,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "sent_at": self.sent_at.isoformat() if self.sent_at else None,
        }


class ProcessedEvent(db.Model):
    """
    Idempotency, third implementation in this repo.

    Three services, three ORMs, one identical pattern. That repetition is the
    point: when you see the same structure solve the same problem in Django,
    SQLAlchemy Core and Flask-SQLAlchemy, you stop thinking of it as a
    framework trick and start thinking of it as how distributed systems work.
    """

    __tablename__ = "processed_events"

    event_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    event_type: Mapped[str] = mapped_column(String(100))
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), index=True)
