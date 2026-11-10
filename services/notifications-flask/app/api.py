"""
The notification service's small read API.

Flask has no built-in validation or pagination, so both are assembled by hand
here — which is the honest cost of the minimal core. Compare the amount of code
needed for the same job in DRF (serializers.py) and FastAPI (schemas.py).
"""

from __future__ import annotations

from flask import Blueprint, request
from marshmallow import Schema, ValidationError, fields, validate

from .extensions import db
from .models import Notification

api_bp = Blueprint("api", __name__)


class NotificationQuerySchema(Schema):
    """
    Validating QUERY PARAMETERS, which people routinely forget.

    Query params are user input exactly like a JSON body. `?limit=99999999` is
    a denial of service, and an unvalidated `order_by` passed into raw SQL is
    an injection. Validate everything that crosses the boundary, in every form
    it arrives.
    """

    order_id = fields.Str(required=False, validate=validate.Length(max=36))
    status = fields.Str(required=False,
                        validate=validate.OneOf(["PENDING", "SENT", "FAILED", "DEAD"]))
    limit = fields.Int(load_default=50, validate=validate.Range(min=1, max=200))
    cursor = fields.Str(required=False)


@api_bp.get("/notifications")
def list_notifications():
    try:
        params = NotificationQuerySchema().load(request.args.to_dict())
    except ValidationError as exc:
        return {"error": "validation_error", "fields": exc.messages}, 400

    query = db.select(Notification).order_by(Notification.created_at.desc())
    if params.get("order_id"):
        query = query.where(Notification.order_id == params["order_id"])
    if params.get("status"):
        query = query.where(Notification.status == params["status"])
    if params.get("cursor"):
        query = query.where(Notification.id < params["cursor"])

    rows = db.session.scalars(query.limit(params["limit"])).all()
    return {
        "items": [row.to_dict() for row in rows],
        "meta": {"count": len(rows),
                 "next_cursor": rows[-1].id if len(rows) == params["limit"] else None},
    }


@api_bp.get("/notifications/<notification_id>")
def get_notification(notification_id: str):
    notification = db.session.get(Notification, notification_id)
    if notification is None:
        return {"error": "not_found", "detail": "notification not found"}, 404
    return notification.to_dict()


@api_bp.get("/notifications/stats")
def stats():
    """
    Operational endpoint: counts by status.

    `DEAD` is the number that matters — it is the count of notifications that
    exhausted every retry and will never be sent unless someone intervenes.
    This is the kind of number that belongs on a dashboard with an alert on it,
    not in a log line nobody reads.
    """
    rows = db.session.execute(
        db.select(Notification.status, db.func.count())
        .group_by(Notification.status)
    ).all()
    return {"counts": {status: count for status, count in rows}}
