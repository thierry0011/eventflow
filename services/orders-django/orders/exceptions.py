"""
A single, consistent error shape for the whole API.

Every error from this service looks like:

    {
      "error":          "machine_readable_code",
      "detail":         "human readable sentence",
      "correlation_id": "uuid the user can quote to support",
      "fields":         {"items": ["this field is required"]}   # validation only
    }

Why bother: clients must branch on errors. Branching on a human-readable string
means your error message is now a frozen API contract that no one may improve.
A stable `error` code lets you reword `detail` freely, and translate it.

Returning the correlation id in the error body is the single highest-leverage
support feature you can ship. "Quote this id" turns an unsearchable complaint
into a one-second log query.

THE SECURITY RULE: never leak internals. A 500 returns a generic message and a
correlation id; the stack trace goes to the logs, never to the client. Database
errors in particular expose table names, column names and sometimes data.
"""

from __future__ import annotations

from eventflow_common import get_logger
from eventflow_common.observability import correlation_id_var
from rest_framework.response import Response
from rest_framework.views import exception_handler as drf_default_handler

log = get_logger(__name__)


def api_exception_handler(exc: Exception, context: dict) -> Response | None:
    response = drf_default_handler(exc, context)

    if response is None:
        # DRF did not recognise it => an unhandled exception => a bug.
        # Log it with the full traceback, return nothing useful to the caller.
        log.exception("unhandled exception", extra={
            "view": context.get("view").__class__.__name__ if context.get("view") else "?",
            "path": getattr(context.get("request"), "path", "?"),
        })
        return Response(
            {"error": "internal_error",
             "detail": "An unexpected error occurred.",
             "correlation_id": correlation_id_var.get()},
            status=500,
        )

    detail = response.data
    body: dict = {
        "error": _code_for(response.status_code),
        "correlation_id": correlation_id_var.get(),
    }

    if isinstance(detail, dict) and "detail" in detail and len(detail) == 1:
        body["detail"] = str(detail["detail"])
    elif isinstance(detail, dict):
        body["detail"] = "Validation failed."
        body["fields"] = detail       # field -> [messages]
    else:
        body["detail"] = str(detail)

    response.data = body

    if response.status_code >= 500:
        log.error("server error response", extra={"status": response.status_code})
    return response


def _code_for(status_code: int) -> str:
    return {
        400: "bad_request",
        401: "unauthenticated",
        403: "forbidden",
        404: "not_found",
        405: "method_not_allowed",
        409: "conflict",
        415: "unsupported_media_type",
        422: "unprocessable_entity",
        429: "rate_limited",
        500: "internal_error",
        503: "service_unavailable",
    }.get(status_code, "error")
