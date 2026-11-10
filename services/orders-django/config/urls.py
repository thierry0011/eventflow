"""
URL routing.

Note the /api/v1/ prefix. VERSION YOUR API FROM DAY ONE. Adding a version later
means every existing client is on an unversioned path you can never change;
starting with v1 costs nothing and buys you the ability to ship v2 beside it.
"""

from django.contrib import admin
from django.urls import include, path

from orders import health

urlpatterns = [
    path("admin/", admin.site.urls),

    # Health endpoints are UNVERSIONED and UNAUTHENTICATED. They are consumed by
    # the orchestrator, not by users, and an auth failure must never look like
    # an unhealthy pod.
    #
    # LIVENESS  "is the process alive?"  If this fails, RESTART me.
    #           It must NOT check the database — a DB outage would then cause
    #           every replica to be killed in a loop, turning a recoverable
    #           incident into a total outage.
    # READINESS "can I serve traffic?"   If this fails, STOP SENDING ME TRAFFIC
    #           but leave me running. This one DOES check dependencies.
    path("health/live", health.liveness),
    path("health/ready", health.readiness),
    path("metrics", health.metrics),      # Prometheus scrape target

    path("api/v1/", include("orders.urls")),
]
