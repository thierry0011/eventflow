"""Routing for the orders app."""

from django.urls import include, path
from rest_framework.routers import DefaultRouter
from rest_framework_simplejwt.views import TokenObtainPairView, TokenRefreshView

from .views import OrderViewSet

router = DefaultRouter()
# Accept /orders AND /orders/. With the default ("/") a POST to /orders hits
# APPEND_SLASH, which cannot redirect a POST -> 500. DRF's constructor only
# takes a bool, so the optional-slash regex is set on the attribute directly.
router.trailing_slash = "/?"
router.register("orders", OrderViewSet, basename="order")
# basename is required because get_queryset() is dynamic — DRF cannot infer the
# model name from a queryset attribute that does not exist.

urlpatterns = [
    # Auth. Short-lived access token + refresh token; see SIMPLE_JWT in settings.
    path("auth/token", TokenObtainPairView.as_view(), name="token_obtain_pair"),
    path("auth/refresh", TokenRefreshView.as_view(), name="token_refresh"),
    path("", include(router.urls)),
]
