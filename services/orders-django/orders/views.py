"""
HTTP views.

A view does FOUR things and no more:
    1. authenticate / authorise
    2. deserialise and validate input
    3. call ONE service function
    4. serialise the result and choose a status code

If a view contains an `if` about business rules, that `if` belongs in
services.py. The test for whether you got this right: could you expose the same
operation over gRPC tomorrow by writing a new 15-line adapter? If yes, the
layering is correct.
"""

from __future__ import annotations

from django.core.cache import cache
from eventflow_common import IdempotencyStore, get_logger
from rest_framework import mixins, status, viewsets
from rest_framework.decorators import action
from rest_framework.permissions import IsAuthenticated
from rest_framework.response import Response

from . import services
from .models import Order
from .serializers import (
    CreateOrderSerializer,
    OrderListSerializer,
    OrderSerializer,
)

log = get_logger(__name__)


def _raw_redis():
    """
    Reach past Django's cache abstraction to the underlying Redis client.

    Why: `cache.set(..., nx=True)` does not exist. Django's cache API is a
    lowest-common-denominator interface over memcached, Redis, the filesystem
    and local memory, so it cannot expose Redis-specific atomic primitives —
    and `SET NX EX` is exactly the primitive idempotency needs.

    The lesson generalises: an abstraction that hides the feature you need is
    not helping. Use the abstraction for the 90% case and drop to the real
    client for the 10% where the specific guarantee matters — deliberately, in
    one named place, rather than scattered through the codebase.
    """
    from django.conf import settings as dj_settings

    try:
        return cache._cache.get_client()          # django.core.cache RedisCache
    except AttributeError:                        # pragma: no cover
        import redis

        return redis.from_url(f"{dj_settings.REDIS_URL}/3")


class OrderViewSet(mixins.CreateModelMixin, viewsets.ReadOnlyModelViewSet):
    """
    /api/v1/orders

    Composed from MIXINS rather than inheriting ModelViewSet. ModelViewSet gives
    you list, retrieve, create, update, partial_update AND destroy — and you
    almost never want all six. Orders must never be edited in place or deleted;
    they are financial records, and their state changes through explicit
    transitions (`/cancel`) with rules attached.

    Choosing mixins makes the API surface a deliberate decision rather than a
    framework default. `ModelViewSet` on a financial resource is how a DELETE
    endpoint nobody intended ends up in production.
    """

    permission_classes = [IsAuthenticated]
    filterset_fields = ["status"]
    ordering_fields = ["created_at", "total_amount"]
    ordering = ["-created_at"]

    def get_queryset(self):
        """
        AUTHORISATION LIVES IN THE QUERYSET.

        Filtering by `self.request.user` here means a user physically cannot
        retrieve another user's order through ANY action on this viewset —
        retrieve, list, or one you add next year and forget to protect. The
        alternative (checking ownership inside each handler) is one forgotten
        check away from an IDOR vulnerability, which is consistently in the
        OWASP top ten for exactly this reason.

        Staff get everything; note that this is the only branch, and it is
        about visibility, not about business rules.
        """
        qs = Order.objects.all()
        if not self.request.user.is_staff:
            qs = qs.filter(customer=self.request.user)

        if self.action == "retrieve":
            # prefetch_related for the reverse FK: 2 queries total instead of
            # 1 + N. select_related would be wrong here — it only works for
            # forward FKs/one-to-ones, because it is a SQL JOIN.
            qs = qs.prefetch_related("items")
        return qs

    def get_serializer_class(self):
        return OrderListSerializer if self.action == "list" else OrderSerializer

    def create(self, request):
        """
        POST /api/v1/orders

        IDEMPOTENT BY DESIGN, via the `Idempotency-Key` header.

        The problem it solves is unavoidable: a client sends a request, the
        network drops the RESPONSE, the client retries. The server has no way to
        know whether the retry is "the same order again" or "a second order".
        Only the client knows, so only the client can tell you — with a key it
        generates once and reuses on every retry of that one logical request.

        Defence in depth, both layers used here:
          * Redis    fast check, catches the common case instantly
          * UNIQUE constraint on Order.idempotency_key — the real guarantee,
            which also wins the race between two simultaneous retries

        This is exactly how Stripe, Adyen and every serious payments API do it.
        If you take one thing from this file, take this.
        """
        key = request.headers.get("Idempotency-Key")
        store = IdempotencyStore(_raw_redis(), namespace="orders")

        if key:
            cached_order_id = store.recall(key)
            if cached_order_id:
                order = Order.objects.prefetch_related("items").get(pk=cached_order_id)
                return Response(OrderSerializer(order).data, status=status.HTTP_200_OK)
                # 200, not 201: nothing was created THIS time. The status code
                # is part of the contract and should tell the truth.

        serializer = CreateOrderSerializer(data=request.data)
        serializer.is_valid(raise_exception=True)

        try:
            order = services.create_order(
                customer=request.user,
                lines=serializer.validated_data["items"],
                idempotency_key=key,
            )
        except services.DomainError as exc:
            return Response({"error": exc.code, "detail": str(exc)},
                            status=status.HTTP_400_BAD_REQUEST)
        except services.PricingUnavailable as exc:
            # 503 + Retry-After tells a well-behaved client to come back rather
            # than hammering you while you are already struggling.
            return Response({"error": "dependency_unavailable", "detail": str(exc)},
                            status=status.HTTP_503_SERVICE_UNAVAILABLE,
                            headers={"Retry-After": "5"})

        if key:
            store.remember(key, str(order.id))

        return Response(OrderSerializer(order).data, status=status.HTTP_201_CREATED)
        # 201 CREATED, and the body carries status=PENDING — not "confirmed".
        # BE HONEST ABOUT ASYNC. The order is accepted, not fulfilled. An API
        # that pretends an async workflow finished synchronously will lie to
        # your users the first time inventory is slow.

    @action(detail=True, methods=["post"])
    def cancel(self, request, pk=None):
        """
        POST /api/v1/orders/{id}/cancel

        A state transition modelled as a sub-resource action rather than
        PATCH {"status": "CANCELLED"}. Why: cancelling is a business operation
        with rules and side effects, not a field assignment. Exposing it as a
        field invites clients to invent transitions you never intended.
        """
        self.get_object()   # runs the queryset filter => enforces ownership
        try:
            order = services.cancel_order(pk, reason="cancelled by customer")
        except services.DomainError as exc:
            code = (status.HTTP_404_NOT_FOUND if exc.code == "not_found"
                    else status.HTTP_409_CONFLICT)
            return Response({"error": exc.code, "detail": str(exc)}, status=code)
            # 409 CONFLICT is the right code for "your request is well-formed
            # but the resource is not in a state that allows it". Not 400.
        return Response(OrderSerializer(order).data)
