"""
DRF serializers — the boundary where untrusted input becomes trusted data.

THE RULE: validate at the edge, once, completely. Everything inside the service
layer may then assume its inputs are well-formed. Code that re-checks types
deep in the business logic is code written by someone who did not trust their
own boundary.

Note that read and write use DIFFERENT serializers. Juniors use one class for
both and end up with `read_only=True` scattered everywhere plus accidental
mass-assignment holes. Separate classes make the contract obvious: this is what
you may send, that is what you will get back.
"""

from __future__ import annotations

from rest_framework import serializers

from .models import Order, OrderItem


class OrderLineInputSerializer(serializers.Serializer):
    sku = serializers.CharField(max_length=64)
    quantity = serializers.IntegerField(min_value=1, max_value=1000)
    # max_value is not pedantry: without an upper bound, `quantity: 99999999999`
    # becomes a Decimal overflow or a denial-of-service on your inventory check.
    # Bound every number that comes from outside.


class CreateOrderSerializer(serializers.Serializer):
    items = OrderLineInputSerializer(many=True, allow_empty=False, max_length=50)

    def validate_items(self, value):
        skus = [item["sku"] for item in value]
        if len(skus) != len(set(skus)):
            # The service layer merges duplicates anyway; rejecting here gives
            # the client a clear error instead of a surprising silent merge.
            raise serializers.ValidationError("duplicate SKUs in one order")
        return value


class OrderItemSerializer(serializers.ModelSerializer):
    line_total = serializers.DecimalField(max_digits=12, decimal_places=2,
                                          read_only=True)

    class Meta:
        model = OrderItem
        fields = ("sku", "quantity", "unit_price", "line_total")


class OrderSerializer(serializers.ModelSerializer):
    items = OrderItemSerializer(many=True, read_only=True)
    customer_id = serializers.IntegerField(source="customer.id", read_only=True)

    class Meta:
        model = Order
        fields = ("id", "customer_id", "status", "total_amount", "currency",
                  "items", "failure_reason", "created_at", "updated_at")
        # Explicit field list, never `fields = "__all__"`. With `__all__`, adding
        # an internal column to the model silently publishes it in your public
        # API — that is how `idempotency_key` and `internal_notes` end up in a
        # customer-facing response.
        read_only_fields = fields


class OrderListSerializer(serializers.ModelSerializer):
    """
    A lighter representation for list endpoints: no nested items.

    Nested items on a list endpoint is the classic N+1: 25 orders means 1 query
    for the page plus 25 more for the items. Either exclude them (this
    serializer) or `prefetch_related("items")` on the queryset. Never neither.
    """

    class Meta:
        model = Order
        fields = ("id", "status", "total_amount", "currency", "created_at")
