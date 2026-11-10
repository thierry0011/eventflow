"""
Django admin.

Why this matters in a microservice repo: the admin is Django's genuine
superpower and the strongest argument for choosing it over FastAPI for a
transactional service. An operations team gets a working back office for free.
FastAPI has nothing comparable, and building one costs weeks.

The outbox being visible here is not decoration — it is the first place you look
when someone says "the order went through but no email arrived".
"""

from django.contrib import admin

from .models import Order, OrderItem, OutboxMessage, ProcessedEvent


class OrderItemInline(admin.TabularInline):
    model = OrderItem
    extra = 0
    readonly_fields = ("sku", "quantity", "unit_price")
    can_delete = False


@admin.register(Order)
class OrderAdmin(admin.ModelAdmin):
    list_display = ("id", "customer", "status", "total_amount", "created_at")
    list_filter = ("status", "created_at", "currency")
    search_fields = ("id", "customer__username", "items__sku")
    readonly_fields = ("id", "created_at", "updated_at", "reservation_id",
                       "idempotency_key")
    inlines = [OrderItemInline]
    date_hierarchy = "created_at"

    def get_queryset(self, request):
        # The admin changelist is an N+1 factory: `customer` is rendered for
        # every row, so without select_related you fire one query per row.
        return super().get_queryset(request).select_related("customer")


@admin.register(OutboxMessage)
class OutboxMessageAdmin(admin.ModelAdmin):
    list_display = ("id", "event_type", "aggregate_id", "published_at",
                    "attempts", "created_at")
    list_filter = ("event_type", "published_at")
    search_fields = ("aggregate_id", "event_id", "correlation_id")
    readonly_fields = [f.name for f in OutboxMessage._meta.fields]
    # Entirely read-only. An outbox row is a record of an intent that already
    # committed; editing one by hand corrupts the audit trail and can republish
    # a falsified event. Look, never touch.


@admin.register(ProcessedEvent)
class ProcessedEventAdmin(admin.ModelAdmin):
    list_display = ("event_id", "event_type", "consumer_group", "processed_at")
    list_filter = ("event_type", "consumer_group")
    search_fields = ("event_id",)
