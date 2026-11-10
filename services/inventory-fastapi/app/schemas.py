"""
Pydantic v2 schemas — FastAPI's single biggest advantage.

In Django you write a serializer and hope the docs match. In FastAPI the schema
IS the validation, IS the serialisation, and IS the OpenAPI document. One
definition, three jobs, never out of sync.

Patterns to notice:
  * separate In/Out models (same reason as DRF: never let a client set a field
    it has no business setting)
  * `Field(...)` with constraints, which become BOTH runtime validation AND
    documented limits in /docs
  * `model_config = ConfigDict(from_attributes=True)` so an ORM object can be
    returned directly and Pydantic reads its attributes
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class ProductOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    sku: str
    name: str
    unit_price: Decimal
    quantity_on_hand: int
    quantity_reserved: int
    available: int   # resolved from the @property on the model

    @field_validator("unit_price")
    @classmethod
    def quantize(cls, v: Decimal) -> Decimal:
        return v.quantize(Decimal("0.01"))


class ProductCreate(BaseModel):
    sku: str = Field(min_length=1, max_length=64,
                     pattern=r"^[A-Z0-9\-]+$")
    # A regex at the boundary. It documents the format in /docs, rejects junk
    # before it reaches your logic, and closes off whole classes of injection
    # and encoding bugs. Constrain every string you accept.
    name: str = Field(min_length=1, max_length=255)
    unit_price: Decimal = Field(gt=0, le=Decimal("1000000"), decimal_places=2)
    quantity_on_hand: int = Field(ge=0, le=10_000_000)


class RestockIn(BaseModel):
    quantity: int = Field(gt=0, le=1_000_000)


class PriceRequest(BaseModel):
    skus: list[str] = Field(min_length=1, max_length=100)
    # max_length is backpressure in schema form: without it a client asks for
    # 50,000 SKUs in one call and you serve a single request for ten seconds
    # while everything else queues behind it.


class PriceResponse(BaseModel):
    prices: dict[str, Decimal]


class ReservationOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    order_id: str
    sku: str
    quantity: int
    status: str
    expires_at: datetime


class PageMeta(BaseModel):
    next_cursor: str | None = None
    has_more: bool = False


class ProductPage(BaseModel):
    items: list[ProductOut]
    meta: PageMeta


class ErrorResponse(BaseModel):
    """
    Declared so it can be referenced in `responses={...}` on each route and show
    up in the OpenAPI document. An API whose error shape is undocumented forces
    every client author to discover it by causing failures in production.
    """

    error: str
    detail: str
    correlation_id: str | None = None
