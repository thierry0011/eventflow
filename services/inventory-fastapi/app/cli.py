"""
Small operational CLI.

    python -m app.cli init-db     create tables directly (local only)
    python -m app.cli seed        load demo products

`init-db` uses `create_all`, which is fine for a throwaway local database and
WRONG for anything else: it creates whatever the models currently say, with no
record of how you got there and no way to go back. Real schema changes go
through Alembic migrations, which are reviewed, versioned and reversible.
`make migrate` runs Alembic; this command exists only so a fresh clone works in
one step.
"""

from __future__ import annotations

import asyncio
import sys
from decimal import Decimal

from sqlalchemy import select

from .db import Base, SessionFactory, engine
from .models import Product

DEMO = [
    ("SKU-LAPTOP-01", "14-inch developer laptop", Decimal("1299.00"), 25),
    ("SKU-MOUSE-01", "Wireless mouse", Decimal("29.99"), 300),
    ("SKU-DESK-01", "Standing desk", Decimal("459.50"), 12),
    ("SKU-CHAIR-01", "Ergonomic chair", Decimal("319.00"), 8),
    ("SKU-MONITOR-01", "27-inch 4K monitor", Decimal("549.00"), 40),
    ("SKU-RARE-01", "Limited edition keyboard", Decimal("199.00"), 1),
    # quantity 1 on purpose: this is the SKU to use when you want to reproduce
    # the oversell race by hand. See docs/07-EXERCISES.md.
]


async def init_db() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    print("tables created")


async def seed() -> None:
    async with SessionFactory() as session:
        async with session.begin():
            for sku, name, price, qty in DEMO:
                existing = await session.scalar(
                    select(Product).where(Product.sku == sku))
                if existing is None:
                    session.add(Product(sku=sku, name=name, unit_price=price,
                                        quantity_on_hand=qty))
    print(f"seeded {len(DEMO)} products")


if __name__ == "__main__":
    command = sys.argv[1] if len(sys.argv) > 1 else ""
    if command == "init-db":
        asyncio.run(init_db())
    elif command == "seed":
        asyncio.run(seed())
    else:
        print(__doc__)
        sys.exit(1)
