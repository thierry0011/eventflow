"""
Async SQLAlchemy 2.0 setup.

CONNECTION POOLING, the thing that decides whether your service survives a
traffic spike.

Opening a Postgres connection costs a TCP handshake, TLS, authentication and a
backend process fork on the server — milliseconds each, and Postgres backends
are expensive (several MB of RAM apiece). A pool keeps a set of connections open
and hands them out.

The arithmetic that gets people fired:

    total_connections = replicas x workers x (pool_size + max_overflow)

    4 replicas x 4 uvicorn workers x (10 + 5) = 240 connections

Postgres defaults to max_connections=100. You have just taken the database down
for everyone, including the services that were behaving. ALWAYS do this
multiplication before you deploy. Past a few dozen processes, put PgBouncer in
front in transaction-pooling mode and shrink these numbers.

    pool_size      connections kept open permanently
    max_overflow   extra ones allowed during a burst, closed afterwards
    pool_timeout   how long a request waits for a free connection before
                   failing. NOT infinite — a request that waits forever is a
                   worker you will never get back.
    pool_recycle   close and reopen connections older than this. Firewalls and
                   cloud load balancers silently kill idle TCP connections; the
                   symptom is a mysterious "server closed the connection
                   unexpectedly" at a suspiciously round interval.
    pool_pre_ping  cheap SELECT 1 before handing out a connection. Costs a
                   round trip, saves you from every stale-connection error.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator

from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase

from .config import settings

engine = create_async_engine(
    settings.database_url,
    echo=settings.db_echo,
    pool_size=settings.db_pool_size,
    max_overflow=settings.db_max_overflow,
    pool_timeout=settings.db_pool_timeout,
    pool_recycle=1800,
    pool_pre_ping=True,
)

SessionFactory = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
    # expire_on_commit=False is almost always what you want in an async app.
    # With the default True, touching any attribute after commit triggers a
    # lazy refresh — which in async code raises MissingGreenlet, because a lazy
    # load cannot await. Turning it off means the objects you return from a
    # service function are still usable after the transaction closed.
    autoflush=False,
)


class Base(DeclarativeBase):
    """SQLAlchemy 2.0 declarative base. Alembic autogenerate reads its metadata."""


async def get_session() -> AsyncGenerator[AsyncSession, None]:
    """
    FastAPI dependency: one session per request.

    The `yield` form is a context manager in disguise. FastAPI runs the code
    before `yield` on the way in, injects the value, and runs the code after it
    on the way out — including when the endpoint raised. That is what makes the
    rollback/close reliable.

    ONE SESSION PER REQUEST, never a global session. A SQLAlchemy session is a
    unit of work with identity-map state and is not concurrency-safe; sharing
    one across requests produces data from someone else's request appearing in
    yours, which is both a bug and a security incident.
    """
    async with SessionFactory() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


async def dispose_engine() -> None:
    """Close every pooled connection. Called on shutdown so Postgres is not
    left holding backends for a process that no longer exists."""
    await engine.dispose()
