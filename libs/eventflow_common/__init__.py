"""
eventflow_common — the shared kernel.

A deliberate design decision worth understanding before you copy it:

Sharing code between microservices is dangerous. The moment two services share a
module, a change to that module can break both, and you have quietly rebuilt the
monolith with extra network calls. The rule that keeps you safe:

    SHARE TECHNICAL PLUMBING. NEVER SHARE BUSINESS LOGIC.

This package contains only plumbing — the event envelope format, broker clients,
retry/lock/idempotency helpers, logging setup. It contains no rule about what an
order is or when stock may be reserved. Those live inside the service that owns
them, and they are duplicated on purpose if two services need similar-looking
concepts, because "similar-looking today" is not "identical forever".

In production this would be a versioned package on a private index
(`pip install eventflow-common==1.4.0`), so each service upgrades on its own
schedule. Here it is a path mount to keep the repo readable.
"""

from .envelope import EventEnvelope, EventType, new_event
from .observability import (
    correlation_id_var,
    get_logger,
    new_correlation_id,
    setup_logging,
)
from .reliability import (
    CircuitBreaker,
    CircuitOpenError,
    IdempotencyStore,
    RedisLock,
    TokenBucketLimiter,
    retry,
)

__all__ = [
    "EventEnvelope",
    "EventType",
    "new_event",
    "setup_logging",
    "get_logger",
    "correlation_id_var",
    "new_correlation_id",
    "retry",
    "CircuitBreaker",
    "CircuitOpenError",
    "RedisLock",
    "IdempotencyStore",
    "TokenBucketLimiter",
]

__version__ = "1.0.0"
