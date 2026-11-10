"""
Reliability primitives: retry, circuit breaker, distributed lock, idempotency,
rate limiting.

Distributed systems fail partially and constantly. A senior engineer does not
write code that assumes the network works; they write code that behaves
correctly when it does not. These five primitives cover almost every case you
will meet.

    retry            — the failure is probably transient, try again, backing off
    CircuitBreaker   — the failure is NOT transient, stop hammering the corpse
    RedisLock        — two processes must not do the same thing at once
    IdempotencyStore — this message may arrive twice; do the work once
    TokenBucket      — protect yourself from your own clients

A rule that takes people years to learn: RETRY IS ONLY SAFE IF THE OPERATION IS
IDEMPOTENT. Retrying a non-idempotent charge is how customers get billed twice.
Make the operation idempotent first, then add retry. Never the other way round.
"""

from __future__ import annotations

import functools
import hashlib
import random
import time
import uuid
from typing import Any, Callable, Iterable, TypeVar

from .observability import get_logger

log = get_logger(__name__)
T = TypeVar("T")


# ===========================================================================
# 1. RETRY WITH EXPONENTIAL BACKOFF AND JITTER
# ===========================================================================
def retry(
    exceptions: type[BaseException] | tuple[type[BaseException], ...] = Exception,
    *,
    max_attempts: int = 5,
    base_delay: float = 0.2,
    max_delay: float = 20.0,
    jitter: bool = True,
) -> Callable:
    """
    Why exponential: a service that just fell over needs breathing room. Retrying
    every 100ms turns your outage into their outage.

    Why JITTER is non-negotiable: without it, every client that failed at the
    same moment retries at the same moment. The recovering service is hit by a
    synchronised wall of traffic and falls over again — the "thundering herd".
    A random spread converts a spike into a trickle. One line of code, prevents
    a whole class of production incident.

    Note what is NOT retried: this wrapper retries the exceptions you name, and
    nothing else. Retrying a ValueError from your own bad input just burns CPU
    five times before failing identically. Be specific: ConnectionError,
    TimeoutError, OperationalError — the transient ones.
    """

    def decorator(fn: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            last: BaseException | None = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return fn(*args, **kwargs)
                except exceptions as exc:
                    last = exc
                    if attempt == max_attempts:
                        break
                    delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
                    if jitter:
                        # "full jitter": pick uniformly in [0, delay]
                        delay = random.uniform(0, delay)
                    log.warning(
                        "retrying after failure",
                        extra={"fn": fn.__name__, "attempt": attempt,
                               "max_attempts": max_attempts,
                               "sleep_seconds": round(delay, 3),
                               "error": str(exc)},
                    )
                    time.sleep(delay)
            log.error("retries exhausted", extra={"fn": fn.__name__,
                                                  "attempts": max_attempts})
            raise last  # type: ignore[misc]

        return wrapper

    return decorator


# ===========================================================================
# 2. CIRCUIT BREAKER
# ===========================================================================
class CircuitOpenError(RuntimeError):
    """Raised instead of calling a dependency that is known to be down."""


class CircuitBreaker:
    """
    Retry says "try again". The circuit breaker says "stop trying for a while".

    Three states:
        CLOSED    normal. Calls pass through, failures are counted.
        OPEN      too many failures. Calls fail INSTANTLY without touching the
                  dependency. This protects two parties at once: the dependency
                  gets room to recover, and your own threads/connections are not
                  all parked on a 30-second timeout.
        HALF_OPEN after a cooldown, let ONE call through. Success closes the
                  circuit; failure re-opens it for another cooldown.

    The failure mode it prevents is cascading failure: service A hangs, service
    B's workers all block on A, B's queue fills, C blocks on B, and a problem in
    one leaf takes down the tree. Fail fast, degrade gracefully.

    This implementation is per-process. Shared state across replicas would use
    Redis — worth knowing, rarely worth the complexity.
    """

    CLOSED, OPEN, HALF_OPEN = "closed", "open", "half_open"

    def __init__(self, name: str, failure_threshold: int = 5,
                 recovery_timeout: float = 30.0,
                 expected_exceptions: tuple[type[BaseException], ...] = (Exception,)) -> None:
        self.name = name
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exceptions = expected_exceptions
        self._failures = 0
        self._opened_at = 0.0
        self._state = self.CLOSED

    @property
    def state(self) -> str:
        if self._state == self.OPEN and \
           time.monotonic() - self._opened_at >= self.recovery_timeout:
            self._state = self.HALF_OPEN
            log.info("circuit half-open, probing", extra={"circuit": self.name})
        return self._state

    def call(self, fn: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        if self.state == self.OPEN:
            raise CircuitOpenError(f"circuit '{self.name}' is open")
        try:
            result = fn(*args, **kwargs)
        except self.expected_exceptions:
            self._on_failure()
            raise
        self._on_success()
        return result

    def __call__(self, fn: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            return self.call(fn, *args, **kwargs)
        return wrapper

    def _on_success(self) -> None:
        if self._state != self.CLOSED:
            log.info("circuit closed", extra={"circuit": self.name})
        self._failures = 0
        self._state = self.CLOSED

    def _on_failure(self) -> None:
        self._failures += 1
        if self._failures >= self.failure_threshold or self._state == self.HALF_OPEN:
            self._state = self.OPEN
            self._opened_at = time.monotonic()
            log.error("circuit opened", extra={"circuit": self.name,
                                               "failures": self._failures})


# ===========================================================================
# 3. DISTRIBUTED LOCK (Redis)
# ===========================================================================
class RedisLock:
    """
    A mutex that works across processes and machines.

    Use when two replicas must not do the same thing simultaneously: reserving
    the last unit of stock, running a nightly job, consuming a scarce resource.

    Three details that separate a correct lock from a dangerous one:

    1. SET NX EX — acquire and set the expiry in ONE atomic command. The naive
       `if not exists: set; expire` has a window where the process dies between
       the two commands and the lock is held forever.

    2. A UNIQUE TOKEN per holder. Without it, process A whose lock expired mid-
       work will happily delete process B's freshly acquired lock on the way
       out, and now two processes think they hold it.

    3. RELEASE VIA LUA. "Check the token, then delete" must be atomic for the
       same reason as (1). Redis runs Lua scripts atomically, so compare-and-
       delete becomes a single operation.

    Honest limitation: this is not a consensus algorithm. Under a network
    partition plus clock skew it can be violated (read Martin Kleppmann on
    Redlock). It is correct enough for "avoid duplicate work", and NOT the right
    tool for "money must never be double spent" — for that you use a database
    transaction with `SELECT ... FOR UPDATE`, which is exactly what the
    inventory service does for the actual stock decrement.
    """

    RELEASE_SCRIPT = """
    if redis.call('GET', KEYS[1]) == ARGV[1] then
        return redis.call('DEL', KEYS[1])
    else
        return 0
    end
    """

    def __init__(self, redis_client: Any, key: str, *, ttl_seconds: int = 30,
                 blocking: bool = True, block_timeout: float = 10.0) -> None:
        self.redis = redis_client
        self.key = f"lock:{key}"
        self.ttl = ttl_seconds
        self.blocking = blocking
        self.block_timeout = block_timeout
        self.token = str(uuid.uuid4())
        self._acquired = False

    def acquire(self) -> bool:
        deadline = time.monotonic() + self.block_timeout
        while True:
            if self.redis.set(self.key, self.token, nx=True, ex=self.ttl):
                self._acquired = True
                return True
            if not self.blocking or time.monotonic() >= deadline:
                return False
            time.sleep(0.05 + random.uniform(0, 0.05))  # jitter here too

    def release(self) -> None:
        if self._acquired:
            self.redis.eval(self.RELEASE_SCRIPT, 1, self.key, self.token)
            self._acquired = False

    def __enter__(self) -> RedisLock:
        if not self.acquire():
            raise TimeoutError(f"could not acquire lock {self.key}")
        return self

    def __exit__(self, *exc: Any) -> None:
        self.release()


# ===========================================================================
# 4. IDEMPOTENCY
# ===========================================================================
class IdempotencyStore:
    """
    "Process this message exactly once" is not achievable over a network.
    "Process this message AT LEAST once, and make processing it twice harmless"
    is achievable, and is what every real system does.

    Two uses in this repo:

    a) CONSUMERS. Before handling event E, SET NX its event_id. If the key
       already existed, this is a redelivery — acknowledge and skip. Kafka and
       RabbitMQ both guarantee at-least-once; without this check, a rebalance or
       a redelivered ack means stock is decremented twice.

    b) HTTP APIs. The client sends an `Idempotency-Key` header. Same key +
       same body => return the stored response instead of creating a second
       order. This is how Stripe's API works and why a flaky mobile connection
       does not double-charge anyone.

    Redis TTL rather than forever: duplicates arrive within seconds or minutes,
    not weeks. 24h is a generous default; storing keys forever is an unbounded
    memory leak with a nice name.

    Note the race in (b): two identical requests arriving simultaneously. The
    `SET NX` makes exactly one of them the winner; the loser polls briefly for
    the winner's stored response. Handled in `remember` / `recall` below.
    """

    def __init__(self, redis_client: Any, namespace: str, ttl_seconds: int = 86_400) -> None:
        self.redis = redis_client
        self.ns = namespace
        self.ttl = ttl_seconds

    def _key(self, token: str) -> str:
        return f"idem:{self.ns}:{token}"

    def claim(self, token: str) -> bool:
        """True if this caller is the first to see `token` (so: do the work)."""
        return bool(self.redis.set(self._key(token), "in-progress",
                                   nx=True, ex=self.ttl))

    def remember(self, token: str, result: str) -> None:
        """Store the outcome so a replay can be answered without redoing work."""
        self.redis.set(self._key(token), result, ex=self.ttl)

    def recall(self, token: str) -> str | None:
        value = self.redis.get(self._key(token))
        if value is None:
            return None
        value = value.decode() if isinstance(value, bytes) else value
        return None if value == "in-progress" else value

    def release(self, token: str) -> None:
        """Call on failure, so a transient error does not block a legitimate retry."""
        self.redis.delete(self._key(token))

    @staticmethod
    def fingerprint(*parts: Any) -> str:
        """
        Stable hash of a request body. Used to reject the nasty case where a
        client reuses an idempotency key with a DIFFERENT payload — that is a
        client bug and must be a 422, not a silently wrong response.
        """
        joined = "|".join(str(p) for p in parts)
        return hashlib.sha256(joined.encode()).hexdigest()


# ===========================================================================
# 5. RATE LIMITING (token bucket in Redis)
# ===========================================================================
class TokenBucketLimiter:
    """
    Token bucket, implemented in Lua so the read-modify-write is atomic across
    all your API replicas.

    Why token bucket rather than "count requests per minute": a fixed window lets
    a client send the full quota at 11:59:59 and again at 12:00:00 — double the
    intended rate at the boundary. A token bucket refills continuously, so the
    long-run rate is honest while still allowing a short burst, which is what
    real clients actually need.

    Rate limiting is not only about abuse. It is backpressure: it is how a
    service protects its database from its own success.
    """

    SCRIPT = """
    local key      = KEYS[1]
    local rate     = tonumber(ARGV[1])   -- tokens per second
    local capacity = tonumber(ARGV[2])   -- burst size
    local now      = tonumber(ARGV[3])
    local cost     = tonumber(ARGV[4])

    local data   = redis.call('HMGET', key, 'tokens', 'ts')
    local tokens = tonumber(data[1])
    local ts     = tonumber(data[2])
    if tokens == nil then tokens = capacity; ts = now end

    tokens = math.min(capacity, tokens + (now - ts) * rate)
    local allowed = 0
    if tokens >= cost then
        tokens = tokens - cost
        allowed = 1
    end

    redis.call('HMSET', key, 'tokens', tokens, 'ts', now)
    redis.call('EXPIRE', key, math.ceil(capacity / rate) * 2)
    return {allowed, math.floor(tokens)}
    """

    def __init__(self, redis_client: Any, *, rate_per_minute: int = 120,
                 burst: int | None = None) -> None:
        self.redis = redis_client
        self.rate = rate_per_minute / 60.0
        self.capacity = burst or rate_per_minute
        self._sha: str | None = None

    def allow(self, identity: str, cost: int = 1) -> tuple[bool, int]:
        """Returns (allowed, tokens_remaining). `identity` = user id or IP."""
        if self._sha is None:
            self._sha = self.redis.script_load(self.SCRIPT)
        allowed, remaining = self.redis.evalsha(
            self._sha, 1, f"rl:{identity}", self.rate, self.capacity,
            time.time(), cost,
        )
        return bool(allowed), int(remaining)


# ===========================================================================
# 6. GRACEFUL SHUTDOWN
# ===========================================================================
class GracefulShutdown:
    """
    When Kubernetes (or `docker stop`) wants a container gone it sends SIGTERM,
    waits, then sends SIGKILL. Code that ignores SIGTERM gets killed mid-work:
    a half-handled event, an uncommitted offset, a dropped request.

    The contract for a well-behaved process:
        on SIGTERM -> stop accepting NEW work, finish the item in hand,
                      commit offsets, close connections, exit 0.

    Every long-running loop in this repo checks `shutdown.requested`. That one
    habit is the difference between rolling deploys nobody notices and rolling
    deploys that drop messages.
    """

    def __init__(self, signals: Iterable[int] | None = None) -> None:
        import signal as _signal
        self.requested = False
        for sig in signals or (_signal.SIGTERM, _signal.SIGINT):
            try:
                _signal.signal(sig, self._handle)
            except (ValueError, OSError):  # not in the main thread
                pass

    def _handle(self, signum: int, _frame: Any) -> None:
        log.info("shutdown signal received, draining", extra={"signal": signum})
        self.requested = True

    def __bool__(self) -> bool:
        return not self.requested
