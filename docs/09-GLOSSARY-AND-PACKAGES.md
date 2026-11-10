# 09 — Glossary and the package landscape

---

## Glossary

**Aggregate** — a cluster of objects treated as one unit for consistency. An
Order plus its OrderItems. The aggregate root (the Order) is what you lock,
what you publish events about, and what becomes the partition key.

**At-least-once** — delivery that may duplicate but never loses. The practical
default; requires idempotent consumers.

**Backpressure** — a slow consumer signalling upstream to slow down. Rate
limiting, bounded queues and prefetch limits are all backpressure.

**Bulkhead** — isolating resources so one failure cannot consume everything.
Separate connection pools or thread pools per dependency.

**CDC (Change Data Capture)** — publishing database changes by tailing the
write-ahead log (Debezium). An alternative to the outbox.

**Choreography** — services reacting to each other's events with no central
coordinator.

**Circuit breaker** — fails fast when a dependency is known to be down, so you
stop hammering it and stop parking your own threads on timeouts.

**Compensating action** — a forward transaction that negates an earlier
committed one. A refund, not a rollback.

**Consumer group** — a set of consumers sharing a topic's partitions. Different
groups each get a full copy of the stream.

**Consumer lag** — latest offset minus committed offset. The heartbeat of an
event-driven system.

**Correlation id** — one id per user action, carried across every service and
log line.

**CQRS** — separating the write model from the read model, often with different
storage for each.

**Dead letter queue (DLQ)** — where messages go after exhausting retries, for a
human to inspect.

**Distributed monolith** — microservices that must be deployed together. All of
the cost, none of the benefit.

**Dual write** — writing to two systems without a shared transaction. The
problem the outbox solves.

**Eventual consistency** — the system converges, but there is no instant at
which all parts agree.

**Event sourcing** — the event log *is* the state; current state is a
projection of it.

**Fan-out** — one event delivered to many independent consumers.

**Idempotent** — doing it twice has the same effect as doing it once.

**N+1** — one query for a list plus one more per item. The most common ORM
performance bug.

**Optimistic locking** — detect conflicts with a version column instead of
holding a lock.

**Orchestration** — a coordinator service drives a workflow and holds its
state.

**Outbox** — a table written in the same transaction as the business change,
published later by a relay.

**Partition** — an ordered shard of a Kafka topic. Ordering is guaranteed
within one, and only within one.

**Pessimistic locking** — `SELECT ... FOR UPDATE`. Hold the row; make everyone
else wait.

**Poison message** — a message that fails every time it is processed. Needs a
DLQ or it blocks the queue forever.

**RED metrics** — Rate, Errors, Duration.

**Rebalance** — Kafka reassigning partitions when group membership changes.

**Saga** — a workflow of local transactions across services, with compensation
instead of rollback.

**Semantic lock** — a status field (`PENDING`) that marks a record as
mid-workflow so others leave it alone.

**`SKIP LOCKED`** — skip rows another transaction has locked. Turns a table
into a concurrent work queue.

**Stampede (dogpile)** — many requests missing the same expired cache key
simultaneously.

**Thundering herd** — many clients retrying in sync after a failure. Fixed by
jitter.

**Transactional outbox** — see Outbox. Learn this one properly.

---

## The Python backend package landscape

What a senior engineer is expected to recognise. Not "has used all of" — "knows
what it is for and roughly when to reach for it".

### Web frameworks
| Package | For |
|---|---|
| `django` + `djangorestframework` | batteries-included; ORM, migrations, auth, admin |
| `fastapi` | async, Pydantic validation, auto OpenAPI |
| `flask` | minimal core, assemble your own |
| `litestar` | FastAPI-like, more opinionated |
| `starlette` | the ASGI toolkit under FastAPI |

### Servers
`gunicorn` (WSGI, process manager) · `uvicorn` (ASGI) · `hypercorn` (ASGI,
HTTP/2) · `granian` (Rust-based, newer)

### Data
| Package | For |
|---|---|
| `sqlalchemy` | the ORM and Core outside Django |
| `alembic` | migrations for SQLAlchemy |
| `psycopg` (v3) | Postgres driver, sync |
| `asyncpg` | Postgres driver, async, fast |
| `redis` | Redis, with `redis.asyncio` built in |
| `pydantic` | validation and settings |
| `pandas` / `polars` | analytics; polars is faster and lazier |

### Messaging
| Package | For |
|---|---|
| `kafka-python` | Kafka, sync, pure Python |
| `confluent-kafka` | Kafka, C bindings, faster, the production choice |
| `aiokafka` | Kafka, async |
| `pika` | RabbitMQ, sync |
| `aio-pika` | RabbitMQ, async |
| `celery` | distributed task queue |
| `dramatiq` | simpler Celery alternative |
| `arq` | async-native task queue on Redis |
| `taskiq` | async task queue, FastAPI-flavoured |

### Testing
`pytest` · `pytest-asyncio` · `pytest-django` · `pytest-cov` · `factory-boy` ·
`faker` · `freezegun` · `responses`/`respx` (HTTP mocking) · `testcontainers`
(real services in tests) · `hypothesis` (property-based testing — worth
learning, few people have) · `locust`/`k6` (load)

### Ops
`prometheus-client` · `opentelemetry-*` · `structlog` · `sentry-sdk` ·
`python-json-logger` · `tenacity` (retry done properly)

### Tooling
| Package | For |
|---|---|
| `ruff` | linter + formatter; replaces flake8, isort, black, and is far faster |
| `mypy` / `pyright` | static type checking |
| `uv` | dependency resolution and installs; very fast, increasingly the default |
| `poetry` / `pdm` | dependency and packaging management |
| `pre-commit` | run checks before a commit lands |
| `bandit` / `pip-audit` | security scanning |

### Worth knowing exist
`httpx` (HTTP client with timeouts by default) · `tenacity` · `orjson` (fast
JSON) · `msgspec` (fast serialisation) · `dependency-injector` ·
`strawberry` (GraphQL) · `grpcio` · `boto3` · `temporalio` (durable workflow
orchestration — the serious answer to complex sagas)

---

## Reading that is actually worth the time

**Books**
- *Designing Data-Intensive Applications*, Martin Kleppmann — the single best
  book in this field. Read it slowly, twice.
- *Building Microservices*, Sam Newman — boundaries and organisational
  realities.
- *Microservices Patterns*, Chris Richardson — sagas and the outbox in depth.
- *Database Internals*, Alex Petrov — once you want to know *why* indexes
  behave as they do.
- *Site Reliability Engineering* (Google, free online) — the operations half.

**Documentation worth reading end to end**
- Kafka's design document.
- RabbitMQ's tutorials — all seven, in order.
- The Postgres docs on concurrency control and indexes.
- Django's docs on transactions and database optimisation.

**Specific articles**
- "How to do distributed locking", Martin Kleppmann — why Redlock is not what
  people think.
- "Exponential backoff and jitter", AWS Architecture Blog.
- Stripe's idempotency documentation — the clearest explanation anywhere.
- microservices.io's pattern catalogue.

**The most valuable habit:** read the source of the libraries you depend on.
`kafka-python`'s consumer loop and `celery`'s task routing are both readable,
and an afternoon in either teaches more than a month of blog posts.
