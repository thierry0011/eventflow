# 02 — The learning path

**This is the main document. Everything else in `docs/` is reference material
that this path tells you when to read.**

Eight phases. Each has: what you are learning, what to do, what to read, and a
**gate** — a question you must be able to answer from memory before moving on.
If you cannot answer the gate, you have not finished the phase. Reading it again
will not help; the gates are all about *doing*.

**Time:** roughly 6–10 weeks at 8–10 focused hours a week. Faster than that and
you are reading, not learning. There is no shortcut, and anyone who tells you
otherwise is selling a course.

**The one rule:** type the code. Do not copy-paste. Typing is slow, which is
the point — it is the only reliable way to notice the line you did not
understand.

---

## Phase 0 — Make it run (half a day)

**Goal:** the whole system alive on your machine, and one order flowing through
it end to end.

**Do:**
```bash
cp .env.example .env
make up
make migrate
make seed
make smoke
```

Then, in a second terminal, `make logs` and run `make smoke` again. Watch the
same correlation id appear in orders, then inventory, then notifications.

Open and click around:
- http://localhost:8001/api/v1/ — Django's browsable API
- http://localhost:8002/docs — FastAPI's Swagger UI
- http://localhost:8080 — Kafka UI: look at the topics, the messages, the
  consumer groups and their lag
- http://localhost:15672 — RabbitMQ UI: watch `notifications.work`,
  `notifications.retry` and `notifications.dlq` while the smoke test runs

**Read:** `README.md`, `docs/00-PROJECT-BRIEF.md`, `docs/01-ARCHITECTURE.md`.

**Gate:** Draw the full path of one order on paper, from HTTP request to email,
naming every process it passes through, without looking. If you cannot, run
`make smoke` again with the logs open until you can.

---

## Phase 1 — The HTTP layer, properly (week 1)

**Goal:** stop writing views the way tutorials do.

**Read, in this order, slowly, typing out the bits you do not recognise:**
1. `services/orders-django/orders/models.py` — every comment
2. `services/orders-django/orders/serializers.py`
3. `services/orders-django/orders/views.py`
4. `services/orders-django/orders/services.py` — just `create_order` for now
5. `docs/03-NOTES-CORE-CONCEPTS.md` sections 1–4

**The ideas:** the service layer; validate at the boundary; separate In/Out
schemas; authorisation in the queryset, not the handler; pagination that does
not die at page 4000; a consistent error envelope; API versioning; idempotency
keys.

**Build (do not skip this):** add a `GET /api/v1/orders/summary` endpoint
returning each status and its count for the current user. Requirements: one SQL
query (use `.values().annotate()`), a test, correct authorisation. If you wrote
a Python loop over all orders, you missed the point — do it again.

**Gate:** Explain why `fields = "__all__"` in a serializer is a security bug,
and why the ownership test returns 404 rather than 403.

---

## Phase 2 — Data, transactions and concurrency (week 2)

**Goal:** this is where most "senior" interviews are actually decided.

**Read:**
1. `services/inventory-fastapi/app/models.py`
2. `services/inventory-fastapi/app/service.py::reserve_stock` — the long
   comment at the top is the single densest paragraph in the repo
3. `docs/05-NOTES-DATA-AND-CACHING.md` sections 1–5
4. `services/inventory-fastapi/alembic/env.py` — the migration rules

**The ideas:** transaction boundaries; never do network I/O inside a
transaction; pessimistic vs optimistic locking; deadlocks and ordered lock
acquisition; `SKIP LOCKED`; database constraints as the last line of defence;
indexes, including composite and partial; N+1 and how to see it; money is never
a float; expand/contract migrations.

**Do — break it on purpose:**

```bash
# 1. Prove the race is real. Comment out `.with_for_update()` in reserve_stock,
#    rebuild, then run the concurrency test against real Postgres:
make up
docker compose exec inventory-api pytest -m integration -q
# It should FAIL, and the CHECK constraint should be what saves you.
# Put the lock back. Watch it pass. You now understand row locking.

# 2. Cause a deadlock. Change the `.order_by(Product.sku)` in reserve_stock to
#    `.order_by(Product.sku.desc())` in ONE place, then fire two concurrent
#    orders with the same two SKUs in opposite order. Read the Postgres log.
```

**Build:** add a `GET /api/v1/products/low-stock?threshold=N` endpoint. Make it
fast with an index, prove the index is used with `EXPLAIN ANALYZE`
(`make psql db=inventory_db`), and write down the query plan before and after.

**Gate:** Explain, without notes, why `SELECT ... FOR UPDATE ... ORDER BY sku`
cannot deadlock, and what `SKIP LOCKED` changes. Then explain why the CHECK
constraint exists when the application already checks.

---

## Phase 3 — Async Python and FastAPI (week 3)

**Goal:** understand the event loop well enough to not destroy it.

**Read:**
1. `services/inventory-fastapi/app/db.py` — the pooling arithmetic
2. `services/inventory-fastapi/app/main.py` — lifespan, middleware
3. `services/inventory-fastapi/app/deps.py` — dependency injection
4. `docs/03-NOTES-CORE-CONCEPTS.md` sections 5–7

**The ideas:** what the event loop actually does; concurrency vs parallelism;
why one blocking call stalls everything; `asyncio.to_thread`; `asyncio.gather`;
async context managers; ContextVar vs threading.local; connection pool sizing;
`expire_on_commit=False` and the `MissingGreenlet` error.

**Do — feel the failure:**
```python
# Add this to any route in routes.py, then hammer the service with
# `ab -n 50 -c 10 http://localhost:8002/api/v1/products/SKU-MOUSE-01`
import time
time.sleep(2)        # blocking — watch throughput collapse to ~0.5 rps
# now replace with:
await asyncio.sleep(2)   # non-blocking — throughput stays high
```
That ten-minute experiment teaches more than any article about async.

**Build:** add a `GET /api/v1/products/bulk?skus=A,B,C` endpoint that fetches
prices from the database and, concurrently, each product's cached view, using
`asyncio.gather`. Measure with and without `gather`.

**Gate:** Someone's FastAPI service is handling 40 requests per second and the
CPU is at 5%. Give three hypotheses and how you would test each.

---

## Phase 4 — Events, Kafka and the outbox (week 4) ★

**★ This is the most important phase in the path.**

**Read:**
1. `libs/eventflow_common/envelope.py` — all of it, twice
2. `libs/eventflow_common/bus.py` — the Kafka sections
3. `services/orders-django/orders/management/commands/relay_outbox.py`
4. `docs/04-NOTES-EVENT-DRIVEN.md` — the whole document

**The ideas:** the dual-write problem; the transactional outbox; at-least-once
vs at-most-once vs exactly-once; why exactly-once across a broker is a
marketing term; idempotent consumers; partition keys and ordering; consumer
groups, rebalancing and lag; offset management; schema evolution; events as
contracts; Kafka vs RabbitMQ.

**Do — break delivery on purpose:**
```bash
# 1. Kill Kafka mid-flight. Orders must still succeed; outbox depth grows.
docker compose stop kafka
make smoke                     # 201 Created still works
make psql db=orders_db         # SELECT count(*) FROM outbox_messages WHERE published_at IS NULL;
docker compose start kafka     # watch the backlog drain by itself
```
This is the moment the outbox pattern stops being theory.

```bash
# 2. Prove duplicates are handled. Reset a consumer group to the beginning and
#    replay the entire topic:
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --group inventory-service \
  --reset-offsets --to-earliest --all-topics --execute
# Stock must NOT be double-reserved. Then comment out the `already_processed`
# check and do it again. Watch your inventory go negative — or watch the CHECK
# constraint stop it, which is the point of having both.
```

```bash
# 3. Watch a rebalance. Scale the consumer up and down while events flow:
docker compose up -d --scale inventory-consumer=3
make lag
```

**Build:** add a new event, `order.completed`, emitted when a confirmed order
ships (add a `POST /orders/{id}/complete` endpoint). Have inventory consume it
and convert the reservation from `HELD` to `COMMITTED`, decrementing
`quantity_on_hand`. Full outbox, full idempotency, with tests.

**Gate:** Explain the dual-write problem to a rubber duck, out loud, including
why swapping the order of the two writes does not fix it, and name three
solutions with their trade-offs.

---

## Phase 5 — Sagas, failure and compensation (week 5)

**Read:**
1. `services/orders-django/orders/services.py` — the saga handlers
2. `services/inventory-fastapi/app/service.py::release_reservation`
3. `services/orders-django/orders/tasks.py`
4. `docs/04-NOTES-EVENT-DRIVEN.md`, the saga section

**The ideas:** why there is no distributed rollback; choreography vs
orchestration; compensating actions; saga timeouts; semantic locks; the
reservation-after-cancellation race; retry vs circuit breaker; graceful
degradation; bulkheads.

**Do:**
```bash
# 1. Make inventory fail every time (raise in handle_order_created). Watch the
#    orders pile up PENDING, then watch the Celery sweeper cancel them.
#    Reduce RESERVATION_TTL_SECONDS to 30 so you do not wait 15 minutes.

# 2. Force the nasty race: cancel an order while inventory is reserving it.
#    Add `time.sleep(5)` inside reserve_stock, POST an order, then immediately
#    POST /orders/{id}/cancel. Follow the logs and confirm the reservation is
#    released rather than leaked.

# 3. Trip the circuit breaker: `docker compose stop inventory-api`, then POST
#    an order. The first few requests take ~2s (the timeout); after five
#    failures they fail in microseconds. That is the breaker.
```

**Build:** add a `payments` step to the saga — a fourth service, or a stub
inside orders. `inventory.reserved` → attempt payment → `payment.succeeded`
(→ `CONFIRMED`) or `payment.failed` (→ compensate: release the reservation and
cancel). This is the exercise that proves you understand sagas.

**Gate:** Explain why a saga cannot roll back, and what a compensating action
for "money was taken" looks like. Then explain why every saga needs a timeout.

---

## Phase 6 — Caching, Redis, Celery and RabbitMQ (week 6)

**Read:**
1. `services/inventory-fastapi/app/cache.py` — every comment
2. `libs/eventflow_common/reliability.py` — all five primitives
3. `libs/eventflow_common/bus.py` — the RabbitMQ section
4. `services/notifications-flask/app/worker.py`
5. `docs/05-NOTES-DATA-AND-CACHING.md` sections 6–10

**The ideas:** cache-aside; the three invalidation strategies; stampede,
penetration and avalanche; what must never be cached; distributed locks and
their limits; token-bucket rate limiting; Celery task rules; RabbitMQ
exchanges, queues, bindings, acks, prefetch, TTL-based retry, dead letters.

**Do:**
```bash
# 1. Watch the retry path. The worker fails 15% of the time on purpose.
make smoke
make logs s=notifications-worker
# Open the RabbitMQ UI and watch messages move: work -> retry -> work -> dlq.

# 2. Raise FAILURE_RATE to 1.0 in worker.py. Every message should land in the
#    DLQ after exactly 5 attempts. Then write a management command that
#    replays the DLQ back onto the work queue — every real system needs one.

# 3. Prove the cache is stale-by-design: GET a product, restock it with a
#    direct SQL UPDATE (bypassing the API), GET again. You will see the old
#    value until the TTL expires. Now do it through the API and watch the
#    delete-on-write invalidation work.
```

**Build:** add a Celery task that emails a daily summary of DEAD notifications,
and add a Redis-backed distributed lock so only one replica runs it.

**Gate:** Explain why stock levels may be cached for display but never for the
reservation decision. Then explain why the RabbitMQ retry queue has a TTL
instead of the worker sleeping.

---

## Phase 7 — Production readiness (week 7)

**Read:**
1. `docs/06-NOTES-PRODUCTION-READINESS.md` — the whole document
2. every service's `health.py` / health endpoints
3. `libs/eventflow_common/observability.py`
4. the three `Dockerfile`s and `docker-compose.yml`

**The ideas:** 12-factor; structured logging; correlation ids; RED metrics and
the four golden signals; metric cardinality; liveness vs readiness vs startup;
graceful shutdown and SIGTERM; multi-stage Docker builds; non-root containers;
secrets handling; the security checklist; what to alert on and what to ignore.

**Do:**
```bash
# 1. Graceful shutdown: `docker compose stop orders-consumer` while events are
#    flowing. The logs must show a clean drain, not a kill. Then remove the
#    GracefulShutdown check from the loop and watch the difference.

# 2. Find the slow query. Set db_echo=True in inventory's config, hit the list
#    endpoint, and read the SQL. Then deliberately remove `prefetch_related`
#    from the Django order detail view and count the queries.

# 3. Write three alerts in plain English: one on consumer lag, one on outbox
#    depth, one on DLQ depth. For each, state the threshold, the duration, and
#    what the on-call person should DO. An alert with no action is noise.
```

**Build:** add `/metrics` scraping with a real Prometheus + Grafana in
`docker-compose.yml`, and build one dashboard with: request rate, p99 latency,
error rate, consumer lag, outbox depth, DLQ depth.

**Gate:** Explain why a liveness probe must not check the database. Then
explain why `/metrics` must never have a label containing an order id.

---

## Phase 8 — Testing, and then teaching it (week 8)

**Read:**
1. all three test files, in full
2. `docs/06-NOTES-PRODUCTION-READINESS.md`, the testing section

**The ideas:** the test pyramid; what to mock and what never to mock; testing
idempotency; testing concurrency (and why SQLite cannot); fixtures and
factories; dependency overrides; contract testing between services; what
coverage does and does not measure.

**Do:** pick any three tests and break the code they cover, one at a time.
Confirm each test fails for the right reason. A test that still passes when you
break its subject is worse than no test — it is a false sense of safety.

**Build the capstone — add a fourth service, end to end, alone:**

> A **shipping** service. It consumes `order.confirmed`, creates a shipment,
> and publishes `shipment.dispatched` and `shipment.delivered`. The orders
> service consumes those and moves the order to `COMPLETED`. Inventory consumes
> `shipment.dispatched` and converts the reservation to a real stock decrement.

Requirements, non-negotiable: its own database, outbox, idempotent consumers,
a saga timeout, health endpoints, metrics, graceful shutdown, Dockerfile,
tests, and an entry in the event catalogue. Pick whichever framework you feel
weakest in — that is the one you need the practice in.

**The final gate:** explain the whole system to another engineer for thirty
minutes, at a whiteboard, taking questions. If you can do that — including "why
not just use a monolith?" and "why two brokers?" — you are operating at the
level this repo was built to teach.

---

## How to use the rest of the docs

| Document | When |
|---|---|
| `03-NOTES-CORE-CONCEPTS.md` | Phases 1 and 3 |
| `04-NOTES-EVENT-DRIVEN.md` | Phases 4 and 5 — the heart of it |
| `05-NOTES-DATA-AND-CACHING.md` | Phases 2 and 6 |
| `06-NOTES-PRODUCTION-READINESS.md` | Phases 7 and 8 |
| `07-EXERCISES.md` | after each phase; they are ordered to match |
| `08-SENIOR-CHECKLIST.md` | at the end, and before any interview |
| `09-GLOSSARY-AND-PACKAGES.md` | whenever a word or a package is unfamiliar |

## Honest advice about becoming senior

Finishing this repo will not make you senior. It will give you the **vocabulary
and the reflexes** that senior engineers share, which is most of what separates
a strong mid-level engineer from a senior one in an interview, and perhaps half
of what separates them on the job.

The other half is judgement, and judgement comes from consequences — from
having shipped something that broke at 3am, from arguing for a design and being
wrong, from maintaining your own code two years later. There is no repository
that substitutes for that, and nobody who has been through it will pretend
otherwise.

What you *can* do is arrive already knowing why the outbox exists, so that when
the incident comes you recognise it instead of discovering it. That is what this
is for.
