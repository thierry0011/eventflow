# 01 — Architecture

## The happy path, drawn

```
CUSTOMER          ORDERS (Django)         KAFKA         INVENTORY (FastAPI)
   │                    │                   │                   │
   │ POST /orders       │                   │                   │
   ├───────────────────>│                   │                   │
   │                    │ ┌─ BEGIN ──────┐  │                   │
   │                    │ │ INSERT order │  │                   │
   │                    │ │ INSERT items │  │                   │
   │                    │ │ INSERT outbox│  │                   │
   │                    │ └─ COMMIT ─────┘  │                   │
   │ 201 {PENDING}      │                   │                   │
   │<───────────────────┤                   │                   │
   │                    │                   │                   │
   │          [relay process]               │                   │
   │                    ├─ order.created ──>│                   │
   │                    │                   ├──────────────────>│
   │                    │                   │        ┌─ BEGIN ──────────┐
   │                    │                   │        │ dedup check      │
   │                    │                   │        │ SELECT FOR UPDATE│
   │                    │                   │        │ reserve stock    │
   │                    │                   │        │ INSERT outbox    │
   │                    │                   │        └─ COMMIT ─────────┘
   │                    │                   │<─ inventory.reserved ─────┤
   │                    │<──────────────────┤                   │
   │          ┌─ BEGIN ─────────┐           │                   │
   │          │ dedup check     │           │                   │
   │          │ SELECT FOR UPDATE           │                   │
   │          │ status=CONFIRMED│           │                   │
   │          │ INSERT outbox   │           │                   │
   │          └─ COMMIT ────────┘           │                   │
   │                    ├─ order.confirmed ─>│                  │
   │                    │                   │                   │
   │                    │                   ▼
   │                    │        NOTIFICATIONS (Flask)
   │                    │                   │  consumes, writes a row,
   │                    │                   │  publishes to RabbitMQ
   │                    │                   ▼
   │                    │            [worker sends email]
   │<────────────────── email ──────────────┘
```

Three things to notice:

1. **The customer's request returns before anything is reserved.** `201` with
   status `PENDING` is the honest answer. An API that waits for the whole saga
   is a distributed monolith with a loading spinner.
2. **Every box labelled BEGIN/COMMIT includes an outbox insert.** The state
   change and the announcement of it are atomic, everywhere, in every service.
3. **Nobody calls anybody.** After the initial POST, there is not one
   synchronous call in the whole flow except the pricing lookup.

## The failure path

```
order.created ──> inventory: not enough stock
                      │
                      ├─ publishes inventory.rejected  (and COMMITS)
                      ▼
orders: status = CANCELLED, failure_reason = "insufficient stock"
                      │
                      ├─ publishes order.cancelled
                      ▼
notifications: "Your order could not be completed"
```

And the path nobody writes until it bites them:

```
order.created ──> (inventory is down / the message is lost / the consumer is stuck)
                      │
                      │   ... 15 minutes pass ...
                      ▼
orders: Celery beat sweeper finds a PENDING order older than the TTL
        → CANCELLED, publishes order.cancelled
                      │
inventory comes back, consumes order.cancelled → releases any reservation
                      │
and independently, inventory's own sweeper releases reservations past their TTL
```

**Both sides have a timeout.** Neither relies on the other being alive. That is
what "self-healing" actually means in practice — not magic, just two sweepers
and an expiry column.

## The event catalogue

| Event | Producer | Consumers | Carries |
|---|---|---|---|
| `order.created` | orders | inventory, notifications | order id, customer, lines with prices |
| `order.confirmed` | orders | notifications | order id, total |
| `order.cancelled` | orders | inventory, notifications | order id, reason, reservation id |
| `inventory.reserved` | inventory | orders, notifications | order id, reservation id, expiry |
| `inventory.rejected` | inventory | orders | order id, reason, which SKUs |
| `inventory.released` | inventory | (audit) | order id, what was returned |
| `product.restocked` | inventory | (audit) | sku, new quantity |

Defined once in `libs/eventflow_common/envelope.py`. One file, the entire
contract of the system.

## Topics, partitions and consumer groups

```
topic: orders.events          topic: inventory.events
  partition 0 ─┐                partition 0 ─┐
  partition 1 ─┼─ keyed by       partition 1 ─┼─ keyed by order_id
  partition 2 ─┘   order_id      partition 2 ─┘

consumer groups reading orders.events:
   "inventory-service"      ← reserves stock
   "notifications-service"  ← sends emails
```

- **The key is the `order_id`**, so every event for one order lands on one
  partition and is therefore consumed **in order**. Kafka guarantees ordering
  *within a partition only*. Key by the aggregate, always.
- **Two groups, same topic, independent copies.** Adding an analytics consumer
  tomorrow requires zero changes to the orders service. That is the property
  you bought a log for.
- **Partition count caps parallelism.** 3 partitions means a 4th consumer in a
  group idles. You can add partitions later but not remove them, and adding
  them re-shuffles keys — so pick a number comfortably above your expected
  consumer count (12 is a common default) and move on.

## Process inventory

One "service" is several processes. This surprises people coming from a single
`manage.py runserver`.

| Process | Job | Scales on |
|---|---|---|
| `orders-api` | HTTP | request rate |
| `orders-outbox` | publish outbox → Kafka | outbox depth |
| `orders-consumer` | consume inventory events | consumer lag |
| `orders-celery` | sweeper + periodic jobs | fixed (1–2) |
| `inventory-api` | HTTP (read-heavy, cached) | request rate |
| `inventory-consumer` | reserve/release stock | consumer lag, partitions |
| `inventory-outbox` | outbox relay + reservation sweeper | outbox depth |
| `notifications-api` | small read API | request rate |
| `notifications-bridge` | Kafka → RabbitMQ | consumer lag |
| `notifications-worker` | drain RabbitMQ, send | queue depth |

Same image, different command. **Web traffic and background work never share a
process** — one will starve the other, and they fail for different reasons.

## Where each pattern lives

| Pattern | Read it here |
|---|---|
| Transactional outbox | `orders/models.py::OutboxMessage`, `relay_outbox.py` |
| `SKIP LOCKED` queue | `relay_outbox.py`, `inventory/app/outbox.py` |
| Idempotent consumer | `orders/services.py::_already_processed`, `inventory/app/service.py::already_processed` |
| Saga + compensation | `orders/services.py` handlers, `inventory/app/service.py::release_reservation` |
| Saga timeout | `orders/tasks.py::expire_stale_orders` |
| Pessimistic locking | `inventory/app/service.py::reserve_stock` |
| Optimistic locking | `Product.version` |
| Cache-aside + invalidation | `inventory/app/cache.py`, `routes.py::get_product` |
| Circuit breaker | `eventflow_common/reliability.py`, used in `orders/services.py` |
| Retry + jitter | `eventflow_common/reliability.py::retry` |
| Rate limiting | `eventflow_common/reliability.py`, `inventory/app/deps.py` |
| RabbitMQ retry + DLQ | `eventflow_common/bus.py::declare_topology`, `notifications/app/worker.py` |
| API idempotency keys | `orders/views.py::create` |
| Correlation ids | each service's middleware |
| Graceful shutdown | `eventflow_common/reliability.py::GracefulShutdown` |
| Liveness vs readiness | each service's `health.py` / `main.py` |

## Trade-offs this design makes, stated honestly

**Eventual consistency.** For a few hundred milliseconds — or minutes, if
something is down — the orders service thinks an order is PENDING while
inventory has already reserved the stock. There is no instant in which the
whole system is consistent. Your UI must be designed for this, your product
manager must understand it, and your support team must be told.

**Polling the outbox costs latency.** Up to `OUTBOX_POLL_INTERVAL_SECONDS` of
delay before an event leaves. Change Data Capture (Debezium reading the
Postgres WAL) removes it and adds an operational component to run. For most
teams, polling is correct until it measurably is not.

**At-least-once everywhere.** Every handler must be idempotent, forever,
including ones written by people who join next year. This is a permanent tax on
the team, paid in code review.

**Three frameworks is wrong for a real company.** It is right for this repo,
because comparison is the lesson. A real team picks one and gets fluent.
