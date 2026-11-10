# 00 — Project brief

## The business, in plain language

EventFlow is a very small online shop. A customer places an order; the shop must
check that the goods exist, set them aside, confirm the order, and tell the
customer. If the goods are not there, the order must be cancelled cleanly and
nothing may be left half-done.

That is the entire domain. It is deliberately boring, because you are not here
to learn retail — you are here to learn what happens when that boring workflow
is split across three services that can each fail independently.

## The three services

| Service | Owns | Framework |
|---|---|---|
| **orders** | Orders, order lines, customers, the saga | Django + DRF |
| **inventory** | Products, stock levels, reservations | FastAPI |
| **notifications** | What we told the customer, and when | Flask |

Each has **its own database**. No service may read another's tables. The only
way to learn something another service knows is to ask it over HTTP or to
consume an event it published. That constraint is the whole exercise — remove
it and you have a monolith with network latency bolted on.

## The one workflow, step by step

1. Customer POSTs an order. The orders service records it as `PENDING` and — in
   **the same database transaction** — writes an `order.created` event into its
   outbox table. The customer gets `201 Created` with status `PENDING`. The
   shop has not promised anything yet; it has acknowledged the request.
2. A relay process publishes that event to Kafka.
3. The inventory service consumes it and tries to reserve stock, under a row
   lock so two orders cannot take the same last unit. It publishes either
   `inventory.reserved` or `inventory.rejected`.
4. The orders service consumes that and moves the order to `CONFIRMED` or
   `CANCELLED`, publishing `order.confirmed` or `order.cancelled`.
5. The notification service consumes everything, records what should be sent,
   and pushes a work item onto RabbitMQ.
6. A worker drains that queue and sends the message, retrying on failure and
   dead-lettering after five attempts.
7. If inventory never answers at all, a scheduled sweeper in the orders service
   cancels the order after 15 minutes, and the inventory service releases any
   reservation whose TTL has passed. **The system heals itself.**

## What makes this hard, and therefore worth building

Every step above is a place where things go wrong in ways a single-process
application never experiences:

- **The dual write.** Saving the order and publishing the event are two
  different systems. There is no transaction across both. → the **outbox**.
- **Duplicate delivery.** Every broker worth using guarantees *at-least-once*,
  not exactly-once. Your handler will see the same event twice. → **idempotent
  consumers**.
- **No distributed rollback.** Once inventory has committed a reservation, the
  orders service cannot "undo" it. → **sagas with compensating actions**.
- **Concurrency on shared resources.** Two orders, one unit of stock, two
  replicas. → **row locks, ordered acquisition, database constraints**.
- **Partial failure.** Inventory is down. Is the order rejected? Queued? Does
  the customer wait? → **timeouts, circuit breakers, sweepers**.
- **Nothing is where you left it.** One user action touches three services and
  six processes. → **correlation ids, structured logs, metrics**.

A monolith has none of these problems. **That is worth saying out loud:
microservices are a cost you pay for independent deployment and scaling.** If
you cannot name the benefit you are buying, build the monolith. An enormous
amount of real-world engineering misery comes from teams who adopted this
architecture because it sounded senior.

## What you are expected to know afterwards

Not "I have seen Kafka". Specifically:

- Why a dual write is unsafe, and three ways to fix it.
- Why your consumer must be idempotent even though the broker says "at least
  once" as if it were a minor detail.
- When to use Kafka and when to use RabbitMQ, with the data-structure reason.
- Why `SELECT ... FOR UPDATE` ordered by primary key prevents deadlocks.
- What `SKIP LOCKED` does and why it makes a database table into a work queue.
- Why liveness must not check the database.
- Why your metric labels must never contain an id.
- What a saga is, why it has no rollback, and what a compensating action is.
- Why async Python collapses if you put one blocking call in it.
- When caching is dangerous rather than merely stale.

The next document draws the whole system. Read it with `make logs` running.
