# 04 — Notes: event-driven architecture and microservices

**The most important document in this repo.** Reference for Phases 4 and 5.

---

## 1. Microservices: the cost and the benefit

The benefit is **independent deployability**. A team can ship their service
without coordinating a release train with four other teams. Everything else
claimed for microservices — scaling, resilience, technology choice — is either
achievable in a monolith or a consequence of that one property.

The cost is **everything becomes a distributed systems problem**:

| In a monolith | In microservices |
|---|---|
| a function call | a network call that can fail, time out, or half-succeed |
| one transaction | a saga with compensation |
| a `JOIN` | data duplication or an API call |
| one deploy | version compatibility between services, forever |
| a stack trace | distributed tracing, if you built it |
| one database to back up | N databases, consistent with each other at no instant |

**The honest rule:** start with a modular monolith. Split out a service when
you have a *specific* reason — a team boundary, a scaling profile that differs
by an order of magnitude, a compliance boundary, a rewrite you want to isolate.
"We want to be scalable" is not a reason; it is a wish.

### The boundary rule

A service owns its data. Nobody else may read its tables. If service A needs
data from B, A either asks B over an API or consumes B's events and keeps its
own copy.

A shared database between services is **the** anti-pattern. It looks like
microservices and behaves like a monolith with network latency, because now
nobody can change a column.

---

## 2. Events vs commands vs queries

| | Event | Command | Query |
|---|---|---|---|
| Says | "this happened" | "do this" | "tell me this" |
| Tense | past | imperative | interrogative |
| Recipients | 0..N, unknown to producer | exactly 1, known | exactly 1, known |
| Can be refused? | no — it already happened | yes | n/a |
| Coupling | low | medium | medium |
| Example | `order.created` | `reserve_stock` | `get_price` |

**Name events in the past tense.** `order.created`, not `create_order`. If your
"events" are named as commands, you have built RPC with extra latency and a
broker nobody needed.

Transport does not determine the type. A command sent over Kafka is still a
command, and it still couples producer to consumer. The question is always:
*does the producer care who acts on this, or what they do?* If yes, it is a
command.

---

## 3. The dual-write problem ★

The first genuinely hard problem in event-driven systems, and the one most
asked about in interviews.

```python
order.save()                          # (1) database
kafka.publish(OrderCreated(order))    # (2) broker
```

There is **no transaction spanning a database and a broker**. So:

- Crash between (1) and (2): the order exists and nobody is ever told. Stock is
  never reserved; the customer waits forever. **Silent, permanent data
  inconsistency.**
- Publish succeeds, transaction then rolls back: you announced an order that
  does not exist. Downstream reserves stock for a ghost.

Swapping the order does not help — it changes which failure you get, not
whether you get one.

### Three solutions

**1. The transactional outbox** (what this repo does)

Write the event into a table *in the same transaction* as the business change.
One database, one transaction, genuinely atomic. A separate relay polls the
table and publishes.

```
BEGIN
  INSERT INTO orders ...
  INSERT INTO outbox ...
COMMIT
        ↓  (separate process)
  SELECT ... WHERE published_at IS NULL FOR UPDATE SKIP LOCKED
  publish to Kafka
  UPDATE outbox SET published_at = now()
```

- ✅ simple, no new infrastructure, works with any database and any broker
- ❌ polling adds latency; the outbox table needs pruning
- Delivery: **at-least-once** (crash after publish, before the UPDATE →
  republish). Which is fine, because consumers are idempotent.

**2. Change Data Capture (CDC)**

Debezium tails the Postgres write-ahead log and publishes changes to Kafka. No
polling, no relay, very low latency.
- ✅ no latency penalty, no application code at all
- ❌ a significant operational component; your events become shaped by your
  schema unless you still write to an outbox table and tail *that*

**3. Event sourcing**

The event log *is* the state. There is no separate write to be inconsistent
with.
- ✅ the problem disappears by construction; perfect audit trail; temporal
  queries
- ❌ a large conceptual and operational commitment; every read becomes a
  projection; schema evolution across years of stored events is genuinely hard

**Pick the outbox unless you have a specific reason not to.** It is boring,
which is the highest compliment available in infrastructure.

---

## 4. Delivery semantics

| | Means | Reality |
|---|---|---|
| **At-most-once** | may be lost, never duplicated | ack before processing. Almost never what you want. |
| **At-least-once** | never lost, may be duplicated | ack after processing. **The practical default.** |
| **Exactly-once** | never lost, never duplicated | not achievable end-to-end across a network |

Kafka advertises "exactly-once semantics". It is real but narrow: it applies to
a Kafka→Kafka transaction (consume, process, produce, commit offsets, all
atomically within Kafka). The moment your side effect is a Postgres write, an
email or an HTTP call, you are back to at-least-once.

**So: accept at-least-once and make your handlers idempotent.** Teams that
chase exactly-once end-to-end spend a quarter and arrive at at-least-once with
extra steps.

---

## 5. Idempotent consumers ★

An operation is idempotent if doing it twice has the same effect as doing it
once.

Three ways to get there, in order of preference:

**1. Naturally idempotent operations.** `SET status = 'CONFIRMED'` is
idempotent. `balance = balance + 10` is not. Prefer absolute assignment over
relative change where you can.

**2. A dedup table** (what this repo does). Insert the `event_id` with a UNIQUE
constraint *in the same transaction as the side effect*. The second attempt
fails the constraint; you catch it and skip.

```python
with transaction.atomic():
    if already_processed(event):    # INSERT event_id, catch IntegrityError
        return
    do_the_work()                   # same transaction
```

Why not Redis for the dedup key? Because if the dedup record and the business
change are in different systems, a rollback can leave "handled" recorded for
work that never happened — and the redelivery that would have fixed it is now
suppressed. Redis is a fine *pre-filter*; Postgres is the truth.

**3. State checks.** "Only act if the order is still PENDING." Natural,
readable, and it handles out-of-order delivery too. Needs a row lock to avoid a
read-modify-write race.

**Pruning matters.** The dedup table grows forever unless you delete rows older
than your broker's retention. Nothing older than that can be redelivered.

---

## 6. Kafka, properly

### The mental model

A **partitioned, append-only, replayable log**. Not a queue — messages are not
deleted when read. Consumers track an **offset**: "I have read up to here".

```
topic: orders.events
  partition 0:  [e0][e1][e2][e3][e4] ...
                               ▲
                      group-A offset=3
                    ▲
           group-B offset=1
```

### The things that matter

**Partition key.** Determines which partition a record lands on, and **ordering
is guaranteed within a partition only**. Key by the aggregate id (`order_id`),
so all events for one order are ordered. Key by something random and you will
process `order.cancelled` before `order.created` and never understand why.

**Consumer groups.** Members of a group *share* the partitions (scale out).
Different groups each get a *full copy* (fan-out). This one parameter is why
adding a new consumer requires zero producer changes.

**Partition count caps parallelism.** 3 partitions = at most 3 useful consumers
in a group. You can add partitions but not remove them, and adding them
re-shuffles keys. Start with more than you need (12 is a common default).

**Rebalancing.** When a consumer joins, leaves or is presumed dead, partitions
are reassigned and in-flight work may be redone by someone else. Causes: slow
handlers missing `max_poll_interval_ms`, deploys, crashes. Keep handlers fast,
fetch small batches, and be idempotent.

**Offset management.** `enable_auto_commit=False`, always, and commit *after*
successful handling. Auto-commit runs on a timer and can commit an offset for a
message you have not finished — that is at-most-once delivery and silent data
loss.

**Producer durability.** `acks=all` + `enable_idempotence=True`. With `acks=1`
you lose records when a leader dies before followers caught up.

**Consumer lag is the health metric.** Lag = latest offset − committed offset.
Flat is healthy; growing means you cannot keep up; a cliff to zero means
someone reset offsets. Alert on *sustained growth*, not on a number.

**Retention is a design decision.** 7 days of retention means 7 days of replay
and 7 days of dedup records to keep. Compacted topics (keep the latest value
per key) are a different tool, good for reference data.

### What Kafka is bad at

- **Per-message retry.** There is no "retry this one message in 10 seconds".
  Progress is the offset, so one stuck message blocks everything behind it in
  that partition. Workarounds: a retry topic, or a DLQ topic after N attempts.
- **Priorities.** No such concept.
- **Small-scale simplicity.** Running Kafka for 100 messages a day is operating
  a cargo ship to cross a pond.

---

## 7. RabbitMQ, properly

### The mental model

A **smart broker with routing**. A producer publishes to an **exchange**; the
exchange routes to **queues** by **bindings**; consumers take messages off a
queue and **ack** them, which deletes them.

```
                  ┌─ direct   exact routing key match
publisher ─> exchange ─ topic    pattern match: order.*, #
                  ├─ fanout   everything to everyone
                  └─ headers  match on headers
```

### The things that matter

**Acks.** `basic_ack` after success. On failure: `basic_nack(requeue=True)`
puts it back — but it comes straight back to you, hot-looping on the same
failure. Better: republish to a retry queue, as this repo does.

**Prefetch (QoS).** Default is unlimited, which means one consumer hoards the
queue while its colleagues idle. Set `prefetch_count` to a small number (10) so
work spreads.

**The retry trick, worth memorising.** RabbitMQ has no native delayed retry.
You build one from a queue with a **message TTL** plus a **dead-letter
exchange**:

```
work queue ──fail──> retry queue (TTL 10s, DLX → main exchange)
                           │ 10s later, automatically
                           └──────> back to the work queue
   after N attempts ──> DLQ (a human looks here)
```

The *broker* does the waiting, not your consumer. A sleeping consumer still
occupies its prefetch slot.

**Durability needs all four**: durable exchange + durable queue + persistent
messages (`delivery_mode=2`) + publisher confirms. Miss one and you lose
messages on a broker restart.

**Competing consumers scale linearly.** Unlike Kafka, there is no partition
cap — add replicas, get throughput.

---

## 8. Kafka or RabbitMQ: the answer

> **Does anyone other than the immediate consumer care that this happened, now
> or in the future?** → event → **Kafka**
>
> **Is this one unit of work that must be done once and then forgotten?**
> → task/command → **RabbitMQ** (or Celery)

| | Kafka | RabbitMQ |
|---|---|---|
| Data structure | replayable log | queue |
| After reading | retained | deleted |
| Replay | yes | no |
| Fan-out | consumer groups | exchanges |
| Ordering | per partition | per queue, best effort |
| Per-message retry | no | yes |
| DLQ | build it | built in |
| Priorities | no | yes |
| Parallelism cap | partition count | none |
| Throughput | very high | high |
| Ops burden | significant | moderate |

This repo uses Kafka between services (facts) and RabbitMQ inside the
notification service (work items), so you implement both.

---

## 9. Sagas ★

A saga is a sequence of local transactions across services, where each step
publishes an event that triggers the next, and **failure is handled by
compensating actions rather than rollback**.

**There is no distributed rollback.** By the time step 3 fails, steps 1 and 2
have already committed in their own databases. "Undo" is a *new forward
transaction* that negates the effect. A refund is not a rollback of a payment —
it is a second transaction, and both appear in the ledger. That is correct, and
auditors prefer it.

### Choreography vs orchestration

**Choreography** (this repo): each service listens and reacts. No central
coordinator.
- ✅ no single point of failure, services stay decoupled, easy to add a step
- ❌ the flow exists in nobody's code — you must read N services to understand
  it; cycles are easy to create by accident

**Orchestration**: a coordinator service tells each participant what to do and
tracks the state machine.
- ✅ the flow is explicit and in one place, easy to visualise and debug
- ❌ the coordinator is a dependency and a bottleneck, and tends to accumulate
  business logic that belongs elsewhere

Rule of thumb: **choreography up to ~4 steps, orchestration beyond that.** Past
four, the "where does this flow actually live?" problem dominates. Temporal,
Camunda and AWS Step Functions exist for the orchestrated case.

### Every saga needs a timeout

If a participant never answers, the saga hangs forever. The order sits PENDING
until a customer complains. **Both sides need their own timeout**, neither
depending on the other being alive:

- the orders service sweeps PENDING orders older than the TTL → cancel
- the inventory service sweeps reservations past their expiry → release

### Semantic locks

`status = PENDING` is a semantic lock: it marks the row as "in a saga, do not
touch". Other operations check it. This is how you get isolation without
holding a database transaction open across a network — which you must never do.

### The race that catches everyone

```
t=900  orders sweeper cancels the order (timeout)
t=901  inventory's reservation arrives
```

If you do not handle it, stock stays reserved for an order nobody will pay for.
The fix is in `handle_inventory_reserved`: take the row lock, re-check whether
the order is terminal, and if it is, emit a release instruction. **Re-check
state after acquiring the lock, always.**

---

## 10. Schema evolution: events are contracts

Once published, an event's shape is a promise to consumers you may not know
about.

**Safe:** adding an optional field. Consumers that ignore unknown fields (this
repo's `from_bytes` drops them) are unaffected.

**Never safe:** removing a field, renaming a field, changing a type, changing
the *meaning* of a field. The last one is the most dangerous because nothing
breaks loudly — downstream just becomes quietly wrong.

**Breaking change → new version.** Publish `order.created.v2` alongside v1,
migrate consumers one at a time, retire v1 when the last one has moved. Yes,
that means dual-publishing for a while. That is the cost of independent
deployment, and it is cheaper than a coordinated release.

**A schema registry** (Avro / Protobuf / JSON Schema) enforces this
automatically in CI, rejecting incompatible schemas before they ship. Worth
adding at the point where you have more teams than you have lunches together.

**Consumers must tolerate unknown event types.** A consumer that crashes on an
event type it does not recognise forces lockstep deployment — the exact thing
you built microservices to escape.

---

## 11. Fat events or thin events

**Thin (notification only):** `{"order_id": "123"}`. The consumer calls back
for details.
- ✅ small, no stale data
- ❌ every consumer now calls the producer — you have rebuilt the coupling the
  broker was supposed to remove, and a producer outage breaks all consumers

**Fat (event-carried state transfer):** the event carries what consumers need.
- ✅ consumers are fully decoupled and work while the producer is down; replay
  reconstructs history correctly
- ❌ bigger messages, and the data is a snapshot that was true when published

**Prefer fat**, which is what this repo does. The snapshot property is usually
a feature: the price on the order event is the price the customer agreed to,
not today's price.
