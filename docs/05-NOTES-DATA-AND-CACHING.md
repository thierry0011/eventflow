# 05 — Notes: databases, concurrency, caching

Reference for Phases 2 and 6.

---

## 1. Transactions and ACID, operationally

- **Atomicity** — all or nothing.
- **Consistency** — constraints hold before and after.
- **Isolation** — concurrent transactions do not see each other's half-work.
- **Durability** — committed means committed, even through a power cut.

### Isolation levels and the anomalies they permit

| Level | Dirty read | Non-repeatable read | Phantom read |
|---|---|---|---|
| Read uncommitted | possible | possible | possible |
| **Read committed** (Postgres default) | no | possible | possible |
| Repeatable read | no | no | possible* |
| Serializable | no | no | no |

\* Postgres's Repeatable Read actually prevents phantoms too; it is implemented
as snapshot isolation.

**Read committed is right for almost everything.** Serializable is correct but
makes transactions fail with serialization errors you must retry — a real cost
most applications do not need to pay.

### The two rules of transaction scope

**1. Keep them short.** A transaction holds a connection and locks. Long
transactions exhaust the pool and block other writers.

**2. Never do network I/O inside one.**

```python
# WRONG — a slow dependency now holds a database connection hostage
with transaction.atomic():
    prices = requests.get(...)     # ← the pool drains under load
    order.save()

# RIGHT
prices = fetch_prices(...)         # outside
with transaction.atomic():
    order.save()
```

Django's `ATOMIC_REQUESTS=True` violates rule 2 by design — it wraps the whole
request, including any external call. This repo turns it off and takes
transactions explicitly.

---

## 2. Locking

### Pessimistic — `SELECT ... FOR UPDATE`

"I am going to change this row; everyone else wait."

```sql
SELECT * FROM products WHERE sku = 'X' FOR UPDATE;
```

Use when contention is likely and correctness is non-negotiable: stock, money,
counters. This is what `reserve_stock` uses.

Variants: `FOR UPDATE NOWAIT` (fail instead of waiting), `FOR UPDATE SKIP
LOCKED` (step over locked rows — the queue trick), `FOR SHARE` (read lock).

### Optimistic — a version column

```sql
UPDATE products SET quantity = 5, version = version + 1
WHERE sku = 'X' AND version = 3;
-- 0 rows affected => somebody else changed it => re-read and retry
```

Use when contention is rare. No locks held, so it scales better — but you must
write the retry loop.

### Deadlocks

```
tx1: locks A, wants B
tx2: locks B, wants A        → neither can proceed
```

Postgres detects it after `deadlock_timeout` (1s) and kills one with
`deadlock detected`.

**The fix is always the same: acquire locks in a consistent global order.** Any
total order works as long as every transaction uses it. `ORDER BY sku` in
`reserve_stock` is exactly this — alphabetical by primary key, easy to audit,
impossible to deadlock against itself.

### `SKIP LOCKED` — a queue in a table

```sql
SELECT * FROM outbox WHERE published_at IS NULL
ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 100;
```

Without `SKIP LOCKED`, a second worker blocks behind the first and you have no
parallelism. With it, each worker takes a different set of rows. This is how
the outbox relay scales horizontally, and it is how you build a perfectly
decent job queue on Postgres without adding a broker.

---

## 3. Indexing

An index is a sorted structure (B-tree, usually) that turns a table scan into a
lookup. It costs write time and disk; every index must earn its place.

**Index these:** foreign keys (Postgres does NOT create them automatically),
columns in `WHERE`, columns in `ORDER BY`, columns in `JOIN`.

**Composite indexes are order-sensitive.** `INDEX (customer_id, created_at)`
serves:
- `WHERE customer_id = ?` ✅ (leftmost prefix)
- `WHERE customer_id = ? ORDER BY created_at` ✅
- `WHERE created_at > ?` ❌ — cannot use it at all

Put the equality column first, the range/sort column second.

**Partial indexes** cover only some rows:
```sql
CREATE INDEX idx_outbox_pending ON outbox (created_at) WHERE published_at IS NULL;
```
The outbox is 99.99% published history the relay never looks at. This index
covers only the pending rows, stays tiny forever, and keeps the hot query fast
regardless of table size. One of the highest-value Postgres features people do
not know about.

**Covering indexes** (`INCLUDE (...)`) let the query be answered from the index
alone — an index-only scan, no table access.

**Read `EXPLAIN ANALYZE`.** Seq Scan on a big table in a hot query is a
problem. Index Scan or Index Only Scan is what you want. Compare the estimated
and actual row counts — a large gap means stale statistics (`ANALYZE`).

**`CREATE INDEX CONCURRENTLY`** on a live table. The plain form locks out
writes for the duration.

---

## 4. N+1 queries

The most common performance bug in ORM code, and the easiest to spot once you
know the shape.

```python
for order in Order.objects.all():       # 1 query
    print(order.customer.name)          # + 1 query PER ORDER
```

**Django:** `select_related` for forward FK/one-to-one (a SQL JOIN),
`prefetch_related` for reverse FK and many-to-many (a second query plus a join
in Python). `Prefetch(...)` when you need to filter the prefetched set.

**SQLAlchemy:** `selectinload`, `joinedload`, `subqueryload`.

**How to see it:** `django-debug-toolbar` locally; `db_echo=True` in
SQLAlchemy; `assertNumQueries` in a test — the only version that stops it
coming back.

---

## 5. Schema decisions that matter

**Money is never a float.** `0.1 + 0.2 != 0.3` in binary floating point. Use
`Decimal`/`NUMERIC`, or integer minor units (cents). A float column for money
is a reconciliation nightmare with a two-year fuse.

**Timestamps are always UTC and always timezone-aware.** Store UTC, render
local in the client. Naive datetimes are an incident waiting for a
daylight-saving boundary.

**UUID vs auto-increment PK:**
- UUID: no information leak (sequential ids tell a competitor your order
  volume), generatable without a round trip, no collisions when merging shards.
  Costs 16 bytes and random insert order, which fragments the B-tree. UUIDv7
  is time-ordered and fixes that.
- Auto-increment: small, fast, naturally ordered, leaks volume, collides across
  shards.

**`ON DELETE` is a business decision, not a style preference.** Ask "does the
child exist independently?" A line item does not — `CASCADE`. A customer's
order records do — `PROTECT`, so deleting a customer fails loudly rather than
silently destroying financial history.

**Constraints belong in the database.** `CHECK (quantity_reserved <=
quantity_on_hand)` holds against a buggy deploy, a migration script and a
manual psql session. Application checks are advisory; constraints are law.

**Denormalise deliberately.** Copying `unit_price` onto the order line is not a
mistake — an invoice must show the price paid, not today's price. Historical
records are immutable snapshots.

---

## 6. Caching

### Cache-aside (lazy loading) — the default

```
look in cache → hit?  return
              → miss? read DB, write cache, return
```

The application owns the logic. A Redis outage degrades to "slow", not
"broken". The alternatives — read-through, write-through, write-behind — push
that logic into the cache layer; know the names, use cache-aside.

### Invalidation

| Strategy | How | Trade-off |
|---|---|---|
| **TTL** | let it expire | simplest; serves stale data for up to the TTL |
| **Write-through** | update cache on write | fast reads; the two can diverge if a write half-fails |
| **Delete-on-write** | delete the key on write | **preferred**; a failed delete is a cache miss (harmless), a failed update is stale data (harmful) |

Keep a TTL *even with* delete-on-write. It is the backstop for a failed
invalidation.

### What must never be cached

**Never cache something whose staleness causes an incorrect decision.**

This repo shows stock levels from cache but reads the row under a lock to
*reserve*. Showing "3 left" when there are 2 is a minor annoyance. Selling the
third is a refund and an angry customer. **Cache for display, never for
decisions.**

### The three failure modes

**Stampede (dogpile).** A hot key expires; 500 concurrent requests all miss and
all hit the database at once. Fix: a short lock so one request rebuilds while
the others wait or serve stale.

**Penetration.** Requests for a key that does not exist bypass the cache every
time — a trivial DoS. Fix: cache the negative result briefly.

**Avalanche.** Many keys written together expire together. Fix: jitter the TTL
(±20%).

### Key design

`inv:v1:product:SKU-A` — namespace, **version**, type, id.

The version prefix is a deploy-safety feature: change the cached shape, bump to
`v2:`, and old entries are simply never read. Without it, a deploy that changes
the shape reads v1 data with v2 code and throws on every cache hit until the
TTL drains.

---

## 7. Redis beyond caching

| Use | Structure | Note |
|---|---|---|
| Cache | string / hash | with TTL |
| Distributed lock | `SET key val NX EX` | unique token + Lua release |
| Rate limiting | hash + Lua | token bucket |
| Session store | hash | with TTL |
| Celery broker | lists | `BRPOP` |
| Leaderboard | sorted set | `ZADD`/`ZRANGE` |
| Deduplication | set / string NX | TTL-bounded |
| Pub/sub | pub/sub | **fire and forget — no persistence, no replay.** Not a message broker. |

**Redis is single-threaded**, so every command is atomic — but a *sequence* of
commands is not. Use Lua (`EVAL`) or `MULTI/EXEC` when several commands must be
atomic together.

**`maxmemory-policy`:** `allkeys-lru` for a pure cache, `noeviction` when Redis
holds data you cannot lose (a Celery broker — evicting a queued task silently
loses work).

**Distributed locks have limits.** `SET NX EX` with a unique token and a Lua
compare-and-delete is correct enough for "do not run this job twice". It is
**not** adequate for money or stock, because under a network partition plus
clock skew two holders can both believe they hold it. For those, use the
database's own locking, in the same transaction as the write.

---

## 8. Celery

**The five rules** (expanded in `orders/tasks.py`):

1. **Pass ids, never objects.** The argument is serialised and may run minutes
   later on another machine. A serialised object is a stale snapshot.
2. **Tasks must be idempotent.** `acks_late=True` plus retries means every task
   will eventually run twice.
3. **Always set a time limit.** A task without one hangs forever holding a
   worker slot.
4. **Retry only transient failures, with backoff and jitter.** Retrying a
   `ValidationError` five times is five identical failures.
5. **Never chain tasks across services.** That is what events are for; a
   cross-service Celery chain is a distributed monolith with a Redis
   dependency.

**Never use the pickle serializer.** It executes arbitrary code on
deserialisation; a compromised broker becomes remote code execution. JSON only.

**Separate queues for separate workloads.** One slow task type can starve
everything else sharing its queue. Route by queue, and size worker pools per
queue.

**Celery or Kafka?** Celery for *my own* deferred work that nobody outside this
service knows about. Kafka for *facts other services need*. Using Celery to
tell another service something means that service needs your broker, your task
signature and your Python code.

---

## 9. Connection pools — the arithmetic that gets people fired

```
total = replicas × workers × (pool_size + max_overflow)

4 replicas × 4 workers × (10 + 5) = 240 connections
Postgres default max_connections = 100          ← database down, for everyone
```

**Do this multiplication before every deploy.** Past a few dozen processes, put
PgBouncer in front in transaction-pooling mode and shrink the per-process
numbers to near zero.

Also set: `pool_pre_ping` (survive a silently killed connection),
`pool_recycle` (beat the firewall's idle timeout), and a `pool_timeout` that is
not infinite — a request waiting forever for a connection is a worker you never
get back.

---

## 10. Query patterns worth knowing

```python
# Aggregate in the database, never in Python
Order.objects.values("status").annotate(count=Count("id"))

# Atomic relative update — no lock needed, no race window
Product.objects.filter(sku=s).update(qty=F("qty") + 1)

# Bulk operations: one round trip, not N
OrderItem.objects.bulk_create(items)
Order.objects.bulk_update(orders, ["status"])

# Stream large result sets instead of loading them into memory
for row in Order.objects.iterator(chunk_size=1000): ...

# Fetch only the columns you need
Order.objects.only("id", "status")
Order.objects.values_list("id", flat=True)

# EXISTS beats COUNT when you only need a boolean
if Order.objects.filter(customer=u).exists(): ...
```

**Delete large row sets in batches.** A multi-million-row `DELETE` takes a long
lock, bloats the WAL, and can stall replication. Loop in chunks of ~1000 with
breathing room between them.
