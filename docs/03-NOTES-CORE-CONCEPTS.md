# 03 — Notes: core backend concepts

Reference notes for Phases 1 and 3. Each section is the explanation you would
want someone to give you once, properly.

---

## 1. Layering: why the service layer exists

```
HTTP request
    │
    ▼
 VIEW        auth, parse, validate, call ONE service fn, serialise, status code
    │
    ▼
 SERVICE     business rules, transaction boundaries, event emission   ← the app
    │
    ▼
 MODEL       data shape, constraints, trivial derived properties
    │
    ▼
 DATABASE
```

The test: **can the same operation be triggered by HTTP, a Celery task, a CLI
command and an event consumer, without duplication?** In this repo
`create_order` is called by the API; `cancel_order` is called by the API *and*
by the Celery sweeper. That is only possible because neither lives in a view.

Common failure modes:
- Logic in views → unreachable from anywhere else.
- Logic in models → cannot coordinate several models plus an outbox write
  without the model importing infrastructure.
- "Fat models" works until two aggregates must change together. Then you need a
  service.

A service function should read like the business process it implements. If you
cannot read `create_order` aloud to a product manager, it is in the wrong shape.

---

## 2. Validate at the boundary

Untrusted input becomes trusted data **exactly once**, at the edge, completely.
Everything inside may then assume well-formedness.

Bound everything that crosses:
- strings → max length, and a regex where the format is known
- numbers → `min` *and* `max` (an unbounded int is a DoS or an overflow)
- lists → `max_length` (100 SKUs, not 100,000)
- enums → an explicit allow-list, never free text
- query params too — `?limit=99999999` is user input like any other

Separate In and Out schemas. One schema for both leads to `read_only=True`
scattered everywhere and eventually to mass assignment: a client sets
`is_staff` or `total_amount` because the field was in the write schema by
accident.

Never `fields = "__all__"`. Adding an internal column to the model then
silently publishes it.

---

## 3. REST API design that will not embarrass you

**Version from day one.** `/api/v1/`. Adding versioning later means every
existing client is on an unversioned path you can never change.

**Status codes carry meaning:**

| Code | Means |
|---|---|
| 200 | here it is (including an idempotent replay — nothing was created) |
| 201 | created, and the body is the new resource |
| 202 | accepted, work continues asynchronously |
| 204 | done, nothing to say |
| 400 | malformed request |
| 401 | who are you? |
| 403 | I know who you are; no |
| 404 | not found — also the right answer for "exists but is not yours" |
| 409 | well-formed, but the resource is in the wrong state |
| 422 | well-formed, semantically invalid |
| 429 | slow down; include `Retry-After` |
| 503 | I am unwell; include `Retry-After` |

**404 over 403 for other people's resources.** 403 confirms the resource
exists, which leaks information to anyone enumerating ids.

**State transitions are sub-resources, not fields.** `POST /orders/{id}/cancel`,
not `PATCH {"status": "CANCELLED"}`. Cancelling has rules and side effects; it
is not an assignment, and exposing it as one invites clients to invent
transitions you never intended.

**One error shape across every service**, with a machine-readable code, a
human-readable detail, and the correlation id. Clients branch on the code, so
you stay free to reword the message.

**Be honest about async.** If the work is not done, do not return as though it
is. `201 {status: PENDING}` is honest; `201 {status: CONFIRMED}` is a lie that
will come true most of the time, which is the worst kind.

---

## 4. Idempotency keys

The problem is unavoidable: a client sends a request, the network drops the
*response*, the client retries. The server cannot distinguish "the same order
again" from "a second order". **Only the client knows**, so only the client can
tell you — with a key it generates once and reuses on every retry.

```
POST /orders
Idempotency-Key: 7f3e...

first time:   do the work, store key -> result, return 201
replay:       look up the key, return the stored result, 200
```

Two layers, both needed:
- **Redis** — fast, catches the common case, TTL-bounded.
- **A UNIQUE constraint** — the actual guarantee. It wins the race between two
  simultaneous retries, and it survives a cache flush.

Edge case worth handling: the same key with a *different* body is a client bug.
Fingerprint the body, compare, and return 422 rather than silently answering
the wrong question.

This is how Stripe works, and it is the single most useful API pattern to know.

---

## 5. Async Python: what the event loop actually does

A single thread with a queue of tasks. When a task hits an `await` on I/O, it
yields control and the loop runs something else. When the I/O completes, the
task resumes.

- **Concurrency** — many things in flight, one at a time, interleaved. Async.
- **Parallelism** — many things literally at once. Needs processes (the GIL
  means threads do not give you CPU parallelism in Python).

So: **async is for I/O-bound work.** Waiting on a database, an HTTP call, a
broker. For CPU-bound work it does nothing, and `ProcessPoolExecutor` is the
answer.

**The rule that matters:**

> One blocking call stalls every coroutine on the loop.

Not "slows down" — *stalls*. `time.sleep(1)`, `requests.get(...)`, a psycopg2
query, a CPU-heavy loop: all of them freeze the entire service, including the
Kafka heartbeat that tells the broker you are alive. The symptom is a consumer
mysteriously kicked out of its group.

```python
await asyncio.sleep(1)                      # yields
result = await client.get(url)              # yields
rows = await session.execute(query)         # yields
data = await asyncio.to_thread(blocking_fn) # escape hatch for blocking code
```

Run things concurrently with `asyncio.gather`:
```python
product, price, stock = await asyncio.gather(
    fetch_product(sku), fetch_price(sku), fetch_stock(sku)
)   # ~max(a,b,c) instead of a+b+c
```

**ContextVar, not threading.local.** A ContextVar is correct under both threads
and asyncio tasks. `threading.local` in an async service means two concurrent
requests on the same thread read each other's values — a data leak that looks
like a race condition.

---

## 6. WSGI vs ASGI, and choosing a framework

| | WSGI (Django, Flask) | ASGI (FastAPI, Django async) |
|---|---|---|
| Model | one request per worker at a time | thousands of concurrent connections per worker |
| Scale by | more workers/threads | more concurrency, then more workers |
| Good at | transactional, DB-bound work | high-concurrency I/O, websockets, SSE, fan-out |
| Server | gunicorn | uvicorn / hypercorn |

Worker count for WSGI, I/O-bound: `(2 × cores) + 1`. Remember each worker holds
its own DB connections — multiply before you deploy.

**Choosing:**

- **Django** when the domain is transactional CRUD plus workflow, when you want
  the ORM's migrations and transaction handling, when an ops team needs a back
  office (the admin is a genuine competitive advantage and FastAPI has no
  equivalent), and when the team values convention over assembly.
- **FastAPI** when the service is I/O-bound and high-concurrency, when the API
  contract matters enough that auto-generated OpenAPI pays for itself, when you
  want Pydantic validation, and when you are happy to assemble auth, admin and
  migrations yourself.
- **Flask** when the service is genuinely small — a consumer with a thin API, a
  webhook receiver, an internal tool. Its minimal core is a strength at that
  size and a liability at scale, where you will rebuild a worse Django.

The wrong reason to pick FastAPI: "it is faster". For a service that spends
95% of its time waiting on Postgres, framework overhead is noise. Pick on
architecture and team, not benchmarks.

---

## 7. Dependency injection (FastAPI) vs middleware (Django/Flask)

Both solve "this must happen on every request". They differ in granularity:

- **Middleware** wraps *everything*. Right for correlation ids and metrics.
- **Dependencies** are *per route*. Right for auth, pagination, rate limits —
  anything that varies by endpoint, and anything you want to override in tests.

`app.dependency_overrides[get_session] = test_session` is FastAPI's best
testing feature: no monkeypatching, no import gymnastics.

Flask's `g` is per-request scratch space — thread-local and absent in
background threads, which is a trap worth remembering.

---

## 8. Python things that show up in review

**Type hints.** Not for the runtime; for the reader and for mypy.
`def reserve(session: AsyncSession, *, order_id: str) -> Reservation | None`
documents the contract better than a docstring and is checkable.

**Keyword-only arguments** (`*` in the signature). `create_order(customer=...,
lines=...)` cannot be called with the arguments swapped. Use it for anything
with more than two parameters.

**Dataclasses with `frozen=True`** for values that must not change — events,
money, configuration.

**Context managers for resources.** Anything acquired must be released on the
exception path too. `with`, `async with`, `try/finally`.

**Never mutable default arguments.** `def f(items=[])` shares one list across
every call. Use `None` and build inside.

**`__slots__`** when you have millions of small objects and memory matters.
Otherwise it is noise.

**Generators and `.iterator()`** for large result sets. `Model.objects.all()`
on a 10-million-row table is an OOM kill with a stack trace.

**Never bare `except:`.** It catches `KeyboardInterrupt` and `SystemExit`, so
your process becomes unkillable. `except Exception:` at minimum; name the
exception where you can.

**Raise from.** `raise DomainError(...) from exc` keeps the original traceback.
`from None` deliberately hides it — use that only when the inner exception
would leak internals to a caller.
