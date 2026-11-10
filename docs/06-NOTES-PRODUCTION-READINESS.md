# 06 — Notes: production readiness

Reference for Phases 7 and 8. This is the material that most separates someone
who *writes* backends from someone who *runs* them.

---

## 1. The twelve factors, the ones that matter

1. **Config in the environment.** No `if ENV == "prod"` branching in code —
   that means your test environment runs different code paths than production
   and the difference surfaces at the worst moment.
2. **Explicit, pinned dependencies.** `django==5.0.6`, not `django>=5`.
3. **Backing services are attached resources.** A database swap is a config
   change, not a code change.
4. **Stateless processes.** Any replica can serve any request. No local files,
   no in-memory session state, no "sticky" anything.
5. **Logs to stdout.** The platform collects, rotates and ships them. An app
   that manages its own log files cannot be scaled horizontally without losing
   lines.
6. **Disposability.** Fast startup, graceful shutdown on SIGTERM.
7. **Dev/prod parity.** Same Postgres, same Redis, same broker — which is what
   `docker-compose.yml` is for.
8. **Admin tasks as one-off processes.** Migrations are a job, not something
   that runs at import time.

---

## 2. Observability

### Logs

**Structured, always.**
```python
log.info("order created", extra={"order_id": oid, "total": total})   # ✅
log.info(f"order {oid} created for {total}")                          # ❌
```
A string must be parsed with a regex somebody will break. A field can be
filtered, aggregated and alerted on.

**Levels, used properly:**
- `DEBUG` — development only
- `INFO` — business events worth a record: order created, event handled
- `WARNING` — recovered from something: a retry, a cache miss storm, a slow
  request
- `ERROR` — a request or a message failed; a human may need to look
- `CRITICAL` — the service cannot function; wake someone

**Never log:** passwords, tokens, API keys, full card numbers, personal data
you would not want in a third-party log index. Logs are copied to more places
than you think.

### Correlation ids

One id per user action, minted at the edge, carried in every event envelope,
attached to every log line via a ContextVar, returned in the response header so
the client can quote it.

**The cheapest observability win available.** Five minutes of work turns "the
checkout failed at about 2pm" into a single filter that shows every step across
all three services.

### Metrics

**RED**, for request-driven services: **R**ate, **E**rrors, **D**uration.
**USE**, for resources: **U**tilisation, **S**aturation, **E**rrors.
**The four golden signals** (Google SRE): latency, traffic, errors, saturation.

For this system specifically, the ones that matter:

| Metric | Watch for |
|---|---|
| consumer lag | sustained growth → you cannot keep up |
| outbox pending | non-zero and unchanging → the relay is dead |
| DLQ depth | anything > 0 → messages are being lost to humans |
| p99 latency | the average hides everything; p99 is the user's experience |
| error rate | by endpoint, not in aggregate |
| pool saturation | checked-out connections / pool size |

**The cardinality trap.** A time series is created per unique combination of
label values. Using the raw path as a label means `/orders/<uuid>` creates one
series *per order* — a million orders is a million series and a dead monitoring
stack. **Label with the route template, never with an id, user, email or
free-text value.** All three services do this; look at how.

**Percentiles, not averages.** An average latency of 100ms with a p99 of 8
seconds means 1% of your users are having a terrible time and the average is
actively lying to you.

### Tracing

OpenTelemetry gives you the waterfall across services. Correlation ids are the
poor-but-effective version and are enough to start; add tracing when "which
service is the slow one?" becomes a recurring question.

---

## 3. Health checks

| Probe | Question | On failure | Checks dependencies? |
|---|---|---|---|
| **Liveness** | is the process broken beyond recovery? | **restart the container** | **NO** |
| **Readiness** | can I serve a request right now? | remove from the load balancer, keep running | **YES** |
| **Startup** | has it finished booting? | suppress the others until it has | n/a |

**The mistake that converts an incident into an outage:** checking the database
in liveness. A 60-second database blip then makes Kubernetes kill every replica
simultaneously, and when the database recovers there is nothing left running.
You turned a blip into a full outage with your own probe.

**Decide per dependency** whether its loss means "unready" or merely
"degraded". Redis-as-a-cache → degraded. Postgres-as-source-of-truth →
unready. RabbitMQ in a service whose job is queueing → unready. Reflexively
failing readiness on every dependency you happen to connect to makes your
service less available than its dependencies, not more.

**Report which check failed.** "not ready" with no detail wastes the first ten
minutes of every incident.

---

## 4. Graceful shutdown

SIGTERM → wait (30s by default in Kubernetes) → SIGKILL.

The contract for a well-behaved process:

> On SIGTERM: stop accepting **new** work, finish the item in hand, commit
> offsets, close connections, exit 0.

Every long-running loop in this repo checks `shutdown.requested`. That one
habit is the difference between rolling deploys nobody notices and rolling
deploys that drop messages.

Two details:
- **Interruptible sleeps.** `asyncio.sleep(60)` means SIGTERM waits up to a
  minute and SIGKILL arrives first. Wait on a stop event with a timeout
  instead.
- **In async, use `loop.add_signal_handler`,** not `signal.signal` — it is
  loop-aware and wakes the loop immediately.

---

## 5. Containers

**Multi-stage builds.** Compile wheels in a builder stage; copy only the
finished wheels into a slim runtime. Smaller image, and — more importantly — no
compiler in the runtime, so an attacker with code execution cannot build
anything.

**Never run as root.** `useradd --uid 10001 appuser`. Limits the blast radius
and prevents accidental writes to system paths.

**`PYTHONUNBUFFERED=1`.** Without it, logs are buffered until the process dies,
which is exactly when you need them.

**Layer order matters.** Copy `requirements.txt` and install *before* copying
source, so a code change does not re-run the install.

**One concern per container.** Same image, different command. Look at
`docker-compose.yml`: four containers for the orders service.

**Healthcheck in the image.** Docker uses it; Kubernetes uses its own probes
pointing at the same endpoint.

---

## 6. Security checklist

**Authentication / authorisation**
- [ ] Secure by default: `IsAuthenticated` globally, opt *out* per view. The
      reverse default is one forgotten decorator away from a breach.
- [ ] Authorisation in the queryset, not in each handler — then every action,
      including ones added next year, is protected.
- [ ] 404 rather than 403 for other people's resources.
- [ ] JWT: short-lived access token, explicit `algorithms=[...]` allow-list
      (never let the token choose — `alg: none` forges anything), `exp`
      required. Accept that JWTs cannot be revoked; that is the price of
      statelessness.

**Input**
- [ ] Validate and bound everything at the boundary, query params included.
- [ ] Use the ORM's parameterisation; never f-string into SQL.
- [ ] Bound list sizes and page sizes.

**Secrets**
- [ ] Never in code or git. Environment variables locally, a secret manager in
      production.
- [ ] Rotate them. Which requires that nothing hardcodes them.
- [ ] `.env` gitignored; `.env.example` committed with fake values.

**Transport and headers**
- [ ] HTTPS everywhere; HSTS; secure, HttpOnly cookies.
- [ ] `X-Frame-Options: DENY`, `nosniff`.
- [ ] CORS: an explicit origin list, never `*` with credentials.

**Operational**
- [ ] `DEBUG = False`. Flask's debugger is a remote code execution console.
- [ ] Generic 500 responses; stack traces to logs only. Exception text leaks
      table names, SQL and occasionally credentials.
- [ ] Rate limit by user and by IP.
- [ ] Non-root containers; `/metrics` not publicly exposed.
- [ ] Dependency scanning in CI (`pip-audit`, Dependabot).

**Data**
- [ ] Encrypt at rest and in transit.
- [ ] Log no personal data.
- [ ] Have a retention policy, and enforce it with a job.

---

## 7. Testing

### The pyramid

```
      ╱ E2E ╲          few    whole stack, slow, brittle, highest confidence
    ╱────────╲
  ╱ integration╲       some   real database, real transactions
╱──────────────╲
╱     unit       ╲     many   fast, isolated, run on every save
```

An inverted pyramid gives you a 40-minute suite that fails for unrelated
reasons and that nobody trusts. **Push tests down the pyramid whenever you
can.**

### What to mock

| Mock | Do not mock |
|---|---|
| external HTTP | your database |
| brokers (in unit tests) | your own business logic |
| email/SMS providers | your framework |
| the clock (`freezegun`) | |

**Never mock the database.** That is where constraints, transactions and SQL
live — which is where the bugs are. This repo mocks Kafka and keeps Postgres.

### What to actually test

Not "does `save()` save". Test *your* rules:
- order created ⇒ outbox row created, in the same transaction
- a rollback leaves **nothing**, including no outbox row
- the same event twice ⇒ one side effect ← **if you write one test, write this**
- rejection ⇒ CANCELLED **and** a compensating event emitted
- an expired PENDING order gets swept
- an idempotency key replay ⇒ one order, 200 not 201
- a user cannot read another user's order
- **concurrent reservations cannot oversell** ← needs real Postgres

That last one is the test almost nobody writes. SQLite ignores `SELECT FOR
UPDATE`, so under SQLite it passes for the wrong reason and gives you false
confidence in code that oversells. Mark it `integration` and run it against the
real database.

### Coverage

A floor, not a target. It measures which lines *ran*, not whether they were
*verified* — 100% coverage with no assertions is meaningless. Use it to find
whole modules nobody tested, then read the report and ignore the number.

### Contract testing

In microservices, unit tests on both sides can pass while the two disagree
about the message format. Pact (or a shared schema registry) verifies that the
producer's output satisfies the consumer's expectations in CI. Worth adding
when you have more than one team.

---

## 8. Deployment

**Rolling deploys** are the default: replace replicas gradually, old and new
running simultaneously for a few minutes. Which means:

> **Every change must be backward compatible for one release.**

The **expand/contract** pattern for a column rename:
1. add the new column, write to **both**
2. backfill, switch reads to the new column
3. stop writing the old one
4. drop it

Four releases to rename a column. Slow, boring, and the reason your deploy does
not cause an outage.

Migrations run **before** the new code, as a separate job — which is why the
migration must work with the *old* code too.

**Feature flags** decouple deploy from release: ship the code dark, turn it on
for 1% of users, watch, ramp. The alternative is that "deploy" and "release"
are the same terrifying event.

**Know how to roll back.** If the answer involves a database restore, you do
not have a rollback, you have a hope.

---

## 9. What to alert on

**Alert on symptoms users feel, not on causes.** "CPU is at 80%" is not a
problem; "p99 latency is 4 seconds" is.

A good alert has: a threshold, a duration, and **an action**. An alert with no
action is noise, and noise is how real alerts get ignored.

| Alert | Threshold | Action |
|---|---|---|
| Consumer lag growing | rising for 10 min | scale consumers, or find the slow handler |
| Outbox pending stuck | > 0 and flat for 5 min | the relay is dead — restart it, check Kafka |
| DLQ not empty | > 0 for 15 min | inspect the messages; fix and replay |
| Error rate | > 1% for 5 min | check the last deploy first |
| p99 latency | > 2s for 10 min | check the database and the pool |
| Pool saturation | > 90% for 5 min | find the long transaction |

**Alert on the derivative, not the value.** An outbox depth of 500 is fine if
it is zero two seconds later. A depth of 50 that has not moved in five minutes
means events are silently not leaving your service.

---

## 10. The incident runbook

1. **Stop the bleeding.** Roll back or disable the feature flag first.
   Understanding comes second; it is not more important than the users.
2. **Find the correlation id** from the user's report or the error response.
3. **Follow it across services.** This is why you built it.
4. **Check the obvious, in order:** the last deploy, the queues (lag, DLQ,
   outbox), the database (connections, long transactions, locks), the
   dependencies (are they up? are your breakers open?).
5. **Write the postmortem,** blameless, with an action item that has an owner
   and a date.

The questions worth asking afterwards: how long until we *knew*? How long until
we *knew why*? What would have made each shorter? Those two numbers are the
real measure of your observability, and improving them is worth more than any
dashboard.
