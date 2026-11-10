# EventFlow — a senior-level Python backend playground

A single business domain implemented as **three microservices**, each in a different
Python framework, talking to each other over **Kafka** and **RabbitMQ**, with **Redis**
and **PostgreSQL** underneath.

It is deliberately small enough to read in a weekend and deliberately complete enough
that every pattern a senior backend engineer is expected to know appears at least once,
implemented properly, with the reason written next to it.

```
                   ┌──────────────────────────────────────────────┐
                   │                 CLIENTS                      │
                   └───────┬──────────────────────┬───────────────┘
                           │ HTTP                 │ HTTP
                  ┌────────▼────────┐    ┌────────▼─────────┐
                  │  orders-django  │    │ inventory-fastapi│
                  │  Django + DRF   │    │  FastAPI + async │
                  │  (writes, saga) │    │  (reads, cache)  │
                  └───┬────────▲────┘    └────┬────────▲────┘
       order.created  │        │ inventory.*  │        │ order.*
                      │        │              │        │
                 ╔════▼════════╧══════════════▼════════╧════╗
                 ║            KAFKA  (event backbone)        ║
                 ║   topics: orders.events, inventory.events ║
                 ╚════════════════════╤══════════════════════╝
                                      │ consumes everything
                        ┌─────────────▼──────────────┐
                        │   notifications-flask      │
                        │   Flask app factory        │
                        └─────────────┬──────────────┘
                                      │ publishes work items
                        ╔═════════════▼══════════════╗
                        ║  RABBITMQ (work queues)    ║
                        ║  retry + DLQ + TTL         ║
                        ╚═════════════╤══════════════╝
                                      │
                           ┌──────────▼──────────┐
                           │  notification worker │
                           └──────────────────────┘

        Shared: PostgreSQL (one database per service) · Redis (cache, locks, rate limit)
```

## Why three frameworks in one repo

Because the job interview question is never "do you know Django", it is
**"why did you pick Django here and FastAPI there?"** This repo forces you to answer it
by making each service play to its framework's strength:

| Service | Framework | Why this framework fits |
|---|---|---|
| `orders-django` | Django + DRF | Write-heavy transactional core. Needs the ORM's transaction handling, migrations, admin, mature auth. Django's "batteries included" is a feature when the domain is CRUD + workflow. |
| `inventory-fastapi` | FastAPI | Read-heavy, high-concurrency, cache-fronted. Async I/O, Pydantic validation at the boundary, auto-generated OpenAPI. |
| `notifications-flask` | Flask | Small, mostly a consumer with a thin API. Flask's app-factory + blueprint pattern keeps it tiny with no framework ceremony. |

## What is actually inside

**Architecture patterns:** microservices with database-per-service, event-driven
choreography, saga with compensation, transactional outbox, idempotent consumers,
CQRS-lite read model, dead-letter queues, circuit breaker, distributed locking.

**Infrastructure:** Kafka (streaming backbone, consumer groups, replay), RabbitMQ (work
queues, retry with TTL, DLX), Redis (cache-aside, distributed lock, rate limiter, Celery
broker), PostgreSQL (one DB per service, migrations, indexing, `SELECT … FOR UPDATE`).

**Craft:** 12-factor config, structured JSON logging with correlation IDs, Prometheus
metrics, health/readiness probes, graceful shutdown, API versioning, pagination,
JWT auth, rate limiting, pytest with fixtures, Docker + Compose, Makefile.

## Start here

Do not read the code first. Read in this order:

1. `docs/00-PROJECT-BRIEF.md` — what the system does, in plain language.
2. `docs/01-ARCHITECTURE.md` — how the pieces fit, every event drawn out.
3. `docs/02-LEARNING-PATH.md` — **the main document.** Eight phases, junior → senior.
4. The notes (`03`–`06`) as the learning path tells you to read them.
5. `docs/07-EXERCISES.md` when the path says "now break it".

## Run it

```bash
cp .env.example .env
make up          # starts postgres, redis, kafka, rabbitmq + all three services
make migrate     # creates schemas and seeds demo data
make smoke       # places an order end-to-end and prints what happened
```

Then:

- Orders API — http://localhost:8001/api/v1/ (Django, DRF browsable API)
- Inventory API — http://localhost:8002/docs (FastAPI, Swagger UI)
- Notifications API — http://localhost:8003/health (Flask)
- RabbitMQ UI — http://localhost:15672 (guest/guest)
- Kafka UI — http://localhost:8080

```bash
make logs            # tail everything
make logs s=orders   # tail one service
make test            # run every service's test suite
make down            # stop, keep data
make clean           # stop and wipe volumes
```

## Repo layout

```
eventflow/
├── docs/                        ← read these first
├── libs/eventflow_common/       ← shared: event envelope, buses, reliability, logging
├── services/
│   ├── orders-django/           ← Django + DRF + Celery
│   ├── inventory-fastapi/       ← FastAPI + async SQLAlchemy
│   └── notifications-flask/     ← Flask + RabbitMQ worker
├── docker-compose.yml
├── Makefile
└── .env.example
```

## A promise and a warning

**The promise:** if you work through `docs/02-LEARNING-PATH.md` properly — typing the
code, doing the exercises, breaking things on purpose and fixing them — you will be able
to hold your own in a senior Python backend interview and, more importantly, on a senior
Python backend team.

**The warning:** reading this repo will not do that. Senior is not a set of facts, it is
a set of reflexes, and reflexes come from doing. Every phase ends with something to build
or break. Do those parts.
