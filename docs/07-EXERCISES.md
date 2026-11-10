# 07 — Exercises

Ordered to match the phases in `docs/02-LEARNING-PATH.md`. Difficulty:
🟢 straightforward · 🟡 requires thought · 🔴 a real piece of engineering.

**The breakage exercises are the most valuable ones here.** Reading about a
failure mode teaches you the words; causing it teaches you the reflex. Always
put the code back afterwards.

---

## Phase 1 — HTTP layer

**1.1 🟢 Order summary endpoint.** `GET /api/v1/orders/summary` returns each
status and its count for the current user. **One** SQL query
(`.values().annotate()`). If you wrote a Python loop, do it again.

**1.2 🟢 Filtering.** Add `?created_after=` and `?min_total=` to the order list
with proper validation. Confirm a bad value returns 400 with your error shape.

**1.3 🟡 Break authorisation.** Delete the `filter(customer=self.request.user)`
line from `get_queryset`. Confirm `test_a_user_cannot_read_another_users_order`
fails. Now put it back and instead move the check into `retrieve()` only — the
test passes again, but `list` leaks everything. **That is why authorisation
belongs in the queryset.**

**1.4 🟡 Idempotency edge case.** Send the same `Idempotency-Key` with a
*different* body. Currently it returns the first order, which is wrong.
Implement a body fingerprint and return 422 on mismatch.

**1.5 🔴 Partial response.** Add `?fields=id,status,total_amount` to control
which fields come back. Think about: unknown field names, nested fields, and
whether this actually belongs in your API at all.

---

## Phase 2 — Data and concurrency

**2.1 🔴 Prove the race (do this one).** Comment out `.with_for_update()` in
`reserve_stock`, rebuild, and run `pytest -m integration` against real
Postgres. It must fail. Note **what** saved you — the CHECK constraint. Restore
the lock. You now understand row locking in a way no article can give you.

**2.2 🔴 Cause a deadlock.** Change `.order_by(Product.sku)` to `.desc()` in
one code path, then fire two concurrent orders containing the same two SKUs in
opposite order. Read the Postgres log. Fix it by restoring consistent ordering.

**2.3 🟡 Find the N+1.** Remove `prefetch_related("items")` from the order
detail view. Count queries with `assertNumQueries` or the debug toolbar. Add a
test that *fails* if the N+1 returns — that is the only fix that sticks.

**2.4 🟡 Index investigation.** `make psql db=inventory_db`, then
`EXPLAIN ANALYZE` a query filtering on `name`. Drop `idx_product_name`, run it
again, compare. Write down both plans.

**2.5 🟡 Low-stock endpoint.** `GET /api/v1/products/low-stock?threshold=N`,
backed by an index, with the query plan documented in a comment.

**2.6 🔴 Optimistic locking.** Rewrite `restock` to use the `version` column
with a retry loop instead of an atomic UPDATE. Benchmark both under
contention. Explain when each wins.

---

## Phase 3 — Async

**3.1 🟢 Feel the block.** Put `time.sleep(2)` in a route, hammer it with
`ab -n 50 -c 10`. Replace with `await asyncio.sleep(2)`. Compare throughput.

**3.2 🟡 Concurrent fetch.** `GET /api/v1/products/bulk?skus=A,B,C` that
fetches each product concurrently with `asyncio.gather`. Measure against the
sequential version.

**3.3 🟡 The pool.** Set `db_pool_size=1, db_max_overflow=0`, then send 10
concurrent requests. Watch them queue and then time out. Explain what
`pool_timeout` does and why infinite would be worse.

**3.4 🔴 `to_thread`.** Add an endpoint that does something CPU-heavy
(hash a large string a million times). Measure the impact on *other* endpoints
while it runs. Fix it with `asyncio.to_thread`, then explain why that helps
less than you might hope, and what actually fixes CPU-bound work.

---

## Phase 4 — Events and the outbox ★

**4.1 🔴 Kill the broker (do this one).**
```bash
docker compose stop kafka
make smoke                    # must still return 201
make psql db=orders_db        # outbox_messages: published_at IS NULL grows
docker compose start kafka    # watch it drain by itself
```
Then remove the outbox entirely — publish directly in `create_order` — and
repeat. Watch order creation fail when Kafka is down. **That contrast is the
entire argument for the pattern.**

**4.2 🔴 Replay the topic.** Reset the `inventory-service` consumer group to
the earliest offset and let the whole history replay. Stock must not be
double-reserved. Then comment out the `already_processed` check and replay
again. Watch what happens — and notice which layer saves you.

**4.3 🟡 Watch a rebalance.** `docker compose up -d --scale
inventory-consumer=3` while events flow. Run `make lag`. Observe partition
assignment. Scale back to 1.

**4.4 🟡 Wrong partition key.** Change the Kafka key from `aggregate_id` to a
random UUID. Produce `order.created` and `order.cancelled` for the same order
rapidly. Observe out-of-order processing. Explain precisely why.

**4.5 🔴 A new event, end to end.** `order.completed`: a
`POST /orders/{id}/complete` endpoint, emitted through the outbox, consumed by
inventory, which converts the reservation from HELD to COMMITTED and decrements
`quantity_on_hand`. Full idempotency. Tests both sides.

**4.6 🔴 Schema evolution.** Add an optional `promo_code` field to
`order.created`. Deploy the *producer* change only and confirm the old
consumers still work. Then make a breaking change (rename `lines` to `items`)
and work out, in writing, the full migration plan.

---

## Phase 5 — Sagas

**5.1 🟡 Watch the timeout.** Set `RESERVATION_TTL_SECONDS=30`. Make inventory
raise on every `order.created`. Watch orders go PENDING → CANCELLED by the
sweeper, and the reservation sweeper clean up behind it.

**5.2 🔴 The nasty race.** Add `time.sleep(5)` inside `reserve_stock`. POST an
order, then immediately cancel it. Confirm the reservation is released, not
leaked. Then remove the `is_terminal` re-check after the lock and watch the
leak appear.

**5.3 🟡 Trip the breaker.** `docker compose stop inventory-api`, POST an
order. Time the first five requests (~2s each) and the sixth (microseconds).
Then set `failure_threshold=100` and feel why the breaker exists.

**5.4 🔴 Payments.** Add a payment step: `inventory.reserved` → attempt payment
→ `payment.succeeded` (CONFIRMED) or `payment.failed` (compensate: release the
reservation, cancel the order). Make the payment service fail 30% of the time.
**This is the exercise that proves you understand sagas.**

**5.5 🔴 Orchestration.** Reimplement the same saga with an explicit
orchestrator holding the state machine, instead of choreography. Write a page
on which you would choose and why.

---

## Phase 6 — Caching, Celery, RabbitMQ

**6.1 🟡 Watch the retry path.** `make smoke`, then watch
`make logs s=notifications-worker` with the RabbitMQ UI open. Follow one
message: work → retry → work → sent.

**6.2 🟡 Fill the DLQ.** Set `FAILURE_RATE = 1.0`. Confirm messages land in the
DLQ after exactly 5 attempts. Inspect them in the UI.

**6.3 🔴 DLQ replay tool.** Write a command that moves messages from the DLQ
back to the work queue, with a `--limit` and a `--dry-run`. **Every real system
needs one of these**, and writing it teaches you more about RabbitMQ than any
tutorial.

**6.4 🟡 Prove staleness.** `GET` a product, then `UPDATE` its stock directly
in psql, then `GET` again. See the stale value. Now do the same through the
API and watch delete-on-write invalidation work.

**6.5 🟡 Cause a stampede.** Set `cache_ttl_seconds=2`, hammer one product with
50 concurrent requests, and count database queries. Add the rebuild lock.
Count again.

**6.6 🔴 Distributed lock.** Add a Celery task that runs on a schedule, and
make sure only one replica executes it using `RedisLock`. Then scale to 3
replicas and prove it.

---

## Phase 7 — Production readiness

**7.1 🟡 Graceful shutdown.** `docker compose stop orders-consumer` while
events flow — confirm a clean drain in the logs. Remove the
`shutdown.requested` check and compare.

**7.2 🟡 Cardinality explosion.** Change the metrics middleware to label with
`request.path` instead of the route template. Place 100 orders. Look at
`/metrics` and count the series. Now imagine a million orders.

**7.3 🟡 Break liveness.** Make the liveness probe check Postgres. Stop
Postgres. Watch Docker restart every container in a loop. **Then explain to
yourself why that is worse than the original problem.**

**7.4 🔴 Prometheus + Grafana.** Add both to `docker-compose.yml`. Build one
dashboard: request rate, p99 latency, error rate, consumer lag, outbox depth,
DLQ depth.

**7.5 🔴 Write three alerts** in Prometheus rule syntax, each with a threshold,
a duration and a documented action. Then test each by causing the condition.

---

## Phase 8 — Testing and the capstone

**8.1 🟡 Mutation testing by hand.** Pick three tests. Break the code each
covers, one at a time. Confirm each fails for the right reason. Any test that
still passes is worse than no test.

**8.2 🟡 Test the sweeper with frozen time.** Use `freezegun` instead of the
`.update(created_at=...)` trick. Compare readability.

**8.3 🔴 Contract test.** Write a test in the inventory service that validates
`order.created` against a schema file, and one in orders that validates what it
produces against the same file. Break the producer; watch the consumer's test
fail.

**8.4 🔴🔴 The capstone — a shipping service.**

> Consumes `order.confirmed`, creates a shipment, publishes
> `shipment.dispatched` and `shipment.delivered`. Orders consumes those and
> moves to COMPLETED. Inventory consumes `shipment.dispatched` and converts the
> reservation to a real stock decrement.

Non-negotiable requirements: its own database · outbox · idempotent consumers ·
a saga timeout · health endpoints · metrics · graceful shutdown · Dockerfile ·
tests · an entry in the event catalogue.

Use whichever framework you feel **weakest** in.

---

## Open-ended, for afterwards

- **Event sourcing.** Rebuild the orders service with the event log as the
  source of truth and the order table as a projection.
- **CQRS.** Add a read model fed by events — a denormalised "order history"
  table in the notification service that needs no joins.
- **Kubernetes.** Write the manifests. Get the probes, resource limits,
  `terminationGracePeriodSeconds` and a PodDisruptionBudget right.
- **CI/CD.** A GitHub Actions pipeline: lint, unit, integration, build, scan,
  deploy. Make integration a separate job.
- **Load testing.** Use `k6` or `locust` to find the breaking point. Find out
  which resource saturates first — it is almost never the one you guessed.
- **Chaos.** Randomly kill a container every 5 minutes for an hour under load.
  Count lost orders. If the answer is not zero, you have found real work.
- **Tracing.** Add OpenTelemetry and compare the experience with correlation
  ids alone.
