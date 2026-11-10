# 08 — The senior checklist

Two uses: a self-assessment before you call yourself senior, and a revision
sheet before an interview.

**How to use it honestly:** for each item, answer out loud, from memory, as if
to another engineer. "I have read about it" is a no. "I can explain it with the
trade-off" is a yes. Anything you cannot answer maps to a phase in
`docs/02-LEARNING-PATH.md`.

---

## Python

- [ ] Explain the GIL and what it does and does not prevent.
- [ ] Choose correctly between threads, processes and async for a given task.
- [ ] Explain what `await` actually does to the event loop.
- [ ] Name three ways one line of code can stall an entire async service.
- [ ] Explain why `ContextVar` and not `threading.local` in async code.
- [ ] Explain mutable default arguments, and why `except:` bare is dangerous.
- [ ] Use generators to process data that does not fit in memory.
- [ ] Explain `__enter__`/`__exit__` and why resource cleanup belongs there.
- [ ] Read a traceback and know what `raise ... from` changes.

## API design

- [ ] Choose the right status code without looking it up.
- [ ] Explain why "exists but is not yours" is 404, not 403.
- [ ] Design an idempotent POST endpoint, and explain why only the client can
      supply the key.
- [ ] Explain why cursor pagination beats offset, and when offset is fine.
- [ ] Version an API, and migrate clients off v1.
- [ ] Design a consistent error envelope and say why the code matters more
      than the message.
- [ ] Explain why a 201 with status PENDING is more honest than waiting.

## Databases

- [ ] Explain ACID and name the isolation level your database defaults to.
- [ ] Explain the difference between pessimistic and optimistic locking and
      pick one for a given workload.
- [ ] Explain how a deadlock happens and give the general fix.
- [ ] Explain `SKIP LOCKED` and build a job queue with it.
- [ ] Read an `EXPLAIN ANALYZE` and say whether an index is being used.
- [ ] Explain why composite index column order matters.
- [ ] Explain a partial index and give a case where it is dramatically better.
- [ ] Spot an N+1 in code review and fix it in both Django and SQLAlchemy.
- [ ] Do the connection-pool arithmetic before deploying.
- [ ] Write a zero-downtime column rename (expand/contract).
- [ ] Explain why money is never a float.

## Event-driven ★

- [ ] **Explain the dual-write problem and why swapping the order does not fix
      it.**
- [ ] **Describe the transactional outbox and name two alternatives with
      trade-offs.**
- [ ] Explain at-least-once vs at-most-once, and why exactly-once end-to-end is
      not a thing.
- [ ] **Make a consumer idempotent three different ways.**
- [ ] Explain why the dedup record must be in the same transaction as the side
      effect.
- [ ] Explain Kafka partition keys and what ordering guarantee you get.
- [ ] Explain consumer groups, and how one topic feeds two services
      independently.
- [ ] Explain what a rebalance is and what causes one.
- [ ] Explain why `enable_auto_commit=False`.
- [ ] Explain consumer lag and what you do when it grows.
- [ ] **Say when you would choose Kafka and when RabbitMQ, with the
      data-structure reason.**
- [ ] Build a delayed retry in RabbitMQ with TTL + DLX.
- [ ] Explain prefetch/QoS and what happens at the default.
- [ ] **Explain a saga, why there is no rollback, and what a compensating
      action is.**
- [ ] Explain choreography vs orchestration and when each wins.
- [ ] Explain why every saga needs a timeout — on both sides.
- [ ] State the rules of event schema evolution and handle a breaking change.
- [ ] Argue fat events vs thin events.

## Caching

- [ ] Describe cache-aside and name the alternatives.
- [ ] Give three invalidation strategies and say why delete-on-write is safer
      than update-on-write.
- [ ] **Say what must never be cached, and why.**
- [ ] Explain stampede, penetration and avalanche, with a fix for each.
- [ ] Explain why cache failures must fail open.
- [ ] Explain why a Redis lock is not sufficient for money or stock.
- [ ] Explain why TTL jitter matters.

## Reliability

- [ ] Explain exponential backoff and why jitter is not optional.
- [ ] Explain why retry is only safe when the operation is idempotent.
- [ ] Describe a circuit breaker's three states and what each protects.
- [ ] Explain cascading failure and how fail-fast prevents it.
- [ ] Explain the difference between a timeout, a retry and a breaker, and
      when you need all three.
- [ ] Design graceful degradation for a dependency outage.
- [ ] Explain backpressure, and why rate limiting is a form of it.

## Operations

- [ ] **Explain why liveness must not check the database.**
- [ ] Explain readiness vs liveness vs startup probes.
- [ ] Explain the SIGTERM contract and implement a clean drain.
- [ ] **Explain metric cardinality and why an order id must never be a label.**
- [ ] Name the RED metrics and the four golden signals.
- [ ] Explain why p99 matters and averages lie.
- [ ] Write an alert that has a threshold, a duration and an action.
- [ ] Explain why you alert on the derivative of outbox depth, not its value.
- [ ] Write a multi-stage Dockerfile and say why no compiler in the runtime.
- [ ] Explain why containers do not run as root.
- [ ] Debug a production incident using correlation ids.

## Testing

- [ ] Explain the test pyramid and what an inverted one costs you.
- [ ] Say what you mock and what you never mock, with reasons.
- [ ] Write a test that proves idempotency.
- [ ] Explain why a concurrency test cannot run on SQLite.
- [ ] Explain what coverage does and does not measure.
- [ ] Explain contract testing and the problem it solves.

## Architecture judgement

- [ ] **Argue AGAINST microservices for a given project, convincingly.**
- [ ] Explain what independent deployability actually buys, and its cost.
- [ ] Explain why a shared database between services is the anti-pattern.
- [ ] Decide where a service boundary goes and defend it.
- [ ] Explain eventual consistency to a non-engineer.
- [ ] Explain when a synchronous cross-service call is acceptable.
- [ ] Recognise a distributed monolith.
- [ ] **Say when a pattern in this repo is over-engineering for the problem in
      front of you.**

---

## The ten questions, if you only have an hour

1. You save an order and publish an event. Describe everything that can go
   wrong and how you fix it.
2. Your consumer processed the same message twice. Whose fault is it, and what
   do you change?
3. Kafka or RabbitMQ for this, and why?
4. Two customers buy the last item simultaneously. Walk me through your code.
5. A saga's third step fails. What happens to the first two?
6. Why is the p99 fine but users complaining?
7. Your liveness probe checks the database. What happens during a database
   blip?
8. How would you rename a column with zero downtime?
9. When would you *not* use microservices?
10. What is the most over-engineered thing in your current codebase?

Question 10 is the real one. An engineer who can only add complexity is not
senior. Knowing which patterns to *leave out* — and being able to say "the
outbox is overkill here, and here is why" — is the judgement the title is
actually for.

---

## What this checklist cannot give you

Everything above is knowledge, and knowledge is necessary. It is not
sufficient.

Senior is also: noticing when a requirement is wrong before you build it,
telling a product manager a deadline is unrealistic in a way that gets heard,
leaving a codebase better for the person after you, mentoring without
condescending, and being right about a design often enough that people listen —
while still changing your mind when the evidence says to.

None of that is testable in a checklist, and all of it comes from shipping
things, breaking things, and sitting with the consequences. Work through the
repo, then go and do that.
