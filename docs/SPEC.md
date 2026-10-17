# TicketRush - Project Spec

The original brief from the project owner. This is the source of truth for scope.
Approved deviations are listed at the bottom and detailed in [DESIGN.md](DESIGN.md).

## Purpose

A flash-sale ticketing platform that simulates a real distributed system. It is a portfolio
project that proves distributed-systems skills (Kafka, Kubernetes, failure handling), not a
CRUD demo. The owner is a junior developer: explain key design decisions briefly as you go.

## Goal

Sell a limited number of tickets to thousands of simultaneous buyers without overselling,
double-charging, or losing orders when services crash.

**Central invariant:** never sell more tickets than exist, and never create or charge the same
order twice.

## Stack

Non-negotiable unless there is a better reason, stated and approved.

- Java 21, Spring Boot, Spring Kafka, Maven multi-module (one module per service + a shared
  `common` module for event schemas)
- Apache Kafka (KRaft mode) via the Strimzi operator on Kubernetes (kind for local)
- PostgreSQL for orders and the outbox, Redis for fast seat counters
- Angular (latest stable) for a small dashboard, built LAST and kept minimal
- Testcontainers for integration tests, JUnit 5, AssertJ
- Observability: Micrometer + Prometheus + Grafana, OpenTelemetry traces
- Autoscaling: KEDA on Kafka consumer lag
- Load testing: k6. Chaos: Chaos Mesh or plain kubectl pod-kill scripts
- Delivery: Helm or Kustomize, Dockerfiles (non-root, multi-stage), GitHub Actions CI

Use the latest stable version of everything and record the chosen versions in DESIGN.md.

## Architecture

```
client -> api-gateway -> [orders.requested] -> inventory-service -> [tickets.reserved]
       -> payment-service (calls a fake, failure-injectable payment provider) -> [payments.result]
       -> order-service -> [orders.confirmed] -> notification-service
```

A reaper job releases reservations on payment failure or timeout via `tickets.released`.
This is a saga with compensating events. Failed messages go to a dead-letter topic.

### Key design rules

- Partition by event id so one event's inventory has a single writer and ordered processing.
- Clients send an idempotency key; retries never create duplicate orders.
- Transactional outbox pattern: the DB write and the event are atomic.
- Consumers are idempotent (at-least-once delivery, effectively-once results).
- Commit offsets only after successful processing; handle SIGTERM gracefully.
- Under overload the gateway returns 429 with a Retry-After instead of queueing forever.

## How to work

Work in milestones. After each milestone: run its tests, summarize what exists, list what is
NOT done or not verified, and STOP and wait for the owner's go-ahead. Do not start the next
milestone. Write the failing test first for any logic involving money, inventory, or
idempotency. Keep it simple: no speculative abstractions, no extra services beyond the
architecture above. Do not add dependencies without saying why.

### Milestone 1: Happy path locally
Docker Compose with Kafka, Postgres, Redis. Services: gateway, inventory, payment, order,
notification. One successful purchase works end to end. Shared event records in `common`.
**Acceptance:** an integration test (Testcontainers) buys a ticket and sees `orders.confirmed`.

### Milestone 2: Correctness under contention
Add the oversell test: 10,000 concurrent buyers, 500 tickets, assert exactly 500 sold.
Fix every race the test exposes. Add idempotency keys and a duplicate-message replay test.
**Acceptance:** both tests pass repeatedly (run them 10 times to check for flakiness).

### Milestone 3: Kubernetes
Deploy to kind with Strimzi. Helm or Kustomize manifests, readiness/liveness probes,
resource requests/limits, graceful shutdown.
**Acceptance:** one command brings up the system and the happy path works in the cluster.

### Milestone 4: Failure handling
Implement the transactional outbox, saga timeout + compensation, retry with backoff, and the
dead-letter topic. Make the fake payment provider fail on demand (configurable rate).
**Acceptance:** tests for payment failure releasing tickets, poison message landing in the DLQ
without blocking the partition, and no duplicate charge after a retry.

### Milestone 5: Observability and autoscaling
Metrics for consumer lag, processing time, orders/sec, saga outcomes; a Grafana dashboard;
KEDA scaling the inventory/payment consumers on lag.
**Acceptance:** the owner can watch pods scale up during a k6 load test and back down after.

### Milestone 6: Chaos experiments and results
Scripts that: kill the inventory pod mid-sale, restart a broker, make payments fail 30%,
add latency to Postgres, and ramp consumers from 1 to 8 pods. For each, assert the invariant
(no oversell, no lost order, no duplicate order) and record recovery time and throughput in a
results table in the README, measured not guessed. Be honest about bottlenecks found.

### Milestone 7: Minimal Angular dashboard
Only after everything above works. A single page: live ticket stock, orders per second,
a "buy" button, and the saga state of the last orders (via SSE or polling). No auth, no
routing complexity. Keep it small; the backend is the point.

### Milestone 8: Portfolio polish
README with architecture diagram, a how-to-run section that works from a clean clone, the
chaos results table, a "what I would do differently" section, and a demo GIF script.

## Quality bar

- Every milestone's acceptance criteria are verified by running a command, not by reading code.
- Unit tests for domain logic, Testcontainers integration tests for Kafka/DB paths.
- No secrets in the repo; config via env vars; `.env.example` provided.
- Conventional commits, small commits, one branch per milestone.
- If a requirement is ambiguous or a choice is hard to reverse (topic names, partition counts,
  event schema), ask the owner before building it.

## Approved deviations from this brief

| Date | Change | Why |
|---|---|---|
| 2026-10-16 | Spring Boot 4.1.x instead of 3.x | The 3.5 line is out of open-source support. |
| 2026-10-16 | Extra topic `orders.rejected` | Sold-out orders need a path to order-service that does not pass through payment. |
| 2026-10-16 | `fake-payment-provider` runs as its own container | A real HTTP hop is needed to test timeouts and the "charged but no reply" double-charge case. |
