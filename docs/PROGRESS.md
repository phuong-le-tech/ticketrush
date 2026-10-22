# TicketRush - Progress

Update this file at the end of every milestone: status, the exact verification command and its
result, and what is NOT done or not verified.

| # | Milestone | Branch | Status |
|---|---|---|---|
| 1 | Happy path locally | `feat/t_4d7a08e4-m1-finish` | Done (pending owner go-ahead) |
| 2 | Correctness under contention | `milestone-2` | Not started |
| 3 | Kubernetes | `milestone-3` | Not started |
| 4 | Failure handling | `milestone-4` | Not started |
| 5 | Observability and autoscaling | `milestone-5` | Not started |
| 6 | Chaos experiments and results | `milestone-6` | Not started |
| 7 | Minimal Angular dashboard | `milestone-7` | Not started |
| 8 | Portfolio polish | `milestone-8` | Not started |

## Milestone 1 checklist (done)

Checklist (see [DESIGN.md](DESIGN.md) for the approved layout, schemas and topics):

- [x] Parent `pom.xml` (Boot 4.1.1 BOM, Java 21) and all modules from DESIGN.md section 2
- [x] `common`: event records, topic name constants, JSON type mappings
- [x] `deploy/compose/docker-compose.yml`: Kafka (KRaft), Postgres, Redis, topic creation (12 partitions)
- [x] `.env.example` with every config variable
- [x] api-gateway: `POST /orders` -> `orders.requested`
- [x] inventory-service: Redis Lua reserve (all-or-nothing) -> `tickets.reserved` or `orders.rejected`
- [x] fake-payment-provider: charge endpoint, records charges by idempotency key
- [x] payment-service: `tickets.reserved` -> provider -> `payments.result`
- [x] order-service: `PENDING` on `orders.requested`, final state on `payments.result` / `orders.rejected`, emits `orders.confirmed`
- [x] notification-service: logs `orders.confirmed`
- [x] Unit tests for domain logic (written first for money, inventory, idempotency)
- [x] `e2e-tests`: buys one ticket, asserts `OrderConfirmed` on `orders.confirmed` within 30s

**Acceptance command:** `mvn verify -pl e2e-tests -am`
Also: `docker compose -f deploy/compose/docker-compose.yml up` + one `curl` reaches `orders.confirmed`.

## Milestone reports

<!-- Append one section per finished milestone:
### Milestone N - <date>
- Verified with: `<command>` -> <result>
- What exists:
- NOT done / not verified:
- Bottlenecks or surprises:
-->

### Milestone 1 - 2026-10-22
- Verified with: `mvn verify` (repo root, all modules) -> BUILD SUCCESS. Includes OrderSagaIT (10 tests), OrderSchemaIT (3), api-gateway OrderValidationTest (6) / KafkaUnavailableTest (1) / OrderPublishIT (2), and PurchaseFlowE2E (1).
- Verified with: `mvn verify -pl e2e-tests -am` -> BUILD SUCCESS, `PurchaseFlowE2E` 1 test, 0 failures: one POST /orders produces an OrderConfirmed with the matching orderId on orders.confirmed (Awaitility, 30s limit, observed about 10s total including container start).
- Verified with: `docker compose -f deploy/compose/docker-compose.yml up -d --build --wait`, then
  `curl -i -X POST localhost:8080/orders -H 'Content-Type: application/json' -H "Idempotency-Key: $(uuidgen)" -d '{"eventId":"concert-1","customerId":"cust-compose","quantity":1}'`
  -> `HTTP/1.1 202`, body `{"orderId":"91ea3b09-a631-4586-ab2c-e9be75f53ad6"}`. Reading orders.confirmed with kafka-console-consumer:
  `__TypeId__:OrderConfirmed  91ea3b09-a631-4586-ab2c-e9be75f53ad6  {"messageId":"a0ff97bc-...","orderId":"91ea3b09-...","eventId":"concert-1","customerId":"cust-compose","quantity":1,"amountCents":5000,"currency":"EUR","occurredAt":"2026-10-22T17:03:25.267275169Z"}`.
  notification-service logged `NOTIFY orderId=91ea3b09-... customerId=cust-compose quantity=1 amount=5000 EUR`.
- What exists: all modules from DESIGN.md section 2; order-service saga (one conditional upsert per message, OrderConfirmed published only when the transition happens, so replays never confirm twice; terminal states are never overwritten by a late PENDING); V1__orders.sql; api-gateway Dockerfile and executable jar; the six app services in the compose stack; the in-process e2e.
- NOT done / not verified:
  - No outbox: a crash between the order DB write and the OrderConfirmed send loses the event (planned for M4).
  - Idempotency-Key dedupe at the gateway and the contention/replay x10 runs are M2 and were not run.
  - Compose was run once on a developer machine; the stack was not tested under load, nor restarted mid-flow.
  - The app containers have no healthcheck (images have no curl); `--wait` only proves they started.
- Bottlenecks or surprises: JacksonJsonDeserializer strips the `__TypeId__` header by default, so test consumers must call `dontRemoveTypeHeaders()` to assert on it (test fix commit in this PR).
