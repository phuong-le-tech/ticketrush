# TicketRush - Progress

Update this file at the end of every milestone: status, the exact verification command and its
result, and what is NOT done or not verified.

| # | Milestone | Branch | Status |
|---|---|---|---|
| 1 | Happy path locally | `feat/t_4d7a08e4-m1-finish` | Done (pending owner go-ahead) |
| 2 | Correctness under contention | `milestone-2` | Done (pending owner go-ahead) |
| 3 | Kubernetes | `milestone-3` | Not started |
| 4 | Failure handling | `milestone-4` | Review rework implemented; pending reviewer verdict |
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

### Milestone 2 - 2026-10-22
- Verified with: `scripts/m2-acceptance.sh` (builds once with `mvn install -DskipTests`, then runs
  `OversellE2ETest` + `ReplayE2ETest` in 10 separate Maven runs, fresh JVM and containers each) -> 10/10:
  ```
  Building once (mvn install -DskipTests)...
  run 1/10: PASS in 135s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/m2-acceptance/run-1.log)
  run 2/10: PASS in 128s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/m2-acceptance/run-2.log)
  run 3/10: PASS in 132s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/m2-acceptance/run-3.log)
  run 4/10: PASS in 137s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/m2-acceptance/run-4.log)
  run 5/10: PASS in 144s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/m2-acceptance/run-5.log)
  run 6/10: PASS in 129s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/m2-acceptance/run-6.log)
  run 7/10: PASS in 124s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/m2-acceptance/run-7.log)
  run 8/10: PASS in 129s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/m2-acceptance/run-8.log)
  run 9/10: PASS in 128s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/m2-acceptance/run-9.log)
  run 10/10: PASS in 127s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/m2-acceptance/run-10.log)
  RESULT: 10/10 runs passed
  ```
  Per run, surefire time (includes booting containers and the 6 services): OversellE2ETest
  104.8 / 97.5 / 99.5 / 104.4 / 111.7 / 98.1 / 95.5 / 98.6 / 98.4 / 97.5 s; ReplayE2ETest (2 tests)
  28.3 / 27.2 / 29.6 / 29.2 / 28.1 / 27.9 / 25.4 / 27.2 / 26.9 / 27.1 s.
- Verified with: `mvn verify` (repo root, all modules) -> BUILD SUCCESS, 80 tests, 0 failures
  (OversellE2ETest 102.8 s, ReplayE2ETest 29.2 s, PurchaseFlowE2ETest 10.1 s in that run).
- What exists:
  - Gateway: `orderId` = UUIDv5 of (customerId, eventId, Idempotency-Key), see DESIGN.md section 5.
    Same key, customer and event -> same orderId; same key on another event -> another orderId; key
    over 255 chars or blank -> 400.
  - Inventory: `reserve.lua` dedupes by `inventory:order:{orderId}` in the same atomic script as the
    stock check, stores the first outcome (TTL `INVENTORY_DEDUP_TTL_SECONDS`, 7 days) and the
    listener re-publishes it on a replay. An unknown-event rejection is stored too (`reserved=0`).
  - Payment and order saga unchanged: their M1 dedupe (provider key = orderId, conditional upserts)
    holds under the M2 tests.
  - Tests: OversellE2ETest (10,000 buyers, 1,000 concurrent duplicate POSTs, stock 500 -> exactly
    500 CONFIRMED, 9,500 REJECTED, 0 PENDING, stock 0, 500 distinct OrderConfirmed, 1 charge each,
    settled within 120 s of the first request), ReplayE2ETest (3 byte-identical replays of
    OrderRequested / TicketsReserved / PaymentResult change nothing; re-POST after confirmation
    returns the same orderId), OrderIdempotencyTest, ReservationFlowIT dedupe cases.
- NOT done / not verified:
  - A replay older than the 7 day dedupe TTL would reserve again.
  - Redis durability is not configured (no AOF, no volume in Compose): losing Redis data loses stock
    and dedupe state alike.
  - Compose stack was not re-run for M2; the 10 runs are in-process e2e on one developer laptop.
  - Still no outbox (M4); notification-service does not dedupe (log only, out of scope).
- Bottlenecks or surprises: under the 11,000 request burst Tomcat logs a few
  `NioEndpoint: Error setting socket options`; the test client treats connection failures like a
  503 and retries with the same key, and every run still reached 202 for every buyer.

### Milestone 2 blockers closed - 2026-10-24
- Closed the two PR #13 review blockers (approved option a, see DESIGN.md decision log):
  - Same Idempotency-Key reused on another event: eventId is now in the orderId name, so it is a
    different order. Before, one orderId sat on two `orders.requested` partitions (key = eventId)
    and could be reserved on both.
  - Unknown event: the rejection is now stored on `inventory:order:{orderId}` (`reserved=0`, dedupe
    TTL), so that orderId stays rejected after the event is seeded. No event key is created.
- Tests: OrderIdempotencyTest (AC1-AC4: exact name format, different event, different quantity,
  customer/event boundary), InventoryRepositoryIT (stored rejection, TTL, rejected after seeding,
  replay), ReservationFlowIT `unknownThenSeededEventAC6BothRejectedStockUnchanged` (Kafka path).
- Verified with: `mvn verify` (repo root) -> BUILD SUCCESS, 86 tests, 0 failures (OversellE2ETest
  98.2 s, ReplayE2ETest 27.5 s, PurchaseFlowE2ETest 9.6 s).
- Verified with: `scripts/m2-acceptance.sh` -> 10/10:
  ```
  Building once (mvn install -DskipTests)...
  run 1/10: PASS in 139s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/m2-acceptance/run-1.log)
  run 2/10: PASS in 129s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/m2-acceptance/run-2.log)
  run 3/10: PASS in 127s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/m2-acceptance/run-3.log)
  run 4/10: PASS in 129s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/m2-acceptance/run-4.log)
  run 5/10: PASS in 133s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/m2-acceptance/run-5.log)
  run 6/10: PASS in 133s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/m2-acceptance/run-6.log)
  run 7/10: PASS in 133s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/m2-acceptance/run-7.log)
  run 8/10: PASS in 128s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/m2-acceptance/run-8.log)
  run 9/10: PASS in 128s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/m2-acceptance/run-9.log)
  run 10/10: PASS in 133s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/m2-acceptance/run-10.log)
  RESULT: 10/10 runs passed
  ```
  Per run, surefire time: OversellE2ETest 108.7 / 98.5 / 96.5 / 95.9 / 100.3 / 100.7 / 101.5 / 98.2 /
  96.3 / 101.5 s; ReplayE2ETest (2 tests) 28.1 / 27.4 / 27.2 / 29.5 / 29.2 / 29.3 / 28.0 / 27.2 /
  28.2 / 28.2 s.
- NOT done / not verified: in-flight retries across the deploy get a new orderId (name changed; no
  production data). SonarCloud gate is checked on the PR, not locally. The other M2 items above
  still stand (TTL, Redis durability, Compose not re-run, no outbox).

### Milestone 3 - 2026-10-25
- What exists: Helm chart `deploy/helm/ticketrush` (Strimzi Kafka KRaft with 3 brokers, 6 KafkaTopic CRs
  with 12 partitions and RF 3, Postgres and Redis StatefulSets, 6 service Deployments with startup,
  readiness and liveness probes, requests and limits, non-root, grace period 60 s, credentials from a
  chart-generated Secret); `scripts/m3-up.sh` (idempotent), `scripts/m3-chart-check.sh`,
  `scripts/m3-acceptance.sh`; actuator probes in every service.
- Restart cause found: on a cold start order-service (Flyway) and inventory-service (Redis seeding)
  exited 1 because Postgres and Redis were not reachable yet (7-8 restarts each). Fixed with init
  containers (`waitFor`). The Docker VM was also restarted by the host during that session (uptime
  10 min), which restarted the control plane pods; not a chart problem.
- Verified with `scripts/m3-chart-check.sh` and `--selftest`: both pass.
- Verified with `mvn verify` (repo root): BUILD SUCCESS, 98 tests, 0 failures, 6 ActuatorProbesIT included.
- Verified with `scripts/m3-acceptance.sh` (deletes the cluster first), exit 0:
  ```
  PASS AC4_clean_up - 13 pods Ready
  PASS AC4_second_run - same node, 0 restarts
  PASS AC5_order_confirmed - POST 202, 0d47129f-400a-5583-8484-2b823ae56800 CONFIRMED in Postgres and on orders.confirmed
  PASS AC6_graceful_restart - old pod logged 'Graceful shutdown complete', rollout completed
  RESULT: m3-acceptance passed
  ```
  Cold start to all Ready took about 2.5 min (images built from cache).
- NOT done / not verified: Compose stack not re-run (files untouched); a first run with an empty
  Docker build cache and empty image cache (long Maven and image pulls) was not timed; no HA for
  Postgres/Redis; secrets are dummy local defaults; only one laptop (Docker VM 8 GB, 6 CPU) tried.

### Milestone 4 RED step - 2026-10-30
- What exists: planned failure, replay, timeout, outbox, DLT, provider retry and conservation tests; real Testcontainers fixtures; compile-only `SagaReaper` and `OutboxRelay` contracts. No M4 production behavior implemented.
- Verified with Java 21: `mvn test-compile -DskipTests` -> exit 0. `mvn verify -DexcludedGroups=m4-red` -> BUILD SUCCESS, 98 existing tests pass, including M2 contention and replay once.
- RED verification: `mvn verify -fae -pl inventory-service,fake-payment-provider,order-service -am -Dgroups=m4-red` -> BUILD FAILURE as expected: inventory 2 missing-consumer timeouts; provider 1 failure/4 passes; order 35 missing-outbox assertion failures.
- RED verification: targeted E2E command -> 21 missing-DLT timeouts, 8 failure-handling RED invocations, 1 missing-outbox failure. Final OutboxE2ETest rerun -> AC4 RED, AC9 real-container/log-capture test PASS.
- `scripts/m3-chart-check.sh` -> Helm lint passes, topology RED: six DLT topics missing from Helm and Compose.
- Full commands, AC mapping, observed failures, skeleton contracts and deviations: [M4-TEST-HANDOFF.md](M4-TEST-HANDOFF.md).
- NOT done / not verified: M4 implementation, deeper outbox/race assertions beyond the missing-migration guards, a fully green default `mvn verify`, M2 x10 rerun, M3 cluster acceptance and chart mutation phase. AC7 has a documented randomness limitation until an approved seed hook exists. This is RED-step evidence, not milestone acceptance.

### Milestone 4 - 2026-10-30
- What exists:
  - order-service: Flyway V2 (`orders.outbox`, status `FAILED` replaces `PAYMENT_FAILED`, `RESERVED`
    allowed, partial indexes for the reaper and the relay). Each status change and its event
    (OrderConfirmed, or TicketsReleased on payment failure / timeout) commit in one transaction.
    `OutboxRelay` (every 100 ms) locks the oldest unpublished rows with `FOR UPDATE SKIP LOCKED`,
    sends them, waits for the acks, then sets `published_at`. `SagaReaper` (every 1 s) turns
    PENDING/RESERVED orders older than `ORDER_SAGA_TIMEOUT_SECONDS` (120) into FAILED(TIMEOUT).
  - inventory-service: `TicketsReleasedListener` + `release.lua` restock the stored reservation
    quantity once per orderId (`released` field on `inventory:order:{orderId}`).
  - payment-service: a decline (200 FAILED or 4xx) is final; 5xx, timeouts, network errors and
    unreadable replies are retried with the same Idempotency-Key, then dead-lettered.
  - fake-payment-provider: `FAKE_PROVIDER_FAILURE_RATE` (0.0 to 1.0); a decline is stored under the
    key so a retry gets the same answer.
  - Every consumer: shared `KafkaErrorHandling` (common): 4 attempts, backoff 1 s x2
    (`KAFKA_RETRY_*`), then `<topic>.DLT`, same partition, original bytes. All exceptions are
    retried, deserialization included.
  - notification-service: one NOTIFY per orderId (in memory, per pod).
  - Compose and Helm create the six `<topic>.DLT` topics (12 partitions; RF 1 / RF 3).
- Verified with `mvn verify` (repo root, head `faf7699`) -> BUILD SUCCESS in 12:46 min, 172 tests,
  0 failures, 0 errors, 0 skipped (common 20, api-gateway 22, inventory 23, payment 7, provider 12,
  order 50, notification 3, e2e 35). e2e classes in that run:
  ```
  Tests run: 1, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 101.0 s -- in com.ticketrush.e2e.OversellE2ETest
  Tests run: 21, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 373.9 s -- in com.ticketrush.e2e.DltE2ETest
  Tests run: 8, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 127.3 s -- in com.ticketrush.e2e.FailureE2ETest
  Tests run: 2, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 28.77 s -- in com.ticketrush.e2e.ReplayE2ETest
  Tests run: 1, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 8.813 s -- in com.ticketrush.e2e.PurchaseFlowE2ETest
  Tests run: 2, Failures: 0, Errors: 0, Skipped: 0, Time elapsed: 21.99 s -- in com.ticketrush.e2e.OutboxE2ETest
  ```
  Module ITs in the same run: OutboxIT 3, SagaReaperIT 32, OrderSagaIT 10, ReleaseIT 2,
  PaymentFlowIT 5, ChargeControllerIT 10, all green.
- Verified with `scripts/m3-chart-check.sh` -> `RESULT: m3-chart-check passed`, and `--selftest` ->
  `RESULT: m3-chart-check selftest passed` (the six AC8 DLT mutations are all rejected).
- Verified with `scripts/m2-acceptance.sh` (M2 contention and replay, 10 separate Maven runs) -> 10/10:
  ```
  run 1/10: PASS in 132s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/m2-acceptance/run-1.log)
  run 2/10: PASS in 135s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/m2-acceptance/run-2.log)
  run 3/10: PASS in 133s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/m2-acceptance/run-3.log)
  run 4/10: PASS in 134s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/m2-acceptance/run-4.log)
  run 5/10: PASS in 133s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/m2-acceptance/run-5.log)
  run 6/10: PASS in 133s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/m2-acceptance/run-6.log)
  run 7/10: PASS in 131s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/m2-acceptance/run-7.log)
  run 8/10: PASS in 133s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/m2-acceptance/run-8.log)
  run 9/10: PASS in 132s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/m2-acceptance/run-9.log)
  run 10/10: PASS in 130s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/m2-acceptance/run-10.log)
  RESULT: 10/10 runs passed
  ```
  OversellE2ETest per run: 99.1 / 101.8 / 100.4 / 101.5 / 100.0 / 99.4 / 99.4 / 99.7 / 99.6 / 98.2 s;
  ReplayE2ETest (2 tests): 28.9 / 29.1 / 29.4 / 29.0 / 29.3 / 29.5 / 28.5 / 28.8 / 29.0 / 29.0 s.
- Verified with `scripts/m3-acceptance.sh` (deletes and recreates the kind cluster; the old node
  container had exited 4 days earlier), exit 0:
  ```
  PASS AC4_clean_up - 13 pods Ready
  PASS AC4_second_run - same node, 0 restarts
  PASS AC5_order_confirmed - POST 202, 121fe014-a107-554b-9c0a-6973ae86e301 CONFIRMED in Postgres and on orders.confirmed
  PASS AC6_graceful_restart - old pod logged 'Graceful shutdown complete', rollout completed
  RESULT: m3-acceptance passed
  ```
  After it, `kubectl get kafkatopics` shows the 6 sources and 6 `.DLT` topics, 12 partitions, RF 3, Ready.
- NOT done / not verified:
  - Refunds: a charge that succeeds after the order FAILED(TIMEOUT) is only logged (WARN with the
    charge id) and stays on `payments.result`.
  - The relay has no backoff: a row whose publish or mark keeps failing re-sends its batch every
    tick and holds the outbox behind it (consumers dedupe, but nothing behind it moves). Not exercised.
  - In order-service a DB outage longer than the ~7 s retry window dead-letters valid messages; the
    order then times out. Not exercised.
  - Whether the offset of a dead-lettered record is committed when no later record follows on that
    partition (manual ack mode) was not checked; if not, a restart dead-letters it again.
  - notification dedupe is lost on restart and not shared between pods.
  - AC7 is stochastic (no seeded random source): the assertions require both CONFIRMED and FAILED
    orders and passed, but the exact share is not asserted.
  - Compose stack not re-run (its kafka-init command is only checked statically by the chart check).

### Milestone 4 review recovery - 2026-10-31
- Status: both PR 18 blockers implemented on `milestone-4`; pending code-reviewer verdict, not
  milestone acceptance or owner-ready. Recovery card `t_007379d5`, original `t_0371160d`.
- Verified code head: `c06b4f9ea9a259010993dda2b21e629a8a4cc47f`. The subsequent documentation
  commit changes only this report and DESIGN.md.
- Blocker 1: `1df0563` rejects replay when `released=1` in the atomic reserve script. Regression
  `FailureE2ETest.releasedReservationReplayNeverEmitsReservedOrCallsProvider` reserves, releases,
  then sends ten concurrent OrderRequested replays: no additional TicketsReserved or HTTP provider
  calls, ten rejections, unchanged restored stock, FAILED order, no charge or confirmation.
  With Java 21, command:
  `mvn -pl e2e-tests -am test -Dtest=FailureE2ETest#releasedReservationReplayNeverEmitsReservedOrCallsProvider -Dsurefire.failIfNoSpecifiedTests=false`
  -> RED: 1 failure (expected 1 TicketsReserved, got 11); GREEN: 1 test, 0 failures/errors/skipped,
  BUILD SUCCESS, 27.168 s.
- Blocker 2: `9902e15` implements the reviewer's legacy-compatibility option. V2 retains
  PAYMENT_FAILED in the expanded CHECK, then converts existing rows to FAILED. Expanding first
  also fixes upgrading a populated V1 database (the old CHECK rejects FAILED).
  `MigrationCompatibilityIT.v2AllowsLegacyPaymentFailureWrites` and
  `v2MigratesExistingPaymentFailures` use real PostgreSQL/Flyway. Command:
  `mvn -pl order-service -am verify -Dit.test=MigrationCompatibilityIT -Dfailsafe.failIfNoSpecifiedTests=false -Dtest=NoUnitTests -Dsurefire.failIfNoSpecifiedTests=false`
  -> RED: 2 CHECK-violation errors; GREEN: 2 tests, 0 failures/errors/skipped, BUILD SUCCESS, 4.876 s.
- First root `mvn verify` attempt -> BUILD FAILURE in 1:44 min: two SagaReaperIT snapshot
  comparisons raced the scheduled relay; only published_at changed from null to a timestamp.
  `c06b4f9` disables that fixture's relay with the existing switch. No assertions were removed or
  weakened; relay behavior is exercised separately in OutboxIT and OutboxE2ETest.
  `mvn -pl order-service -am verify -Dit.test=SagaReaperIT,MigrationCompatibilityIT -Dfailsafe.failIfNoSpecifiedTests=false -Dtest=NoUnitTests -Dsurefire.failIfNoSpecifiedTests=false`
  -> BUILD SUCCESS, 34 tests, 0 failures/errors/skipped, 12.401 s.
- Final `mvn verify` at repo root, Java 21, code head above -> exit 0:
  ```
  [INFO] BUILD SUCCESS
  [INFO] Total time:  13:51 min
  ```
  Parsed per-class results: 175 tests, 0 failures, 0 errors, 0 skipped. Includes
  MigrationCompatibilityIT 2, SagaReaperIT 32, OutboxIT 3, FailureE2ETest 9, DltE2ETest 21,
  OversellE2ETest 1, ReplayE2ETest 2, PurchaseFlowE2ETest 1, OutboxE2ETest 2.
- `scripts/m3-chart-check.sh` -> `RESULT: m3-chart-check passed`;
  `scripts/m3-chart-check.sh --selftest` -> `RESULT: m3-chart-check selftest passed`.
  `git diff --check origin/main...HEAD` and recovery added-line dash check -> clean.
  No separate lint or typecheck command is defined in CLAUDE.md/AGENTS.md; Maven compiled all modules.
- Compatibility limits: this retains the legacy writer's status value, not a full schema downgrade
  or an exercised M3 image rollback. V2 is still pre-merge and was edited in place; an existing
  development database with the earlier V2 checksum needs an explicit operator migration plan,
  not an automatic Flyway repair. No down-script or destructive cleanup was added.
- Additional documented limits from review: a TIMEOUT release received before reservation is a
  no-op; a reservation arriving later can hold stock for a FAILED order. Inventory or order-service
  consumer lag exceeding the timeout can also expire an otherwise valid payment. These lag cases
  were not failure-injected in this recovery; tuning against HTTP latency alone is insufficient.
- NOT re-run here: M2 ten-run acceptance, M3 cluster acceptance, Compose stack or CI (no checks
  reported on PR 18 before the push). The original M4 ten-run/cluster evidence above remains
  historical; root verification here includes contention and replay once. Refunds, 408/429 policy
  and durable storage of late-success charge ids remain outside these blocker fixes.
- Handoff: request review on this recovery card only. Orchestrator must reconcile stale original
  card `t_0371160d` and its gated pr-manager child `t_64d86fe2` after the reviewer verdict.
