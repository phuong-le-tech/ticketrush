# TicketRush - Progress

Update this file at the end of every milestone: status, the exact verification command and its
result, and what is NOT done or not verified.

| # | Milestone | Merged in | Status |
|---|---|---|---|
| 1 | Happy path locally | PR #12 (`794443c`) | Done |
| 2 | Correctness under contention | PRs #13, #15, #16 (`0f4d85e`) | Done |
| 3 | Kubernetes | PR #17 (`88f15f3`) | Done |
| 4 | Failure handling | PR #18 (`6a5a3b0`) | Done |
| 5 | Observability and autoscaling | PR #19 (`aa465d5`) | Done |
| 6 | Chaos experiments and results | PR #20 (`f409c88`) | Done |
| 7 | Minimal Angular dashboard | PRs #22, #23, #24 (`6335f62`) | Done. Accepted end to end 2026-11-09 with `scripts/dashboard-acceptance.sh`. #22 was merged without same-card review (process defect, see M7 report); #24 changes no file. Owed: one fully green root `mvn verify` (a transient `DltE2ETest` duplicate-delivery failure was seen once, the class passed on rerun) |
| 8 | Portfolio polish and live dashboard | not merged | Work done on branch `milestone-8` (README, CI workflow, demo script, live explanatory dashboard on kind, `scripts/live-dashboard-acceptance.sh`). Awaiting the owner's review, GIF re-recording and go-ahead; see the M8 section |

## Milestone 1 checklist (done)

Checklist (see [DESIGN.md](../DESIGN.md) for the approved layout, schemas and topics):

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
- Verified with: `scripts/oversell-acceptance.sh` (builds once with `mvn install -DskipTests`, then runs
  `OversellE2ETest` + `ReplayE2ETest` in 10 separate Maven runs, fresh JVM and containers each) -> 10/10:
  ```
  Building once (mvn install -DskipTests)...
  run 1/10: PASS in 135s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/oversell-acceptance/run-1.log)
  run 2/10: PASS in 128s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/oversell-acceptance/run-2.log)
  run 3/10: PASS in 132s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/oversell-acceptance/run-3.log)
  run 4/10: PASS in 137s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/oversell-acceptance/run-4.log)
  run 5/10: PASS in 144s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/oversell-acceptance/run-5.log)
  run 6/10: PASS in 129s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/oversell-acceptance/run-6.log)
  run 7/10: PASS in 124s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/oversell-acceptance/run-7.log)
  run 8/10: PASS in 129s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/oversell-acceptance/run-8.log)
  run 9/10: PASS in 128s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/oversell-acceptance/run-9.log)
  run 10/10: PASS in 127s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/oversell-acceptance/run-10.log)
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
- Verified with: `scripts/oversell-acceptance.sh` -> 10/10:
  ```
  Building once (mvn install -DskipTests)...
  run 1/10: PASS in 139s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/oversell-acceptance/run-1.log)
  run 2/10: PASS in 129s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/oversell-acceptance/run-2.log)
  run 3/10: PASS in 127s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/oversell-acceptance/run-3.log)
  run 4/10: PASS in 129s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/oversell-acceptance/run-4.log)
  run 5/10: PASS in 133s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/oversell-acceptance/run-5.log)
  run 6/10: PASS in 133s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/oversell-acceptance/run-6.log)
  run 7/10: PASS in 133s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/oversell-acceptance/run-7.log)
  run 8/10: PASS in 128s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/oversell-acceptance/run-8.log)
  run 9/10: PASS in 128s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/oversell-acceptance/run-9.log)
  run 10/10: PASS in 133s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/oversell-acceptance/run-10.log)
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
  chart-generated Secret); `scripts/kind-up.sh` (idempotent), `scripts/base-chart-check.sh`,
  `scripts/kind-acceptance.sh`; actuator probes in every service.
- Restart cause found: on a cold start order-service (Flyway) and inventory-service (Redis seeding)
  exited 1 because Postgres and Redis were not reachable yet (7-8 restarts each). Fixed with init
  containers (`waitFor`). The Docker VM was also restarted by the host during that session (uptime
  10 min), which restarted the control plane pods; not a chart problem.
- Verified with `scripts/base-chart-check.sh` and `--selftest`: both pass.
- Verified with `mvn verify` (repo root): BUILD SUCCESS, 98 tests, 0 failures, 6 ActuatorProbesIT included.
- Verified with `scripts/kind-acceptance.sh` (deletes the cluster first), exit 0:
  ```
  PASS AC4_clean_up - 13 pods Ready
  PASS AC4_second_run - same node, 0 restarts
  PASS AC5_order_confirmed - POST 202, 0d47129f-400a-5583-8484-2b823ae56800 CONFIRMED in Postgres and on orders.confirmed
  PASS AC6_graceful_restart - old pod logged 'Graceful shutdown complete', rollout completed
  RESULT: kind-acceptance passed
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
- `scripts/base-chart-check.sh` -> Helm lint passes, topology RED: six DLT topics missing from Helm and Compose.
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
- Verified with `scripts/base-chart-check.sh` -> `RESULT: base-chart-check passed`, and `--selftest` ->
  `RESULT: base-chart-check selftest passed` (the six AC8 DLT mutations are all rejected).
- Verified with `scripts/oversell-acceptance.sh` (M2 contention and replay, 10 separate Maven runs) -> 10/10:
  ```
  run 1/10: PASS in 132s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/oversell-acceptance/run-1.log)
  run 2/10: PASS in 135s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/oversell-acceptance/run-2.log)
  run 3/10: PASS in 133s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/oversell-acceptance/run-3.log)
  run 4/10: PASS in 134s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/oversell-acceptance/run-4.log)
  run 5/10: PASS in 133s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/oversell-acceptance/run-5.log)
  run 6/10: PASS in 133s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/oversell-acceptance/run-6.log)
  run 7/10: PASS in 131s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/oversell-acceptance/run-7.log)
  run 8/10: PASS in 133s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/oversell-acceptance/run-8.log)
  run 9/10: PASS in 132s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/oversell-acceptance/run-9.log)
  run 10/10: PASS in 130s - Tests run: 3, Failures: 0, Errors: 0, Skipped: 0 (log: e2e-tests/target/oversell-acceptance/run-10.log)
  RESULT: 10/10 runs passed
  ```
  OversellE2ETest per run: 99.1 / 101.8 / 100.4 / 101.5 / 100.0 / 99.4 / 99.4 / 99.7 / 99.6 / 98.2 s;
  ReplayE2ETest (2 tests): 28.9 / 29.1 / 29.4 / 29.0 / 29.3 / 29.5 / 28.5 / 28.8 / 29.0 / 29.0 s.
- Verified with `scripts/kind-acceptance.sh` (deletes and recreates the kind cluster; the old node
  container had exited 4 days earlier), exit 0:
  ```
  PASS AC4_clean_up - 13 pods Ready
  PASS AC4_second_run - same node, 0 restarts
  PASS AC5_order_confirmed - POST 202, 121fe014-a107-554b-9c0a-6973ae86e301 CONFIRMED in Postgres and on orders.confirmed
  PASS AC6_graceful_restart - old pod logged 'Graceful shutdown complete', rollout completed
  RESULT: kind-acceptance passed
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
- `scripts/base-chart-check.sh` -> `RESULT: base-chart-check passed`;
  `scripts/base-chart-check.sh --selftest` -> `RESULT: base-chart-check selftest passed`.
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

### Milestone 5 - test-only RED phase

- New metrics tests compile on Java 21. Service tests produced 35 assertion failures and zero
  errors; metrics E2E produced 3 assertion failures and zero errors. Missing meters and HTTP 404
  are the expected RED conditions. No production instrumentation or deployment code is added.
- `scripts/observability-chart-check.sh` rejects the current chart's missing monitoring/scaling resources;
  `scripts/autoscale-acceptance.sh` rejects missing `scripts/observability-up.sh` before touching kind.
  Both scripts' offline `--selftest` modes pass. Synthetic fixtures prove only the evaluators.
- AC7 regression: `scripts/base-chart-check.sh` and its `--selftest` pass. The destructive
  `scripts/kind-acceptance.sh` was not run in this test-only phase: cluster bootstrap/real k6 scaling
  belong to the downstream implementation run. This is a not-run limitation, not M3 acceptance.
- NOT verified here: real M5 cluster, dashboard data, KEDA scale-up/down, or k6 conservation.
  See `docs/internal/M5-RED-RESULTS.md` for exact test commands, expected failures and plan deviations.

### Milestone 5 implementation - 2026-10-31

- Status: implementation verified locally, independent review pending on child `t_151f8374`.
  No push, PR, merge or M6 work in this implementation phase. Executable code head `0dddd8c`;
  subsequent documentation/evidence commit does not change code.
- What exists: Prometheus registry/exposure on all six services; per-consumer/topic attempt timers;
  acknowledged gateway POST counter; after-commit saga outcome counters (replays/rollbacks add zero).
  Strimzi exporter supplies real group/topic lag. The chart provisions six ServiceMonitors, exporter
  PodMonitor, Prometheus, a Secret-backed Grafana with a four-panel ConfigMap dashboard, and Kafka
  KEDA ScaledObjects for inventory/payment (min 1, max 2 <= 12 partitions). `observability-up.sh` installs pinned
  operators before the chart, reuses content-tagged builds, and preserves generated credentials.
  `autoscale-load.js` drives 24 events with strict k6 thresholds; optional 200ms consumer delay creates lag.
- Full unfiltered root verification in this run, Java 21.0.10:
  ```sh
  export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home
  export PATH="$JAVA_HOME/bin:$PATH"
  mvn verify
  ```
  -> exit 0, **213 tests, 0 failures, 0 errors, 0 skipped** (175 baseline + 38 M5 tests).
  ```text
  [INFO] BUILD SUCCESS
  [INFO] Total time:  17:13 min
  [INFO] Finished at: 2026-10-31T06:35:42+01:00
  ```
  All six PrometheusIT, four MetricsTest classes, SagaMetricsIT (8) and MetricsE2ETest (3) passed.
  Existing contention, replay, failure, DLT, outbox and migration suites passed unchanged.
  Per-class counts and reactor output: [maven-summary.txt](../evidence/autoscaling/maven-summary.txt).
  No separate lint/typecheck command is defined in CLAUDE.md/AGENTS.md; Maven compiled all modules.
- `scripts/observability-chart-check.sh`, `scripts/observability-chart-check.sh --selftest`,
  `scripts/autoscale-acceptance.sh --selftest`, `scripts/base-chart-check.sh` and its `--selftest` -> exit 0.
  Helm lint/template and resource mutation checks pass. `bash -n scripts/observability-up.sh
  scripts/autoscale-acceptance.sh` and added-line typography/`git diff --check` -> clean.
- Real clean-kind acceptance, run in this implementation session:
  ```sh
  M5_CONSUMER_PROCESSING_DELAY_MS=200 scripts/autoscale-acceptance.sh
  ```
  -> exit 0 in 492.626 seconds, clean Kubernetes 1.36.4 on kind 0.33.0, k6 2.3.0.
  ```text
  PASS warm-up - real confirmed order and committed, drained lag for both groups
  PASS AC4_cleanAndRepeat - Ready stack, stable node/Secret, four provisioned queries return data
  PASS AC5_scaleAndConserve - both services causal 1 -> >1 -> 1; stock/orders/charges conserved
  RESULT: autoscale-acceptance passed
  ```
  Full raw output and measured audit: [acceptance.log](../evidence/autoscaling/acceptance.log),
  [evidence.json](../evidence/autoscaling/evidence.json), [samples.jsonl](../evidence/autoscaling/samples.jsonl),
  [k6-summary.json](../evidence/autoscaling/k6-summary.json). These are live outputs, not selftest fixtures.
  Representative raw epoch-second measurements (lag, ready replicas):

  | Service | Baseline | Above threshold at 1 | Scale-up | Drained | After load + >=30s drained |
  |---|---|---|---|---|---|
  | inventory | 1793424641.2098382: 0, 1 | 1793424653.126893: 25, 1 | 1793424674.071604: 126, 2 | 1793424775.815177: 0, 2 | 1793424814.6380641: 0, 1 |
  | payment | 1793424641.255181: 0, 1 | 1793424674.161098: 26, 1 | 1793424690.1167018: 101, 2 | 1793424781.495662: 0, 2 | 1793424814.69504: 0, 1 |

  k6 load timestamps: 1793424642.7319071 to 1793424764.953024. Maximum observed lag: inventory 126,
  payment 206. Both scale down, not merely stop growing. With min=1 the HPA's 30s scale-down
  stabilization controls return to 1; KEDA's cooldown governs scale-to-zero, which is disabled.
- Real conservation audit: **960 acknowledged distinct orders = 960 CONFIRMED; 960 charges, exactly
  one per order; stock 12000 = remaining 11040 + sold 960**. No duplicate order/idempotency tuple,
  no unsettled order, no oversell, no provider restart. k6 dropped iterations 0, failed-request rate 0,
  checks 960/960. The isolated warm-up is excluded from these 24-event audit totals.
- Real Grafana provisioned-dashboard API and exact dashboard PromQL returned nonempty finite data:
  lag 5 series, processing time 7, orders/sec 1, saga outcomes 3. Repeat bootstrap preserved the
  kind node ID and Grafana Secret fingerprint; all pods Ready after the audit. Passwords are not logged.
- Diagnosis and reversible adjustments: initial bootstrap failed on an absent operator TLS Secret
  with admission hooks off; disabled operator TLS too. Short Kafka DNS did not resolve from a separate
  KEDA namespace; co-located KEDA with the apps. Grafana at 256Mi/500m repeatedly exhausted its
  startup window; 512Mi/1 CPU made a fresh pod Ready in 10s, and clean acceptance confirmed it.
  The earlier acceptance timeout is not counted as success. Version/compatibility URLs were checked
  against official KEDA, kind, kube-prometheus-stack, Prometheus Operator and k6 sources in DESIGN.md.
- Fixed test: `ec94ce3` adds a real isolated warm-up order and bounded drained-offset wait because the
  exporter emits no group/topic series before any commit. It also uses the permitted exporter-specific
  `kafka_consumergroup_lag_sum` contract. No scaling/conservation assertions or Java tests weakened.
  Raw per-partition lag can be -1 for uncommitted offsets; the documented sum omits unknown partitions.
- NOT done / limitations: independent exact-head review and PR are downstream gates, not claimed here.
  M2 ten-run acceptance and Compose were not rerun (root verify includes contention/replay once).
  Destructive `kind-acceptance.sh` was not rerun: it replaces the same local cluster and disables M5;
  its static checks pass and clean M5 bootstrap exercised the shared app/Kafka/DB/probe paths, but
  M3's dedicated graceful-rollout assertion is not new evidence. Builds used the laptop's image/build
  caches; no empty-cache timing claim. Metrics/Grafana data are ephemeral and outcomes are counters,
  not a durable financial ledger. This is a single 8GB Docker VM demonstration at 8 orders/sec with
  intentional delay, not a capacity benchmark. No refunds or changes to the M4 late-success/timeout
  limitations, topic names/partitions/keys, event schemas or DB schema. No new application services.

### Milestone 6 - partial implementation, blocked on 2026-11-04

- **NOT accepted and NOT handed to review.** Four experiment families have complete real audits:
  inventory-kill 4, broker-restart 3, payment-declines 3, postgres-latency 3. The first consumer ramp
  stalled before eight Ready pods were verified. No M7/M8 work, push or PR.
- Implemented `scripts/chaos.sh`, a shared offline evaluator, context refusal before live tool
  use, acknowledged-ID collection using the reused M5 k6 purchase flow, reversible fault handlers,
  command/ledger/sample evidence and resume-by-re-evaluation. No new dependency, application
  behavior, event/topic/key/schema/public API change or M5 chart-default change. The two new k6
  variables are documented in `.env.example`. Test-writer tests remain unchanged.
- RED reproduced: `scripts/chaos-test.sh --offline` -> 60 assertion failures, 9 skips, 0 errors,
  caused by the missing runner. Fresh offline GREEN -> `OK (skipped=9)`, 25 methods, no failures
  or errors. `python3 scripts/chaos-collector-test.py` -> six tests pass. Collector regressions were
  observed RED first for restoration ordering, child-stop error handling, stale scrape acceptance,
  terminating rollout pods and the actual provider decline reason.
- `scripts/chaos.sh --selftest` -> `RESULT: m6-chaos selftest passed (offline synthetic evidence only)`.
  It rejects synthetic oversell, lost-order and duplicate-charge inputs, without Kubernetes access.
- `scripts/base-chart-check.sh` -> `RESULT: base-chart-check passed`.
  `scripts/observability-chart-check.sh` -> `RESULT: observability-chart-check passed`.
  Their selftests and `scripts/autoscale-acceptance.sh --selftest` also passed earlier in this run.
  A later hygiene check caught extra blank lines at the new documentation EOFs; those were fixed
  rather than changing a test. Final command logs are in `docs/evidence/chaos/verification/`.
- Real execution used `scripts/observability-up.sh` after restarting the previously stopped project node.
  The first bootstrap met a transient API validation error; its retry completed with all pods Ready.
  `scripts/chaos.sh` then completed all three inventory and broker repetitions but exited 1:
  `FAIL m6-chaos: provider audit requires one pod`. A terminating old provider pod caused this
  pre-load collector refusal, fixed test-first without changing application code.
- `scripts/chaos.sh --resume` re-evaluated completed live files and collected only missing runs.
  Its first payment audit was rejected by the checker expecting PAYMENT_FAILED (release-event
  reason) rather than SQL's PROVIDER_DECLINED. The unchanged real JSON passed after a new test
  and evaluator fix and was preserved under its normal run filename, counted only once. Later
  payment repetitions passed directly. The final resumed command was stopped after the ramp
  runtime failure and best-effort cleanup, not reported as exit 0.
- Every completed run, date, exact recovery value and before/during/after completion rate is in
  the [README table](../CHAOS-RESULTS.md#measured-results), linked to its raw JSON. Confirmed orders
  each have one charge; nonconfirmed orders zero, every accepted ID is conserved, stocks are
  nonnegative and conserved, and payment failures release held quantity once. These claims apply
  only to the completed audits, NOT the failed ramp or interrupted runs. k6 acknowledgments are
  960 or 961 per completed run, with the actual count recorded rather than assuming rate x time.
- Real payment outcomes: 671 CONFIRMED / 290 FAILED, 672 / 288 and 667 / 294; all failures in these
  audits were compensated, uncharged provider declines. Postgres baseline/injected wall-clock
  query seconds: 0.08587195799918845 / 4.016240458004177,
  0.07906504100537859 / 3.7067007919831667, and 0.07849620800698176 / 4.01285149998148.
  This is a four-second table-lock injection with kubectl/psql overhead, not general network delay.
- Failed/interrupted attempts are retained and listed in README: two k6 arrival failures (13 and
  5 dropped iterations), one intentionally interrupted inventory audit, the pre-load provider
  baseline refusal and the failed ramp. Verbatim runtime logs are preserved in
  `docs/evidence/chaos/raw-logs.zip`; no whitespace cleanup or synthetic output replaced those bytes.
- **Runtime blocker:** the first inventory rollout toward eight reported only 5 of 8 new replicas
  updated, then timed out. k6 recorded 381 acknowledgments, 923 dropped iterations and 960 failed
  HTTP attempts. API TLS handshakes and the project's containerd CRI subsequently timed out.
  Resource/startup pressure is a hypothesis, not a demonstrated OOM diagnosis. A project-node
  stop/start was BLOCKED by the headless approval gate and did not execute; no bypass was attempted.
- The failed collector and its shell were stopped and verified absent. Raw command output records
  best-effort successful patches for original resources/JVM options, desired replicas 1, provider
  rate 0, maxReplicaCount 2 and removal of pause annotations. A later API read still showed stale
  inventory reconciliation (5 up-to-date for desired 1) and Paused scaler status. **No successful
  after-snapshot or fully restored runtime is claimed.** Baseline and exact commands:
  `docs/evidence/chaos/m6-consumer-ramp-1793788102587266000/before.json` and `commands.jsonl`.
- Final `scripts/chaos-test.sh` -> `FAILED (failures=7, skipped=1)`, 25 tests, no errors.
  All offline contract cases and four real experiment audit methods pass. Remaining failures are
  missing ramp/cleanup evidence, the incomplete results set and the manifest's honest nonzero/
  not-run states. Raw output: `docs/evidence/chaos/verification/full-harness.log`.
- **NOT RUN: `mvn verify`** in this implementation run because the Docker/kind runtime became
  unhealthy. The required next command is
  `JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home mvn verify` after recovery.
  The parent's earlier 213-test result is historical, not fresh verification for this implementation.
  No separate lint/typecheck command exists; shell syntax and Python compilation were checked.
- **NOT done / unverified:** three successful consumer-ramp repetitions, actual throughput at eight,
  SIGINT and timeout cleanup probes, the real one-show 1-versus-8 benchmark, a green full acceptance
  harness, fresh root Maven verification, independent review and PR. Recovery numbers above are
  conservative fault-to-post-load settlement intervals, not isolated minimum MTTR; the provider's
  after-restoration window is idle because its in-memory charge audit precedes rollout.
- Operator action: recover the local project Docker/kind runtime with an approved lifecycle action,
  verify baseline reconciliation, then investigate startup/resource pressure before retrying the
  ramp. Do not rerun the same rollout blindly. [Blocker details](../evidence/chaos/blocked.txt) and
  `verification.json` record failures/not-run states explicitly; they are not a green milestone manifest.

### Milestone 6 retry - 2026-11-08, 8GB attempt (superseded by recovery below)

- Phuong authorized a fresh retry after the prior quota/gateway failures. Branch remains
  `milestone-6`; code change committed locally as `5f4842e`. No push, PR, review handoff or M7/M8 work.
- Contrary to the initial operator note, the project cluster was not empty. Docker Desktop had
  changed current-context to `docker-desktop`. All project reads used `--context kind-ticketrush`;
  current-context was explicitly switched back and read back before the guarded live runner.
- Both old reduced inventory pods reported `OOMKilled`, exit 137, at 256Mi. Restored the original
  M5 baseline before load and read back consumers 1/1 Ready, original 512Mi limits / 384Mi
  requests and MaxRAMPercentage=60, max 2 and Paused=False scalers, provider rate 0 and zero
  chaos DB sessions. All project pods were Ready before load.
- `scripts/observability-up.sh` failed twice fetching its unchanged pinned Prometheus chart with HTTP 503.
  Logs: `verification/bootstrap-2026-11-08.log` and `bootstrap-retry-2026-11-08.log` under the M6
  evidence directory. Reused the existing healthy M5 stack as allowed by the spec; no new dependency.
- Added two collector regressions, observed both fail, then implemented waiting for terminating
  surplus pods and a 384Mi experimental limit. Preserved the incremental startup work from the
  interrupted retry, extending ramp load to 360s without changing 8/s or strict k6 thresholds.
  `python3 scripts/chaos-collector-test.py` -> `Ran 9 tests in 1.009s`, `OK`.
- `scripts/chaos.sh --experiment consumer-ramp --runs 1 --timeout-seconds 900` -> exit 1:
  `FAIL m6-chaos: command failed: kubectl --context kind-ticketrush -n ticketrush --request-timeout=30s rollout: error: timed out waiting for the condition`.
  Inventory reached four Ready / five desired; no eight-pod or payment-ramp result was collected.
  Raw evidence: `docs/evidence/chaos/m6-consumer-ramp-1794134769312914000/`.
  k6 recorded 963 acknowledgments, 794 dropped iterations, 851 failed HTTP requests and 235 failed
  checks. These are failed-attempt observations, NOT a completed invariant audit or recovery result.
- During slowdown, kubelet log retrieval timed out at TLS handshake. VM memory available was
  967Mi and swap use 590Mi; Docker reported 6.076GiB usage in an earlier sample. Memory/startup
  and I/O pressure remain possible causes, not a conclusive root cause of the node slowdown.
  The four Ready reduced pods had zero restarts at the observed checkpoint.
- SIGINT was sent to the collector during failure handling. The collector exited 1, with cleanup
  commands logged as successful. Subsequent reads showed all pods Ready, inventory/payment
  1/1, max 2 and Paused=False, and the original inventory resources/JVM settings. No collector
  or k6 process remained. No node stop/restart or approval-gate bypass was attempted in this run.
- An exact full after-snapshot comparison did NOT run: a combined command was refused by the
  user-defined `jq *.env*` deny rule. It was not retried or rephrased. Earlier, a one-off Python
  `-c` restoration command was denied and did not run; restoration instead used direct baseline
  kubectl operations. Only the observed readbacks above are claimed, not exact cleanup acceptance.
- Fresh `scripts/chaos.sh --selftest` ->
  `RESULT: m6-chaos selftest passed (offline synthetic evidence only)`.
  Fresh `scripts/chaos-test.sh --offline` -> `OK (skipped=9)`, 25 tests, no failures.
  Fresh `scripts/base-chart-check.sh` -> `RESULT: base-chart-check passed`.
  Fresh `scripts/observability-chart-check.sh` -> `RESULT: observability-chart-check passed`.
  M3/M5 chart selftests and M5 acceptance selftest also passed in this retry.
- Fresh full `scripts/chaos-test.sh` -> `FAILED (failures=7, skipped=1)`, 25 tests.
  The original 13 successful audits re-evaluate successfully, but no additional pass is claimed.
  All fresh verification logs use `*-2026-11-08.log` names, distinct from historical evidence.
  Verbatim raw logs for the preserved interrupted retry and this run are in `retry-raw-logs.zip`;
  original local files remain unmodified.
- **NOT RUN: `mvn verify`** in this retry. The monitored ramp again destabilized the shared Docker
  runtime; no Testcontainers workload was added to it. Parent/historical Maven output is not fresh.
  **NOT done / unverified:** three successful ramp repetitions, SIGINT/timeout cleanup acceptance,
  single-show 1-versus-8 measurements, fresh Java 21 Maven verification, full acceptance, review, PR.
- Operator action needed before further load: investigate the local Docker VM's resource/runtime
  pressure and provide sufficient headroom for eight consumers. Current VM reports 8,320,028,672
  bytes and six CPUs. Do not repeat the same ramp blindly or relax acceptance thresholds.

### Milestone 6 - 16GB recovery and verification (2026-11-08)

- Phuong increased Docker memory during this run. Independently observed MemTotal 16,788,209,664
  bytes (15.64GiB), six CPUs unchanged, node restart `2026-11-08T10:56:12.6753005Z`, all project
  pods Ready. Rechecked consumers at chart requests 384Mi / limits 512Mi and
  `-XX:+UseSerialGC -XX:MaxRAMPercentage=60`, provider rate 0, zero chaos sessions, max 2/unpaused
  scalers. Explicitly selected and read back `kind-ticketrush` again after Docker changed context.
- Removed the experimental small-heap/resource override rather than changing M5 defaults. Kept
  one-at-a-time staging and waiting for terminating pods. Strengthened the own collector sizing
  test to forbid deployment-template changes; observed RED before the implementation removal.
  Commits: `b4bce0e` test contract correction and `7490fe4` implementation. Test-writer tests and
  application code remain unchanged; no new dependency or public/topic/schema/key change.
- `scripts/chaos.sh --experiment consumer-ramp --runs 3 --timeout-seconds 900` -> exit 0,
  `RESULT: m6-chaos passed`. Each 24-show repetition acknowledged 2,881 IDs, reached eight Ready
  inventory and payment replicas in separate phases, passed terminal/stock/unique-order/charge
  invariants, and restored exact before/after settings. Recovery seconds:
  340.56781005859375, 339.5589737892151, 340.2294728755951. Before/during/after and per-Ready-count
  measurements are in README, with every JSON linked once. Log: `verification/ramp-16gb-2026-11-08.log`.
- `scripts/chaos.sh --resume` -> exit 0, `RESULT: m6-chaos passed`. This is the actual successful
  whole-runner invocation; the earlier unqualified `scripts/chaos.sh` exit 1 is retained as
  historical failure, not rewritten as success. Resume re-evaluated completed live evidence,
  then actually ran both cleanup probes and the one-show comparison. Log:
  `verification/resume-16gb-2026-11-08.log` under `docs/evidence/chaos/`.
- Real SIGINT and deadline-timeout probes both mutated effective settings after acknowledged
  load, exited 1 as intended, restored exact snapshots, then completed independently audited
  inventory-kill reruns with disjoint order IDs. Evidence: `cleanup-SIGINT.json`,
  `cleanup-timeout.json` and their linked rerun JSONs. Aborted first loads are NOT counted as passes.
- Real single-show comparison (`bottleneck.json`): 175 completions at one Ready inventory replica,
  7.999579402694281 terminal outcomes/s; 124 completions in the shorter eight-Ready window,
  8.025785165011973/s. The entire single-show ramp also passed invariants with 2,881 acknowledged
  IDs and exact restoration. At an offered 8/s this is not a saturation ceiling or evidence that
  inventory was the active bottleneck. The one-key/one-partition limit remains documented.
- Completed evidence totals: inventory-kill 6 (including the two cleanup reruns), broker-restart 3,
  payment-declines 3, postgres-latency 3, consumer-ramp 4 (three standard plus one single-show).
  All 19 completed audits are listed individually in README. The original 13 audits remain
  unchanged. 8GB and 16GB results are not pooled; failed and interrupted attempts remain separate.
- After the resumed batch, both ScaledObjects read max 2 and Paused=False, consumers 1/1, with no
  remaining collector or k6 process. Runtime readback: `verification/runtime-16gb-2026-11-08.log`.
- Fresh Java 21 `mvn verify` (after all chaos load stopped) -> exit 0, `[INFO] BUILD SUCCESS`,
  `[INFO] Total time:  15:37 min`. Parsed all 43 Surefire/Failsafe XML reports: 213 tests,
  zero failures, errors or skips. Module counts: common 20, gateway 23, inventory 30, payment 11,
  provider 13, order 70, notification 7, E2E 39. E2E: Oversell 1, DLT 21, Metrics 3, Failure 9,
  Replay 2, PurchaseFlow 1, Outbox 2, all green. Counts and verbatim selected lines:
  `verification/maven-16gb-counts.json`, `verification/maven-16gb-summary-2026-11-08.log`.
  Full unmodified Maven output is the `runtime-2026-11-08/maven.log` member of `retry-raw-logs.zip`.
- Latest `scripts/chaos.sh --selftest` passes; `python3 scripts/chaos-collector-test.py` passes
  nine tests; `scripts/chaos-test.sh --offline` -> `OK (skipped=9)`, 25 discovered tests.
  Latest `scripts/base-chart-check.sh` -> `RESULT: base-chart-check passed`.
  Latest `scripts/observability-chart-check.sh` -> `RESULT: observability-chart-check passed`. Its first final check
  rejected trailing whitespace in the untracked raw Maven output; preserving that output in the
  ignored raw-log directory/ZIP made the unchanged hygiene check pass. The failed log is retained.
  Shell syntax and Python compilation pass; the repository has no separate lint/typecheck command.
- All raw archive members were CRC-tested and compared byte-for-byte with original local files:
  `PASS archive CRC and original-byte comparison: 98 members`. SHA256/byte counts are in
  `retry-raw-logs-index.json`; earlier `raw-logs.zip` is unchanged.
- Final `scripts/chaos-test.sh` -> exit 0, `OK (skipped=1)`, 25 tests, no failures or errors.
  Only the opt-in regression wrapper is skipped; its unchanged Maven/chart commands passed
  separately in this same run. Final log: `verification/full-harness-16gb-2026-11-08.log`.
- **NOT done / unverified:** independent review and PR. No push, merge or M7/M8 work.
  Six CPUs remain unchanged, CPU throttling and maximum
  sustainable rate are not quantified, recovery includes remaining load and polling, and provider
  after-restoration throughput is an idle audit window. This is not a simultaneous 8+8 benchmark.

### Milestone 7 backend implementation - 2026-11-09

- Backend phase only, independent same-card review pending on `t_31ecab6c`. M7 is not accepted.
  Open Design reference approval is recorded in `docs/design/dashboard/README.md`; Angular and serving
  work remain downstream. No M8 work.
- Implemented inventory-service `GET /events/{eventId}/stock` with one Redis HGET, long stock,
  blank/overlength validation and unknown-event 404. Implemented order-service
  `GET /orders/activity?limit=20` with limit 1..50, explicit public projection, newest-first
  `created_at` ordering and a strict PostgreSQL ten-second count independent of limit/status.
  No customer id/idempotency key in the projection. Nullable saga metadata remains null.
- Commits: `516dacb` (stock), `d03f922` (order activity). No test-writer test changed, no new
  dependency, no write path, Lua, topic, event/DB schema, key or M5/M6 default changed.
  DESIGN.md records API contracts, M7 decisions, scan/sort cost and the UI container pin.
- RED reproduced in this run, Java 21.0.10:
  `mvn verify -fae -pl inventory-service,order-service -am -Dgroups=m7-red` -> expected exit 1,
  26 cases, 25 assertion failures, zero errors. Missing routes returned 404; the repository count
  threw its placeholder exception, captured as an assertion failure. Unknown-event 404 already passed.
- GREEN: `mvn verify -fae -pl inventory-service,order-service -am` -> exit 0, BUILD SUCCESS,
  146 tests, zero failures/errors/skips, 01:24 min. Includes StockEndpointIT 8,
  OrderActivityIT 11 and OrderActivityWindowIT 7; all existing module tests also passed.
- Full unfiltered root verification in this run:
  ```sh
  export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home
  export PATH="$JAVA_HOME/bin:$PATH"
  mvn verify
  ```
  -> exit 0; all 46 Surefire/Failsafe XML reports aggregated with xmllint:
  ```text
  Reports=46 tests=239 failures=0 errors=0 skipped=0
  [INFO] BUILD SUCCESS
  [INFO] Total time:  15:21 min
  ```
  Includes contention, replay, failure, DLT, metrics, outbox, schema/migration and happy-path E2E.
  The first foreground root attempt was interrupted by the tool's 420s call deadline and ended
  BUILD FAILURE without test assertion failures. The successful rerun used a tracked background
  process and completed normally; no code/test changes were needed between attempts.
- `git diff --check` passes. No separate lint/typecheck command exists in CLAUDE.md/AGENTS.md;
  Maven compiled all modules. Docker daemon 29.9.2 was reachable. Verified the downstream pin:
  `docker run --rm node:24.22.2 node --version` -> `v24.22.2`; no npm command or host upgrade.
- NOT done / unverified: Angular/Vitest, nginx/Compose serving, real running-dashboard screenshot,
  `scripts/dashboard-acceptance.sh`, M2 ten-run acceptance, kind/k6 or capacity testing. Root verification
  includes contention/replay once only. No UI/deployment AC is checked off for this backend phase.
  Order count and recent rows are separate snapshots, no new index bounds large-table query cost,
  no outage response contract or performance claim. Review and owner approval remain gates.

### Milestone 7 dashboard acceptance and docs - 2026-11-09

- Branch `docs/dashboard-acceptance`, cut from `origin/main` `199bde2` (not `milestone-7`): Phuong had
  already squash-merged PR #22 (M7 backend, head `a1eb727`) as `199bde2` before the same-card
  review, so main already carried the backend, the dashboard and the serving commit. Working on
  `milestone-7` would have produced a conflicting or duplicate PR.
- The same-card review of the M7 backend card and of the Angular implementation card did not
  happen before the owner merge. That is recorded here as a defect in the process, not as a
  verified approval: this section reports execution evidence only.
- What exists now: inventory-service `GET /events/{eventId}/stock`, order-service
  `GET /orders/activity?limit=20` (public projection, no customer id or idempotency key),
  the Angular 22.2.5 dashboard in `dashboard/`, the nginx-unprivileged Compose service on port
  4200 with same-origin proxies, and `scripts/dashboard-acceptance.sh`.
- `scripts/dashboard-acceptance.sh` is one command and proves by execution: preflight (Docker 29.9.2,
  Java 21.0.10, `node:24.22.2` -> `v24.22.2`), backend module tests
  (`mvn verify -pl inventory-service,order-service -am`), dashboard `npm ci` + Vitest + production
  build in the pinned Node container, `docker compose up -d --build --wait` (it first resets the
  local anonymous Postgres volume, because a container recreated from an older milestone kept an
  outdated Flyway history and order-service refused to start), readiness waits on both read APIs
  through the dashboard proxy, then the flow through http://localhost:4200. It does not touch
  kind, Helm or k6.
- Real output of the recorded run (exit 0, 2026-11-09 11:38 to 11:40 local):

  ```text
  ==> Step 1/5: preflight (Docker, Java 21, pinned Node image node:24.22.2)
      Docker server 29.9.2
      Java: java version "21.0.10" 2026-01-20 LTS
      Node in container: v24.22.2
      PASS: toolchain ready

  ==> Step 2/5: backend module tests (inventory-service, order-service)
      [INFO] BUILD SUCCESS
      [INFO] Total time:  01:24 min
      Surefire/Failsafe report files: 19
      PASS: backend module tests green

  ==> Step 3/5: dashboard npm ci + Vitest + production build in node:24.22.2
      Application bundle generation complete. [1.028 seconds] - 2026-11-09T10:38:27.668Z
       Test Files  3 passed (3)
            Tests  25 passed (25)
      Application bundle generation complete. [1.101 seconds] - 2026-11-09T10:38:31.515Z
      PASS: dashboard tests and production build green

  ==> Step 4/5: compose up with the dashboard (clean local data, build images, wait for health)
      api-gateway running Up 9 seconds
      dashboard running Up 5 seconds (healthy)
      fake-payment-provider running Up 28 seconds
      inventory-service running Up 5 seconds
      kafka running Up 28 seconds (healthy)
      notification-service running Up 9 seconds
      order-service running Up 9 seconds
      payment-service running Up 9 seconds
      postgres running Up 28 seconds (healthy)
      redis running Up 9 seconds (healthy)
      dashboard container uid: 101
      ready: GET /api/events/concert-1/stock -> 200
      ready: GET /api/orders/activity -> 200
      PASS: stack healthy, dashboard non-root (uid 101), read APIs answering

  ==> Step 5/5: end-to-end through http://localhost:4200
      GET /healthz -> 200 (ok)
      GET /api/events/concert-1/stock -> 200 {"eventId":"concert-1","stock":1000}
      GET /api/orders/activity -> 200 (windowSeconds=10, ordersInWindow=0, orders=0)
      PASS: dashboard serves, stock read works, activity payload is public
      POST /api/orders eventId=concert-1 quantity=1 customerId=web-0ebf60e2-da41-449b-a118-e0da81f85a2c Idempotency-Key=0aca0661-bde8-4e01-8d9c-d6cab3dd8496-dashboard-acceptance
      POST /api/orders -> 202 {"orderId":"98a38be9-b6b4-5beb-9794-ddb5cee2a7ab"}
      polling activity for order 98a38be9-b6b4-5beb-9794-ddb5cee2a7ab to become CONFIRMED (timeout 60s)
      11:39:50 order 98a38be9-b6b4-5beb-9794-ddb5cee2a7ab -> absent from last 20
      11:39:52 order 98a38be9-b6b4-5beb-9794-ddb5cee2a7ab -> absent from last 20
      11:39:54 order 98a38be9-b6b4-5beb-9794-ddb5cee2a7ab -> PENDING
      11:39:56 order 98a38be9-b6b4-5beb-9794-ddb5cee2a7ab -> CONFIRMED
      saga row: orderId=98a38be9-b6b4-5beb-9794-ddb5cee2a7ab eventId=concert-1 quantity=1 status=CONFIRMED amountCents=5000 EUR
      PASS: order 98a38be9-b6b4-5beb-9794-ddb5cee2a7ab CONFIRMED within 60s
      GET /api/events/concert-1/stock -> 200 {"eventId":"concert-1","stock":999}
      PASS: stock decreased by 1 (1000 -> 999)

  ALL M7 ACCEPTANCE CHECKS PASSED
  Dashboard still running at http://localhost:4200
  Stop it with: docker compose -f deploy/compose/docker-compose.yml down
  ```

  The earlier first attempt failed at step 5 with a 502 on the stock read because
  `docker compose --wait` only waits for services that declare a healthcheck and the Java
  services do not; the script now polls both read APIs before asserting. That failure and its log
  are real and were not hidden. The run above is the second, post-fix execution; a third full run
  also passed with the same steps before the screenshot was taken.
- Screenshot of the running page: `docs/design/dashboard/dashboard-running.png` (1440 x 1600 RGB PNG,
  SHA-256 `e2ae9795f2f32b89de3101fa1603d3e8fd690932cb063004b5aa0bbdfacc2992`), captured with
  headless Google Chrome against the live Compose dashboard:

  ```sh
  "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome" --headless=new --disable-gpu \
    --hide-scrollbars --no-first-run --no-default-browser-check \
    --user-data-dir="$HOME/.hermes/profiles/docs-devops/cache/scratch/chrome-m7" \
    --window-size=1440,1600 --virtual-time-budget=4000 \
    --screenshot=docs/design/dashboard/dashboard-running.png http://localhost:4200
  ```

  It shows stock 995, 0.1 orders/sec (1 order in the 10-second window) and four CONFIRMED rows
  with EUR amounts. The exact command, hashes and the states that were not produced live are in
  `docs/design/dashboard/README.md`.
- Backend read contracts are proven by Testcontainers tests, not only by the script: the module
  run above includes `StockEndpointIT` (8 cases: seeded stock, decrease after reservations,
  unknown event 404, 256-character id 400, Redis unchanged before and after) and
  `OrderActivityIT` (11) plus `OrderActivityWindowIT` (7: newest first, limit bounds 0/51 -> 400,
  no customer id or idempotency key in JSON, ten-second window count). The UI idempotency and
  polling behaviour is proven by the dashboard suite: 25 Vitest cases in 3 files, plus the
  dependency-free Node proxy-routing test (1 passing) that checks the loaded Angular proxy config.
- Root `mvn verify` on the whole reactor was run in this card as well, and it is NOT fully green
  in this session. It ended `BUILD FAILURE` after 15:03 min with one failure in the pre-existing
  M4 dead-letter test, not in an M7 path:

  ```text
  [ERROR]   DltE2ETest.AC5_eachConsumerMalformedJsonInvalidTypeAndPoisonReachSamePartitionDLTThenValidSucceeds:141
  Expecting actual:
    [1, 2, 1, 2, 3, 4]
  to contain exactly (and in same order):
    [1, 2, 3, 4]
  but some elements were not expected:
    [1, 2]
  [ERROR] Tests run: 39, Failures: 1, Errors: 0, Skipped: 0
  ```

  Every other module reported SUCCESS. Re-running that single class against the same tree
  (`mvn -B verify -pl e2e-tests -am -Dtest=DltE2ETest -Dsurefire.failIfNoSpecifiedTests=false`,
  exit 0, `BUILD SUCCESS`, 08:19 min) gave `DltE2ETest` 21 tests, zero failures, so the failure
  reproduces as a transient duplicate delivery under at-least-once semantics rather than a code
  defect, but it is not proven flaky by a third run and the full green root run is still owed.
  The M7 backend card had recorded a green root run (239 tests, 46 reports) on the same
  application code before this branch was cut.
- NOT done / unverified: the dashboard has no Helm chart and is not deployed to kind (explicit
  out of scope); there is no SSE, auth or router; `FAILED`, `RESERVED` and `REJECTED` rows were
  only exercised against mocked responses, never produced by a live failure during the capture;
  no mobile, Firefox/Safari or accessibility audit; no performance, throughput or capacity claim,
  and no new database index, so the activity query can scan and sort the order table; the
  acceptance is a single sequential purchase, not the M2 ten-run contention command; the script
  intentionally resets the local Compose database and leaves the stack running. Order count and
  recent rows come from two separate reads and are not one snapshot.

### Milestone 8 - live explanatory dashboard and portfolio polish (branch `milestone-8`, not merged)

**What exists** (committed on `milestone-8`; nothing pushed or merged):

- README restructure, `docs/OPERATIONS.md`, `docs/CHAOS-RESULTS.md`, the short CI workflow, and `docs/DEMO.md` (earlier M8 commits).
- The rebuilt dashboard (`dashboard/`): a system map with one travelling dot per order, four chapters
  (Concurrency, Kafka, Redis, Kubernetes) fed by Prometheus through two GET-only same-origin nginx routes
  (`/api/metrics/query`, `/api/metrics/query_range`), self-hosted Geist fonts, no charting or animation library.
  Spec: `docs/superpowers/specs/2026-11-11-live-dashboard-design.md`; visual rules: `docs/design/DESIGN.md`.
- The dashboard in the Helm chart (Deployment, Service `dashboard:8080`), built and loaded by `scripts/kind-up.sh`
  and `scripts/observability-up.sh`.
- `scripts/live-dashboard-acceptance.sh` (checks LD1 to LD7). `scripts/autoscale-load.js` gained two optional
  env vars (`LOAD_RATE`, `LOAD_DURATION`, defaults unchanged at 8 and 120s) so the acceptance load can run 60 s.
- Records: SPEC.md approved deviation, DESIGN.md decision log (4 rows), dependency table (Geist), M7 route list and
  repo layout, AGENTS.md commands table, DEMO.md kind demo, dashboard/README.md.

**Acceptance command** (kind stack up; `--with-compose` also runs the M7 command and resets the Compose Postgres volume):

```sh
scripts/live-dashboard-acceptance.sh --with-compose
```

Real output of the final passing run of LD1 to LD6, 2026-11-11, after the review fixes (run without
`--with-compose`; `kubectl config current-context` read `docker-desktop` after the run, checked by the controller):

```text
PASS LD1_unit - Tests  93 passed (93); production build ok
PASS LD2_deployed - deploy/dashboard rolled out on kind-ticketrush, /healthz 200 via http://127.0.0.1:56001
PASS LD3_proxy_boundaries - POST query 403; admin/tsdb, metrics/, querymore, actuator 404
PASS LD4_live_queries - all 8 queries succeeded non-empty during 60s at 20 orders/s; ordersRate=12.4; readyPods>=1 for inventory and payment; k6 ok
PASS LD5_reads - stock(concert-1)=999, activity 200
PASS LD6_invariant - 1221 orders (k6 iterations=1201, accepted=1201) settled, none duplicated; load events: confirmed 900 of 900 tickets, remaining+sold=300 each, never negative; concert-1 stock=999 >= 0
SKIP LD7_compose_still_passes - run with --with-compose (resets the Compose Postgres volume)
ALL LIVE DASHBOARD CHECKS PASSED
```

LD7 from the previous run (`--with-compose`, before the review fixes, same kind cluster running), 2026-11-11:

```text
PASS LD7_compose_still_passes - ALL M7 ACCEPTANCE CHECKS PASSED
ALL LIVE DASHBOARD CHECKS PASSED
```

LD6 now also requires: some ticket confirmed across the load events (here 900 of 900, a sell-out, so rejections were exercised),
no repeated Idempotency-Key and no repeated order id among the run's orders, and `concert-1` stock >= 0 (no seed comparison:
the cluster's `concert-1` has history from before the run). The 1221 orders are 1201 k6 iterations plus 20 warm-up purchases.
Why 1201 and not 1200: k6's constant-arrival-rate schedules 1201 arrivals in a 60 s window at 20 per second (both ends
inclusive). One run logged `iterations 1200` with `dropped_iterations 1` (a cold first request), the next logged
`iterations 1201` with none, so 1201 is the full schedule. The script now warms the gateway with 20 purchases before k6
to avoid that dropped first iteration. An interrupted run (SIGTERM) now exits 1 and removes the load keys.

LD7 (Compose) ran after LD1 to LD6 with the kind cluster still running; both fitted in the 16 GB Docker VM.
Checks: LD1 dashboard unit tests and build; LD2 deployed on kind; LD3 nginx boundaries; LD4 all 8 `QUERIES`
(parsed from `dashboard/src/app/metrics.ts`) succeed non-empty while 60 s of k6 load runs, `ordersRate` > 0,
`readyPods` >= 1 for inventory and payment; LD5 stock and activity reads; LD6 load on 3 isolated events of 300 tickets
(sold-out contention) conserves stock per event, creates no duplicate order, and leaves `concert-1` stock >= 0;
LD7 M7 still passes. The script exits 0 only if every check passes and asserts the kubectl current context is unchanged.

**Proof that it can fail.** The Deployment env was changed with
`kubectl --context kind-ticketrush -n ticketrush set env deploy/dashboard PROMETHEUS_ADDR=prometheus-wrong.ticketrush.svc.cluster.local:9090`,
rolled out, and the script run (exit 1, LD4 FAIL):

```text
deployment.apps/dashboard env updated
deployment "dashboard" successfully rolled out
PASS LD1_unit - Tests  92 passed (92); production build ok
PASS LD2_deployed - deploy/dashboard rolled out on kind-ticketrush, /healthz 200 via http://127.0.0.1:51405
PASS LD3_proxy_boundaries - POST query 403; admin/tsdb, metrics/, querymore, actuator 404
FAIL LD4_live_queries - queries failing during load: consumerRate lagByGroup ordersRate partitionLag readyPods redisLatency redisOps sagaOutcomes
PASS LD5_reads - stock(concert-1)=999, activity 200
PASS LD6_invariant - 1200 load orders settled; each load event remaining+sold=300, never negative; concert-1 stock=999, confirmed tickets=1 <= seed 1000
SKIP LD7_compose_still_passes - run with --with-compose (resets the Compose Postgres volume)
LIVE DASHBOARD CHECKS FAILED
exit=1
```

Then the exact original value (`prometheus.ticketrush.svc.cluster.local:9090`, read beforehand) was restored, rolled out
(`prometheus.ticketrush.svc.cluster.local:9090` confirmed), and the script passed (output above). `helm get manifest` shows the
same `PROMETHEUS_ADDR` value as the live Deployment, so the Deployment is as the chart renders it.

Flake seen twice: runs at 30 orders per second (and once at 20, right after a fresh rollout) failed only on the k6
`dropped_iterations` threshold (`k6 exited 99`, the dashboard queries were fine). The default rate was lowered to 20 per
second and a 20-purchase gateway warm-up was added; the cause (slow cold first requests) is a reading, not a measurement.

**Visual check by the controller** in the built-in browser at 1440px on 2026-11-11, on kind: fonts Geist and Geist Mono load
from /fonts, no horizontal scroll, map fits its 1344px region, one Buy click produced an accent dot that crossed the map,
stock 1000 -> 999, confirmed 1, table row CONFIRMED EUR 50.00. 
**Observed under load** by the controller on 2026-11-11 (built-in browser, 1440px, deployed dashboard on kind, k6 at 20 orders
per second for 75 s on a dedicated 500-ticket event typed into the Event id field): stock fell from 500 to 0 in well under a
minute and stayed flat at 0, and a manual Buy afterwards was REJECTED with reason SOLD_OUT; map node rates 13 to 40 per second,
up to 40 dots at once (the cap), red dots for rejected orders; Kafka lag stayed between 0 and 5 messages; EVALSHA about 20
per second at 0.6 ms; ready pods stayed 1 of 2 for both services, no second pod appeared because lag never passed 10; on
Compose, chapters 02 to 04 show "Live metrics need the kind stack." and map rates show dashes. This showed that the saga
counters (confirmed 8601 beside a 500-ticket event) read as an oversell, so they are now labelled "Saga outcomes, all events,
since order-service start". NOT seen: a second pod appearing in the dashboard (expected only with
`M5_CONSUMER_PROCESSING_DELAY_MS=200`), the stale state, reduced motion.

**NOT done / not verified:**

- Owner review, PR and merge of `milestone-8`; nothing pushed.
- `scripts/kind-up.sh` change (dashboard build and load) was only syntax-checked (`bash -n`); it was not run end to end.
  The renamed lowercase `dashboard_tag` variable was not re-run in a bring-up.
- The demo GIF (`docs/design/demo/ticketrush-demo.gif`) and the README screenshot show the previous dashboard; the owner
  must re-record them from the new page.
- The dot animation covers a whole path at once: the saga finishes faster than the 2 s activity poll, so dots show an
  order's path, not live hop-by-hop positions.
- Scaling shows 1 to 2 pods only (KEDA `maxReplicas` 2). With no artificial consumer delay a second pod is not guaranteed,
  so LD4 does not require one; a second pod was not watched in the dashboard.
- `kube-state-metrics` is disabled, so ready pods are scrape targets that are up, not scheduler state.
- The full root `mvn verify` was not re-run for M8; the M7 note about one transient `DltE2ETest` failure still stands.
- No mobile, light-theme, Firefox/Safari or accessibility audit.
- Deferred minor review findings, not fixed:
  - Task 1: `.env.example` does not say the three nginx vars are image-level overrides not read by Compose; two single-line location blocks in `nginx.conf.template` differ from the file's style.
  - Task 2: `metrics.ts` uses `ReturnType<typeof signal<...>>` instead of `WritableSignal`; the 8000 ms read timeout has no test.
  - Task 3: the sliding group is unclipped and draws past the plot's right edge for 600 ms, and the slide plays on first render; the slide step is approximate while the buffer fills; no test for the fewer-than-2-points case and the y regex in `chart.spec` is brittle.
  - Task 4: `.status` gap 6px and padding 4px 10px are off the spacing scale; 13px helper text and 15px rail h2 are outside the DESIGN.md role list; an `&ngsp;` hack in the top bar keeps "stale" a separate word; order id and amount table cells are not mono; `index.html` title is still "Ticket operations".
  - Task 5: the flash effect registers a new `onDestroy` per flash; four helper paragraphs under stock and rate were dropped (visible copy change); figure styles moved from `app.css` to global `styles.css` to stay under the 4 kB component style budget.
  - Task 6: `Dot.id` (`orderId:status`) can repeat if an order leaves and re-enters the activity window; two flashes on one stage within 600 ms cut the second short; the order node is not drawn in `--bad` at rest; names over 17 characters use `textLength` 136 (slightly condensed).
  - Task 7: `build_dashboard` is duplicated across `kind-up.sh` and `observability-up.sh` (matches the existing `build_image` duplication).
