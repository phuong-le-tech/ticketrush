# M4 order-service RED test contracts

- Scope: new order-service tests and the fake-provider AC1 tests only. No production behavior, dependencies, migrations, or configuration changed in this review.
- Existing compile-only APIs: `SagaReaper(OrderRepository, Clock).reapExpired()` and `OutboxRelay(JdbcClient, KafkaTemplate<String, Object>).publishPending()`. These skeletons throw `UnsupportedOperationException` and have no Spring wiring.
- The repository's real `onPayment` transition must commit its status change and outbox insert atomically. No test-created transaction wraps the call, so a missing production transaction cannot be hidden by a test transaction.
- AC3 uses parameterized PENDING/RESERVED fixtures. Separate methods cover the 119/120 second non-expired boundary, timeout-first with late success/replay, payment-first, and simultaneous real DB transitions. The default 120 second timeout is strictly older-than, not inclusive.
- The AC3 race has 12 named attempts per initial state. It asserts one correlated allowed tuple of terminal status, failure reason, topic, key, logical type, and payload reason. Independent allowed-value assertions would incorrectly permit mixed outcomes. Later success/reaping must leave both the durable order and event unchanged.
- AC4 forced insertion failure uses a PostgreSQL trigger scoped to the order UUID and cleans it up in `finally`. The parameter table supplies SUCCEEDED/FAILED, opposite replay status, expected terminal state, topic, type, and key. The recovery/replay checks use the same unconditional assertions for both cases.
- AC4 broker outage pauses/unpauses the actual Testcontainers Kafka broker with cleanup in `finally`, using short real producer timeouts. It checks unpublished rows during outage and wire key, logical type header, full JSON payload, and publication marks after recovery. Delivery can be at-least-once across an ambiguous acknowledgment.
- Kafka topic/partition setup, poll filtering, and transport exception capture live in infrastructure helpers. Test bodies have no loops or conditional assertion selection. Outbox payload assertions use complete expected field maps, not a status branch.
- Fake-provider AC1 uses a rate/status/count/charge-id expectation table for absent, 0.0, and 1.0. Each attempt checks its exact expected status and stored-charge count. A separate parameterized success test checks exact retry bodies and the stored payload. Existing old test bodies are unchanged.
- Transition/race tests call the real Spring-managed repository directly to expose database atomicity without acknowledgment stubs. The relay test exercises real Kafka. Existing OrderSagaIT owns listener ingestion coverage.
- Behavioral limitation: the current V1 schema lacks orders.outbox. The explicit missing-migration assertion blocks race, forced-trigger, and outage bodies until production implements M4. No substitute schema is created by tests.
- Existing OrderSagaIT still names the M1 failure status PAYMENT_FAILED. The M4 tests expect FAILED; old tests were deliberately not rewritten.

## Verification in the approved workspace

Workspace: `/Users/phuong/Documents/Tech/GitHub/ticketrush/.worktrees/feat-t_4e588120`.

With `JAVA_HOME=$(/usr/libexec/java_home -v 21)`:

- `mvn -f /Users/phuong/Documents/Tech/GitHub/ticketrush/.worktrees/feat-t_4e588120/pom.xml -pl order-service,fake-payment-provider -am test-compile -DskipTests`: BUILD SUCCESS, exit 0. Both changed test modules compiled with Java 21.
- `git diff --check`: exit 0.
- A read-only source check confirmed all 8 new AC test methods contain no loops, if/switch, or ternaries. It also confirmed the old ChargeControllerIT test bodies are byte-identical to HEAD and tracked old order-service tests have no diff.
- Earlier verification claims from a different worktree are not evidence for this workspace and have been removed.
- These compile-only review notes are superseded by the final executed evidence and full AC mapping in `../docs/M4-TEST-HANDOFF.md`. Module RED tests were subsequently run in this workspace; all missing-migration assertions failed as expected.
