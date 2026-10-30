# M4 RED handoff

Workspace: `.worktrees/feat-t_4e588120`, branch `milestone-4`, approved base `88f15f3`.
No production behavior implemented. Only compile-only SagaReaper and OutboxRelay contracts were added.

## Commands and actual evidence

Use Java 21: `export JAVA_HOME=$(/usr/libexec/java_home -v 21)`.

- `mvn test-compile -DskipTests`: exit 0, all modules compile.
- `mvn verify -DexcludedGroups=m4-red`: BUILD SUCCESS, all 98 existing tests pass. Includes OversellE2ETest (100.8 s), ReplayE2ETest (2 tests, 29.22 s), PurchaseFlowE2ETest (9.174 s). New tests are deliberately excluded only for this regression check; ordinary `mvn verify` includes them.
- `mvn verify -fae -pl inventory-service,fake-payment-provider,order-service -am -Dgroups=m4-red`: expected exit 1. ReleaseIT: 2 missing-consumer assertion timeouts. Provider: 5 invocations, 1 failure, 4 pass. OutboxIT: 3 missing-migration failures. SagaReaperIT: 32 missing-migration failures.
- `mvn -pl e2e-tests -am test -Dtest=DltE2ETest,FailureE2ETest,OutboxE2ETest -Dsurefire.failIfNoSpecifiedTests=false`: expected exit 1. DLT: 21 missing-DLT assertion timeouts. Failure: 8 RED invocations. Outbox: missing-migration failure.
- Final OutputCapture verification: `mvn -pl e2e-tests -am test -Dtest=OutboxE2ETest -Dsurefire.failIfNoSpecifiedTests=false`: expected exit 1, AC4 missing migration, AC9 passes. This proves the actual notification log is visible to the test.
- `scripts/m3-chart-check.sh`: expected exit 1. Helm lint passes; six DLT topics are missing from both Helm and Compose. Existing Kafka, hardening and credential checks report no failures.
- `git diff --check`: exit 0.

Awaitility classifies missing-behavior assertion timeouts as test errors, not JUnit assertion failures. These were real running-service observations, not import, configuration, or container-start failures. No tests are disabled or conditionally skipped.

## AC mapping and short RED output

- AC1 -> FailureE2ETest.AC1_rateOneFailsPaymentAndRestoresStock_AC9_realContainers: expected FAILED, actual CONFIRMED at rate 1.0.
- AC1 -> ChargeControllerIT.AC1_rateZeroAndAbsentSucceedOneFailsWithoutCharge: rate 1.0 expected FAILED, actual SUCCEEDED. Absent and zero variants pass.
- AC1 -> ChargeControllerIT.AC1_successfulRetryPreservesExactChargePayload: absent and zero variants pass, guarding the existing charge contract.
- AC2 -> FailureE2ETest.AC2_oneAndTenConcurrentFailedReplaysRestoreOnce [1,10]: expected FAILED, actual CONFIRMED.
- AC2 -> ReleaseIT.AC2_unknownRejectedAndRepeatedReleasesNeverIncreaseStock [PAYMENT_FAILED,TIMEOUT]: inventory never commits tickets.released.
- AC3 -> FailureE2ETest.AC3_pendingReservedAt120StayAt121FailRestoreOnceConfirmedNeverReaped [PENDING,RESERVED]: orders.outbox missing.
- AC3 -> SagaReaperIT.AC3_reaperDoesNotExpireAtOrBeforeDeadline [119/120 seconds, PENDING/RESERVED]: orders.outbox missing.
- AC3 -> SagaReaperIT.AC3_timeoutFirstLateSuccessAndReplayNeverConfirm [PENDING,RESERVED]: orders.outbox missing.
- AC3 -> SagaReaperIT.AC3_paymentFirstReaperAndFailureNeverOverwriteConfirmation [PENDING,RESERVED]: orders.outbox missing.
- AC3 -> SagaReaperIT.AC3_reaperPaymentRaceProducesOneTerminalOutcomeLateSuccessNeverConfirmsTimeout [12 attempts per state]: orders.outbox missing.
- AC4 -> OutboxIT.AC4_forcedInsertFailureRollsBackStatusAndOutboxForBothEventTypes [SUCCEEDED,FAILED]: orders.outbox missing.
- AC4 -> OutboxIT.AC4_brokerOutageLeavesUnpublishedRecoveryPublishesCorrectKeyTypePayload: orders.outbox missing.
- AC4 -> OutboxE2ETest.AC4_concurrentRelaysAndCrashAfterAckReplayYieldOneNotification: orders.outbox missing.
- AC5 -> DltE2ETest.AC5_eachConsumerMalformedJsonInvalidTypeAndPoisonReachSamePartitionDLTThenValidSucceeds: all seven topic/group routes x three variants lack a same-partition DLT record. Each invocation owns fresh containers so poison never leaks into another case.
- AC6 -> FailureE2ETest.AC6_transient500AndChargedButLostReplyRetrySameKeyOneChargeOneConfirmation [HTTP500,LOST_REPLY]: expected CONFIRMED, actual PAYMENT_FAILED.
- AC7 -> FailureE2ETest.AC7_ratePointThree1000OrdersSettleStockPlusSoldConservedUniqueOrdersCharges: actual 1,000 CONFIRMED and no FAILED at configured 0.3.
- AC8 -> scripts/m3-chart-check.sh/check_AC8_dltTopology: source DLTs missing in both deployments. Regression evidence is the 98-test command above.
- AC9 -> OutboxE2ETest.AC9_realContainersAndNotificationOutputCapture: PASS. All failure/DLT tests also instantiate real Kafka/Postgres/Redis through E2EStack; inventory integration uses real Kafka/Redis and order integration uses real Kafka/Postgres.

## Deviations and implementation notes

- Split the combined AC3 integration test into parameterized deadline, timeout-first, payment-first and race tests. This removes conditional assertions and identifies the failed behavior.
- Expanded provider tests with exact successful retry-payload checks; AC9 adds a passing infrastructure/log-capture test. Existing provider test bodies remain unchanged.
- Tagged only new tests `m4-red`, enabling a clean regression command without suppressing RED tests in the default build.
- Order transition tests call the actual Spring-managed repository with no test transaction, so a missing production transaction cannot be hidden. Test-only triggers force real Postgres failure, and outage tests pause the actual Kafka container.
- Missing-outbox assertions intentionally expose the absent approved migration before SQL can fail with a setup exception. Deeper race, trigger rollback, outage and crash-window bodies have not executed yet. Passing those deeper paths is NOT verified.
- E2EStack provides package-visible context/connection access, provider properties, and DLT topics. Failure helpers explicitly drain releases and unpublished outbox rows; notification crash tests wait for the replay offset before counting side effects.
- AC6 uses a test-only HTTP proxy in front of the real provider. Reflection redirects ProviderClient's captured chargesUri only; retries and charge persistence remain production behavior.
- AC7 uses 1,000 orders, quantities of one, conservation, terminal settlement and per-order charge checks. There is no approved seeded production randomness hook, so the mixed-outcome guard remains stochastic; it does not assert an exact 30 percent count. This is a known determinism limitation, not a seeded-randomness claim.
- DLT timing observes DefaultErrorHandler retry listeners and successful record interceptors without supplying a handler. Deserialization and poison exceptions must receive all four attempts per the approved decision. Backoff checks allow small scheduling tolerance.
- Existing OrderSagaIT/PaymentFlowIT expect the old PAYMENT_FAILED/no-retry behavior. They are unchanged and currently pass. The code-writer must reconcile these legacy assertions with the explicitly approved FAILED/retry semantics, not silently discard coverage.
- For manual relay/clock tests, keep scheduled production jobs from competing with test-controlled ticks. No test production wiring was added for this.
- `scripts/m2-acceptance.sh` x10 and `scripts/m3-acceptance.sh` were not run in this RED step. M2 contention/replay passed once in the full 98-test regression. Run all ten M2 repetitions and the cluster acceptance after implementation; do not treat prior milestone evidence as current verification.
- Chart mutation selftests cannot reach their mutation phase until the unmutated deployment includes DLTs. That phase remains unverified.
- Two preliminary regression attempts used incomplete Surefire/Failsafe filters and correctly ran new RED tests; a third duplicated integration tests and hit the terminal timeout. The final tag-based regression command completed successfully.
- A delegated first draft was written in the dispatcher worktree despite the supplied feature path. Draft files were copied into the approved feature workspace and all final evidence above comes from that feature workspace. Uncommitted drafts in the dispatcher workspace are not the deliverable.

## Compile-only skeletons

- `order-service/src/main/java/com/ticketrush/order/SagaReaper.java`: constructor `(OrderRepository, Clock)`, method `reapExpired()`.
- `order-service/src/main/java/com/ticketrush/order/OutboxRelay.java`: constructor `(JdbcClient, KafkaTemplate<String,Object>)`, method `publishPending()`.

Every skeleton constructor/method throws UnsupportedOperationException. No annotations, scheduling, transaction logic, migration or runtime wiring was added. No new dependencies.
