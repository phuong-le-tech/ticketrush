# M5 RED test handoff

Test-only changes. No production classes, dependencies, wiring or skeletons were added.
Every new Java test is tagged `m5-red`; the tag is not excluded by default.

## Commands actually executed

Maven uses a different JDK from shell `java` unless JAVA_HOME is explicit here:

```sh
export JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home
mvn test-compile -pl e2e-tests -am -DskipTests
mvn verify -pl api-gateway,inventory-service,payment-service,fake-payment-provider,order-service,notification-service -am -Dtest=MetricsTest -Dit.test=PrometheusIT,SagaMetricsIT -Dsurefire.failIfNoSpecifiedTests=false -Dfailsafe.failIfNoSpecifiedTests=false -Dmaven.test.failure.ignore=true
mvn verify -pl e2e-tests -am -Dtest=MetricsE2ETest -Dit.test=NoM5Integration -Dsurefire.failIfNoSpecifiedTests=false -Dfailsafe.failIfNoSpecifiedTests=false -Dmaven.test.failure.ignore=true
mvn verify -pl fake-payment-provider -am -Dtest=NoUnitTests -Dsurefire.failIfNoSpecifiedTests=false -Dit.test=PrometheusIT -Dfailsafe.failIfNoSpecifiedTests=false
mvn verify -DexcludedGroups=m5-red
scripts/m5-chart-check.sh
scripts/m5-chart-check.sh --selftest
scripts/m5-acceptance.sh
scripts/m5-acceptance.sh --selftest
scripts/m3-chart-check.sh
scripts/m3-chart-check.sh --selftest
bash -n scripts/m5-chart-check.sh scripts/m5-acceptance.sh
git diff --check
```

The diagnostic RED Maven commands deliberately continue through modules using
`maven.test.failure.ignore=true`: their BUILD SUCCESS/exit 0 is NOT a green test suite.
They report 35 service-test failures plus 3 E2E failures, all assertions, zero errors.
The separate fake-provider PrometheusIT run without failure-ignore exits 1 as required.
Compilation, shell syntax and both M3 chart commands pass. Both M5 offline selftests pass.
Real M5 chart and bootstrap checks exit 1 for missing behavior.

Unfiltered `mvn verify` is the final implementation gate. Do not exclude `m5-red` there.
The existing-only regression's final result is recorded in the task completion handoff.
The initial root run was terminated by the tool's effective 420-second foreground limit
and used Maven's Homebrew JDK25. It is not counted as verification. The explicit-JDK21
regression was launched as a tracked background process to avoid that foreground limit.

## AC mapping and observed RED output

| AC | Test/check names | Observed result |
|---|---|---|
| AC1 | `AC1_regression`: existing root `mvn verify -DexcludedGroups=m5-red` | Existing-only baseline; completion handoff records final result |
| AC2 | Six `PrometheusIT.AC2_scrapesBeforeTraffic` | Each: expected HTTP 200, got 404 |
| AC2 | inventory `MetricsTest.AC2_timerRegisteredAtZero[orders.requested]` | Missing processing timer |
| AC2 | inventory `MetricsTest.AC2_timerRegisteredAtZero[tickets.released]` | Missing processing timer |
| AC2 | inventory `MetricsTest.AC2_timerCountsOneAttempt[orders.requested]` | Missing processing timer |
| AC2 | inventory `MetricsTest.AC2_timerCountsOneAttempt[tickets.released]` | Missing processing timer |
| AC2 | inventory `MetricsTest.AC2_timerCountsConcurrentAttempts[orders.requested]` | Missing processing timer |
| AC2 | inventory `MetricsTest.AC2_timerCountsConcurrentAttempts[tickets.released]` | Missing processing timer |
| AC2 | payment `MetricsTest.AC2_timerRegisteredAtZero` | Missing processing timer |
| AC2 | payment `MetricsTest.AC2_timerCountsOneFailedAttempt` | Missing processing timer |
| AC2 | payment `MetricsTest.AC2_timerCountsConcurrentFailedAttempts` | Missing processing timer |
| AC2 | order `MetricsTest.AC2_timerRegisteredAtZero[orders.requested]` | Missing processing timer |
| AC2 | order `MetricsTest.AC2_timerRegisteredAtZero[payments.result]` | Missing processing timer |
| AC2 | order `MetricsTest.AC2_timerRegisteredAtZero[orders.rejected]` | Missing processing timer |
| AC2 | order `MetricsTest.AC2_timerCountsOneAttempt[orders.requested]` | Missing processing timer |
| AC2 | order `MetricsTest.AC2_timerCountsOneAttempt[payments.result]` | Missing processing timer |
| AC2 | order `MetricsTest.AC2_timerCountsOneAttempt[orders.rejected]` | Missing processing timer |
| AC2 | order `MetricsTest.AC2_timerCountsConcurrentAttempts[orders.requested]` | Missing processing timer |
| AC2 | order `MetricsTest.AC2_timerCountsConcurrentAttempts[payments.result]` | Missing processing timer |
| AC2 | order `MetricsTest.AC2_timerCountsConcurrentAttempts[orders.rejected]` | Missing processing timer |
| AC2 | notification `MetricsTest.AC2_timerRegisteredAtZero` | Missing processing timer |
| AC2 | notification `MetricsTest.AC2_timerCountsOneAttempt` | Missing processing timer |
| AC2 | notification `MetricsTest.AC2_timerCountsConcurrentAttempts` | Missing processing timer |
| AC2 | `SagaMetricsIT.AC2_paymentTerminalTransitionsExactlyOnce[SUCCEEDED,CONFIRMED]` | Missing CONFIRMED saga counter |
| AC2 | `SagaMetricsIT.AC2_paymentTerminalTransitionsExactlyOnce[FAILED,FAILED]` | Missing CONFIRMED saga counter |
| AC2 | `SagaMetricsIT.AC2_rejectionCountsOnceAndConcurrentReplayAddsZero` | Missing CONFIRMED saga counter |
| AC2 | `SagaMetricsIT.AC2_timeoutCountsOnceAndConcurrentReplayAddsZero` | Missing CONFIRMED saga counter |
| AC2 | `SagaMetricsIT.AC2_rollbackLeavesStateAndEveryCounterUnchanged[CONFIRMED]` | Missing CONFIRMED saga counter |
| AC2 | `SagaMetricsIT.AC2_rollbackLeavesStateAndEveryCounterUnchanged[FAILED]` | Missing CONFIRMED saga counter |
| AC2 | `SagaMetricsIT.AC2_rollbackLeavesStateAndEveryCounterUnchanged[REJECTED]` | Missing CONFIRMED saga counter |
| AC2 | `SagaMetricsIT.AC2_rollbackLeavesStateAndEveryCounterUnchanged[TIMEOUT]` | Missing CONFIRMED saga counter |
| AC2 | `MetricsE2ETest.AC2_gatewayCounterMatchesAcknowledgmentsIncludingRetries` | Missing acknowledged-order counter |
| AC2 | `MetricsE2ETest.AC2_lagRisesThenDrainsAndProcessingCountsRise` | Missing inventory processing timer |
| AC2 | `MetricsE2ETest.AC2_httpScrapesAfterTrafficWithoutSensitiveLabels` | Real purchase completes; gateway scrape expected 200, got 404 |
| AC3 | `AC3_templatesAndMutations`: `check_AC3_scaledObjects/serviceMonitors/dashboard` | Missing two ScaledObjects, six ServiceMonitors, dashboard |
| AC4 | `AC4_cleanAndRepeat` | Missing m5-up assertion before cluster mutation |
| AC5 | `AC5_scaleAndConserve`, `AC5_selftestRejectsFalsePass` | Live gate blocked by missing bootstrap; offline evaluator/mutation tests pass |
| AC6 | `AC6_hygiene`, `check_AC6_secrets` | Hygiene clean; Grafana workload missing |
| AC7 | `AC7_regression`: existing M3 chart checks and cluster acceptance command | Chart checks pass; destructive kind acceptance not run, reason in PROGRESS |

Deeper zero/one/concurrent, replay, rollback and lag assertions are behind missing-meter
prerequisites. They compile but are not claimed to have been exercised past those assertions.

## Plan deviations and implementation contracts

- Split the composite timer test into zero/one/concurrent behavior tests. Four consumer services
  have these tests; gateway and fake provider have no Kafka consumer to time. Their applicable
  metrics are covered by HTTP and gateway E2E counter checks, not invented Kafka timers.
- MetricsTest uses real Spring listener beans, registries and infrastructure, not pure unit
  fixtures. There is no instrumentation API to unit-test yet, and this card forbids production
  code. Direct listener invocation with auto-startup disabled isolates exact attempt counts.
  Payment deliberately exercises real failed HTTP attempts; E2E covers successful payment.
- Idle real-configuration HTTP checks live in the six planned PrometheusIT files. After-traffic
  HTTP checks use MetricsE2ETest and an opt-in E2EStack.startMetrics fixture. Existing fixture
  modes are unchanged. E2E deliberately bypasses application.yml, so its opt-in mode exposes
  health/prometheus explicitly; PrometheusIT separately checks real exposure configuration.
- Micrometer names are `ticketrush.orders`, `ticketrush.consumer.processing{consumer,topic}`,
  and `ticketrush.saga.outcomes{outcome}`. Prometheus names follow the planner's contract.
  Pre-register the applicable bounded meter/tag combinations at zero. Saga metrics count
  committed effective transitions, never an uncommitted or rolled-back transaction.
- Do not freeze an undocumented exporter-specific meter in Java. The lag E2E checks real broker
  offsets while the shell acceptance defaults to `kafka_consumergroup_lag{consumergroup,topic}`.
  Its query/service names are configurable via `--config FILE`. Implementation must document
  the actual lag exporter name/labels in DESIGN and supply data to Prometheus before acceptance.
- Scripts implement test validators and mutation fixtures only, not m5-up or k6 production logic.
  Test contracts for the downstream bootstrap/load implementation are in script headers.
  Acceptance uses 24 events to exercise existing partitions without changing topics/keys.
  Offline fixtures are explicitly labeled and cannot be reported as real scaling evidence.
- AC1 and AC7 are existing acceptance commands, not new JUnit methods or POM changes.
  M3 live acceptance is deliberately not run here because it deletes/rebuilds kind; downstream
  must exercise it or retain an explicit limitation. No existing test assertions were weakened.

No skeleton files added. No PR opened. This is not an owner-ready M5 implementation.
