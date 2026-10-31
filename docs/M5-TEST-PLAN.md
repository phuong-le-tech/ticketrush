# M5 test plan

AC1-AC7: checklist order. Medium risk. Java21/JUnit/AssertJ/Awaitility; reuse ActuatorProbesIT/OrderTestInfra/E2EStack; assert behavior.

Paths: S=`<service>/src/test/java/com/ticketrush/<package>` (all six services); O=order-service's S; E=`e2e-tests/src/test/java/com/ticketrush/e2e`; C=`scripts/m5-chart-check.sh`; A=`scripts/m5-acceptance.sh`.

Metrics: `ticketrush_orders_total`: gateway acknowledgments/retries; `ticketrush_saga_outcomes_total{outcome="CONFIRMED|FAILED|REJECTED"}`: committed transitions; `ticketrush_consumer_processing_seconds_count/sum{consumer,topic}`: consumer attempts. Document exporter lag name/group/topic labels in DESIGN.md before freezing tests. Exclude identifiers/payloads/exception-messages.

AC1 | nominal | integration | pom.xml | AC1_regression | mocks:none; real verify succeeds, >=175 existing tests retained; report additions separately.
AC2 | nominal/edge | unit | S/MetricsTest.java | AC2_timerCountsZeroOneConcurrentAttempts | mocks:clock; real registry yields exact counts, nonnegative duration, bounded tags.
AC2 | nominal/edge | integration | S/PrometheusIT.java | AC2_scrapesBeforeAfterTraffic | mocks:none; real HTTP/containers return 200, service-applicable names before/after one operation; no sensitive labels/values; env endpoint remains unexposed.
AC2 | edge/error | integration | O/SagaMetricsIT.java | AC2_terminalTransitionsExactlyOnce | mocks:clock; real DB/Kafka: confirmation/failure/rejection/timeout increment once; concurrent replay adds zero; rollback adds zero.
AC2 | nominal | integration | E/MetricsE2ETest.java | AC2_lagRisesThenDrains | mocks:none; real broker backlog gives group/topic lag >0 then 0, processing counts rise, accepted counter delta matches acknowledgments.
AC3 | nominal/edge/error | integration | C | AC3_templatesAndMutations | mocks:none; lint/render pass; monitors select endpoints, dashboard has four queries; ScaledObjects target correct groups/topics, min=1, max=12 allowed; reject 0/13/null/string maxima, missing resources, wrong selectors, plaintext password; selftest rejects each mutation.
AC4 | nominal/edge | e2e | A | AC4_cleanAndRepeat | mocks:none; real kind: Ready stack, repeat preserves node/credentials, provisioned dashboard queries each return data.
AC5 | nominal | e2e | A | AC5_scaleAndConserve | mocks:none; real k6: timestamped group/topic/lag/ready-replica samples show 1 -> >1 above threshold -> 1 after drain/cooldown; sold<=stock, unique orders, <=1 charge/order.
AC5 | error | unit | A | AC5_selftestRejectsFalsePass | mocks:none; real evaluator rejects empty/malformed samples, absent/threshold-equal lag, no-up/no-down, timeout, k6/query/invariant failure.
AC6 | error | integration | C | AC6_hygiene | mocks:none; diff-check/dash/secret scans clean; new envs documented; Grafana uses Secret.
AC7 | nominal | e2e | scripts/m3-acceptance.sh | AC7_regression | mocks:none; real acceptance passes or PROGRESS records not-run reason.

Run: `mvn verify`; `scripts/m3-chart-check.sh` and `--selftest`; C and `--selftest`; A and `--selftest`; `scripts/m3-acceptance.sh`.

Rewrites: AC2: service-applicable metrics; AC5: bounded polling/timeouts, nonempty evidence.
Not tested: chaos/auth/refunds - out-of-scope.
