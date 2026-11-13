# M6 test plan

Medium risk. AC1-AC9: checklist order. C=`scripts/chaos.sh`; T=`scripts/chaos-test.sh` (new harness). Reuse M5 fixtures/load, not destructive bootstrap.

Rewrites: AC1 selftest succeeds iff valid evidence passes and corrupt fixtures fail. AC2-AC6: acknowledged IDs equal terminal-or-correlated-DLT IDs; AC2/AC3 require terminal conservation. Recovery: fault-to-resumed-completion AND drained lag within declared timeout. Throughput: completions/second in timestamped before/during/after windows.

AC1 | nominal/edge | unit | T | AC1_validBoundaries | mocks:none; evaluator accepts one sale, stock=0, quantities 1/4, compensated failure, correlated DLT.
AC1 | error | unit | T | AC1_rejectCorruption | mocks:none; reject oversell/negative-stock, stock-leak, lost/substituted-ID, duplicate-order/key/charge, missing-charge, unresolved-order, empty/null/malformed/wrong-type evidence, NaN, zero-duration, missing-recovery, timeout.
AC2 | nominal | e2e | C | AC2_inventoryKill | mocks:none; real kind/k6: inventory UID changes mid-traffic, conservation, nonnegative-stock samples, <=1 charge/order, recovery measured.
AC3 | nominal | e2e | C | AC3_brokerRestart | mocks:none; broker UID changes mid-load; producers/consumers resume, same invariants/recovery.
AC4 | nominal/edge | e2e | C | AC4_declinesRelease | mocks:none; rate=0.3 before audit baseline; successes/declines observed, FAILED reservations released once, no decline-charge, conservation.
AC5 | nominal | e2e | C | AC5_postgresLatency | mocks:none; dependency-free fault measurably delays DB work; terminal/DLT accounting, conservation, throughput/recovery measured.
AC6 | nominal | e2e | C | AC6_rebalanceRamp | mocks:none; both consumers Ready at 1/8, throughput per tested count, conservation, original ScaledObjects restored.
AC6 | error | integration | T | AC6_abortRestores | mocks:none; real-kind interruption/timeout restores replicas/scalers/provider/DB settings; rerun isolates data.
AC7 | nominal | integration | T | AC7_resultsTraceable | mocks:none; README matches every raw run, >=3/experiment, dated throughput/recovery/invariants; measured single-show limit documented.
AC8 | nominal | integration | pom.xml | AC8_regressions | mocks:none; unchanged Maven suite/M3/M5 chart checks pass; report counts.
AC9 | nominal | integration | docs/internal/PROGRESS.md | AC9_honestReport | mocks:none; commands/results/limitations match logs; DESIGN decisions recorded.
AC2 | error | integration | T | AC2_contextRefusal | mocks:none; temporary kubeconfigs: absent/nonproject contexts fail before mutation; only kind-ticketrush permitted.

Run: T; C --selftest; C (>=3 runs/experiment); JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home mvn verify; scripts/base-chart-check.sh; scripts/observability-chart-check.sh.

Not tested: auth/M7/M8/refunds/production-capacity - out-of-scope.
Not tested: exact 30% observed failures - stochastic.
