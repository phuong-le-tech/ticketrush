# M6 RED test handoff

## Scope and commands

Only `scripts/m6-chaos-test.sh` is executable test code. No production runner,
application logic, skeleton, dependency, chart, Java test or protected instruction
file was added or changed. Synthetic fixtures are embedded in the test harness and
written only to temporary directories. They are never real chaos evidence.

```sh
scripts/m6-chaos-test.sh
scripts/m6-chaos-test.sh --offline
scripts/m6-chaos-test.sh --results docs/evidence/m6
scripts/m6-chaos-test.sh --regressions
JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home mvn verify
scripts/m3-chart-check.sh
scripts/m5-chart-check.sh
bash -n scripts/m6-chaos-test.sh
```

Default harness mode is safe: it calls only the runner's offline evaluator,
selftest and real CLI with temporary non-project kubeconfigs. It never bootstraps,
kills pods, changes replicas, or generates purported real run output. The real
runner must refuse these contexts before tool discovery, connections or mutation.
The harness does not install a fake kubectl or substitute synthetic live evidence.
`--regressions` opts into the existing Maven and chart checks, without changing
`pom.xml`; it can take about 20 minutes.

## AC mapping and observed RED reasons

Each line below is a test method, prefixed with `test_` in the harness.

| AC | Test name | RED reason |
|---|---|---|
| AC1 | AC1_validBoundaries | M6 runner missing; valid quantity 1/4, zero remaining stock, compensated failure and correlated DLT fixtures cannot yet be evaluated |
| AC1 | AC1_rejectCorruption | M6 runner missing; corruption mutations cannot yet reach invariant assertions |
| AC1 | AC1_rejectMalformedEvidence | M6 runner missing; parser rejection cannot yet be exercised |
| AC1 | AC1_rejectDuplicateIdempotencyTuple | M6 runner missing |
| AC1 | AC1_selftestIsOffline | M6 runner missing |
| AC2 | AC2_contextRefusal | M6 runner missing; absence/nonproject/production context refusal cannot yet reach the guard |
| AC2 | AC2_inventoryKillRequiresTerminalConservation | M6 runner missing; correlated DLT alone must not pass this experiment |
| AC2 | AC2_inventoryKill | No measured run evidence |
| AC3 | AC3_brokerRestartRequiresChangedUID | M6 runner missing |
| AC3 | AC3_brokerRestartRequiresTerminalConservation | M6 runner missing; correlated DLT alone must not pass this experiment |
| AC3 | AC3_brokerRestart | No measured run evidence |
| AC4 | AC4_declinesRelease | M6 runner missing |
| AC4 | AC4_declinesCannotBeCharged | M6 runner missing |
| AC4 | AC4_failureRateAndBaseline | M6 runner missing |
| AC4 | AC4_requiresObservedSuccessAndDecline | M6 runner missing; all-success and all-decline runs must not pass |
| AC4 | AC4_realDeclinesRelease | No measured run evidence |
| AC5 | AC5_postgresLatency | M6 runner missing |
| AC5 | AC5_DLTRequiresAcknowledgedOrderCorrelation | M6 runner missing |
| AC5 | AC5_realPostgresLatency | No measured run evidence |
| AC6 | AC6_rebalanceRamp | M6 runner missing |
| AC6 | AC6_realRebalanceRamp | No measured run evidence |
| AC6 | AC6_abortRestores | Missing live SIGINT/timeout cleanup evidence |
| AC7 | AC7_resultsTraceable | No measured run evidence or README links/results |
| AC8 | AC8_regressions | Opt-in method skipped in the default harness; actual commands run separately in this phase |
| AC9 | AC9_honestReport | No M6 PROGRESS section |

The public runner absence is an explicit assertion failure, not a failed shell
import, missing command invocation, test setup error, or falsely passing rejection
test. Once it exists, the public CLI is exercised for each fixture. **The deeper
invariant assertions have not yet failed meaningfully against an implemented
evaluator.** They must be exercised again in GREEN; no stronger RED claim is made.

## Test-only interface contract

This is a reversible local testing interface, following M5's `--evaluate FILE`
pattern, not a public API, event, topic, key, schema or deployment-default change.
The plan did not choose an evidence serialization. Read `fixture()`,
`valid_cases()` and the mutation tables in the harness for exact synthetic inputs.

- `scripts/m6-chaos.sh --evaluate FILE`: offline, exit 0 and a PASS diagnostic for
  valid evidence; exit 1 and a FAIL diagnostic for corrupt/malformed evidence,
  without a Python traceback. It must not need kubectl, k6 or a running cluster.
- `scripts/m6-chaos.sh --selftest`: succeeds with an empty kubeconfig, after
  accepting valid fixtures and rejecting corrupt fixtures.
- No-argument live invocation must reject absent or nonproject contexts with exit
  1 and a diagnostic naming `kind-ticketrush`, without switching kubeconfig.
  Positive invocation on the allowed context is downstream live-run work.
- Evidence v1 extends the M5 stock/order/charge layout with distinct
  `k6.accepted_order_ids`, correlated `invariants.dlt`, `fault`, `recovery`,
  nonnegative stock/lag `samples`, and timestamped completion-rate windows.
  Do not substitute aggregate count equality for acknowledged ID set equality.
- `recovery.seconds` is fault-to-drained-lag time. `resumed_at` independently
  proves completed-order progress after the fault. Both must fit the declared
  run timeout. Window rate equals completions / positive elapsed seconds.
- Inventory/broker experiments need changed pod UIDs during load and full terminal
  conservation. Postgres must demonstrate greater query latency than baseline.
  Payment rate must be 0.3, applied before establishing the stable provider audit
  baseline; observed failures need not be exactly 30%. FAILED orders must release
  reserved quantity once and have zero charges. `release_count` means effective
  release state, not the number of delivered at-least-once release messages.
- Ramp samples cover both inventory/payment consumers at Ready counts 1 and 8,
  with throughput at each tested count. ScaledObject snapshots must match after
  the experiment. No permanent change to M5 max=2.

## Persisted real evidence contract

The harness audits, but does not manufacture or collect, live evidence.

- `docs/evidence/m6/run-*.json`: common evaluator fields plus `source: "live"`,
  `context: "kind-ticketrush"`, unique `run_id`, `date`. Experiment slugs:
  `inventory-kill`, `broker-restart`, `payment-declines`, `postgres-latency`,
  `consumer-ramp`. At least three files for each experiment; audit every file.
- README must have exactly one table row linking each raw run, with its experiment,
  date, PASS invariant outcome, recovery seconds and before/during/after completion
  rates. Numeric string checks currently require the raw values, not rounded
  approximations. Include bottlenecks and the single-show/one-partition limitation.
- `bottleneck.json`: `source: "live"`, `event_count: 1`, and two `measurements`
  with `ready_inventory_replicas` 1 and 8 respectively, `started_at`, `finished_at`,
  `completions`, `orders_per_second`. README links the file and reports both rates.
  These are additional real single-show measurements, not inferred from the
  multi-show consumer-ramp results.
- `verification.json`: `commands` mapping the five commands listed in AC9's test
  to `{exit_code: 0, log: "repo-relative-log-path", summary: "exact output line"}`.
  The referenced log must exist; PROGRESS must contain each command and its exact
  summary line, and record limitations. Review checks real test counts too.
- `cleanup-SIGINT.json` and `cleanup-timeout.json`: `source`, `context`, `cause`,
  `before`, `during`, `after`, `first_run_order_ids`, `rerun_order_ids`.
  Each state snapshot contains `replicas`, `scaledobjects`, `provider_env`,
  `db_settings`. Snapshot effective relevant settings, omit resourceVersion and
  timestamps, never credentials. Abort must happen after a real mutation;
  post-cleanup settings equal baseline, and rerun IDs are nonempty and disjoint.
  `rerun_evidence` links a repo-relative real run file whose acknowledged IDs match
  `rerun_order_ids`; that completed rerun must pass the shared invariant evaluator.
  Implementation must actually interrupt/time out a mutated run and collect
  these snapshots, not populate them with invented values.

## Plan deviations and limits

1. All new executable assertions live in T, not C/pom.xml/PROGRESS.md. C is the
   production runner owned by code-writer; changing Maven tests or writing
   production behavior would violate this card. AC2-AC6 audit persisted live
   evidence; the writer must run the real fault experiments and cleanup probes.
2. The evidence/CLI shape above fills the plan's unspecified reversible test seam.
   No irreversible project or product decision was made.
3. No real chaos/cleanup run, positive allowed-context invocation, single-show
   benchmark, measured README table, or M6 acceptance is claimed here. File and
   field checks cannot independently prove that a collector used real data.
   Independent review must inspect raw kubectl/k6/DB/Redis/provider logs and match
   their timings, outcomes, metrics and docs. AC9 text-presence checks do not
   establish the truth of every report statement.
4. Context tests use real temporary kubeconfigs targeting an unreachable local
   endpoint, not a mocked kubectl. They assert refusal diagnostics and unchanged
   kubeconfig; they cannot prove every possible absence of external mutation.
5. M5 load currently exports aggregate acknowledgments only. Collect distinct
   acknowledged IDs without changing its default behavior or any service API.
   M5 provider charge storage is in memory: rollout the failure-rate config
   before the audit baseline and fail audits if the provider restarts thereafter.
6. No production skeleton files were added.

## Verification

- Final default harness execution: 25 test methods, 69 assertion failures
  including parameterized subtests, 1 opt-in regression skip, 0 errors. Offline
  selection: 60 assertion failures, 9 skips, 0 errors. Both commands exit 1 as RED.
- `bash -n scripts/m6-chaos-test.sh`: exit 0. Running the harness also compiles
  and executes the embedded Python, with unittest assertion failures, no errors.
- Both M3 and M5 chart checks pass, Helm lint included. Their `--selftest` modes
  and `scripts/m5-acceptance.sh --selftest` also pass (offline evaluator evidence).
- First Java 21 root `mvn verify`: BUILD FAILURE in 02:44. Existing
  `OutboxIT.AC4_forcedInsertFailureRollsBackStatusAndOutboxForBothEventTypes`
  observed the scheduled relay changing only `publishedAt` from null to a
  timestamp between snapshots. No Java test or implementation was changed.
- Second unchanged Java 21 root `mvn verify`: BUILD FAILURE in 02:50, after
  order-service passed. Notification `MetricsTest` initialization failed to connect
  to Testcontainers Ryuk at localhost:49565 (3 errors); E2E was not reached.
- Third unchanged full root run: **BUILD SUCCESS in 15:23**, finished
  2026-11-04T10:27:20+01:00, **213 tests, 0 failures, 0 errors, 0 skipped**
  from 43 Surefire/Failsafe XML report files. All Java tests remained unchanged.
  Includes contention/replay, all 21 DLT cases, failure, metrics and outbox E2E.
  Earlier failures remain disclosed above; they were not fixed or hidden.
- Local raw logs: `target/m6-red/tests.log`, `target/m6-red/offline.log`,
  `target/m6-red/maven.log`, `target/m6-red/maven-rerun.log`,
  `target/m6-red/maven-final.log`. Maven logs are ignored build artifacts, not
  manufactured milestone evidence.
