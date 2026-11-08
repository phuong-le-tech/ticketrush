# TicketRush

A flash-sale ticketing platform built with Java 21, Spring Boot, Kafka (KRaft, Strimzi),
PostgreSQL, Redis and Kubernetes. The invariant is no oversell, duplicate order or duplicate charge.

**Status:** M6 is partially implemented and blocked on local kind/Docker runtime recovery. It is
not accepted. See [verification and limitations](docs/PROGRESS.md),
[spec](docs/SPEC.md) and [design decisions](docs/DESIGN.md). M7-M8 are not started.

## Build and test

Requires Java 21, Maven and Docker for Testcontainers:

```sh
export JAVA_HOME=$(/usr/libexec/java_home -v 21) # macOS; use your JDK 21 path elsewhere
mvn verify
scripts/m5-chart-check.sh
scripts/m5-chart-check.sh --selftest
scripts/m5-acceptance.sh --selftest
```

There is no separate Java lint/typecheck command; Maven compiles all modules. Offline selftests
exercise the test validators, not a cluster and not autoscaling.

## M5: metrics, Grafana and autoscaling on kind

Requires running Docker, kind 0.33.0, Helm 4, kubectl, Python 3, jq, mikefarah yq v4 and k6 2.3.0.
On macOS install the command-line tools with `brew install kind helm kubectl jq yq k6`.
The bootstrap pins Kubernetes 1.36.4 because KEDA 2.21 is tested through 1.36, not kind's
newer default. Operator charts and container images need network access on a cold start.
Allow at least 8 GB Docker memory; concurrent JVM tests and image builds increase pressure.

```sh
scripts/m5-up.sh
# Repeat is safe: same node, content-tagged app images and Grafana password.
scripts/m5-up.sh
kubectl --context kind-ticketrush -n ticketrush get pods,scaledobjects,hpa
```

Normal consumers have no artificial delay. For the reproducible load demonstration use:

```sh
# DELETES and rebuilds the kind cluster "ticketrush", including its data.
M5_CONSUMER_PROCESSING_DELAY_MS=200 scripts/m5-acceptance.sh
```

Acceptance bootstraps twice, seeds 24 unique events (500 tickets each), runs k6 at 8 acknowledged
orders/second for 120 seconds, samples lag/ready replicas with epoch timestamps, and requires both
inventory and payment to scale 1 -> more than 1 -> 1. It then checks every accepted order settled,
unique order/idempotency tuples, exact per-event stock conservation, and exactly one provider
charge for each confirmed order (zero for other outcomes). It also checks the provider did not
restart and lose its in-memory charge audit. See the printed evidence directory for `samples.jsonl`,
`evidence.json`, `k6.log` and the summary. A timeout, missing signal or failed invariant fails the run.
The optional `--config FILE` accepts the documented bounds/service overrides in the script.

The demonstration delay slows inventory and payment only and is off by default. It does not change
Kafka keys or partitions. Single-show inventory is still serial; the 24 shows exercise the existing
12 partitions. If load exceeds the saga timeout, late payments can require refunds, an existing
M4 limitation. Do not treat this laptop demonstration as a production capacity guarantee.
Afterward disable the artificial delay with `scripts/m5-up.sh` (without the delay variable).

### Open Grafana

Run these in separate terminals:

```sh
kubectl --context kind-ticketrush -n ticketrush port-forward svc/grafana 3000:80 --address=127.0.0.1
kubectl --context kind-ticketrush -n ticketrush port-forward svc/prometheus 9090:9090 --address=127.0.0.1
kubectl --context kind-ticketrush -n ticketrush get pods -w
```

Visit <http://localhost:3000/d/ticketrush-m5> with user `admin`. Retrieve the generated password
locally (do not paste it into issues, logs or screenshots):

```sh
kubectl --context kind-ticketrush -n ticketrush get secret ticketrush-grafana \
  -o jsonpath='{.data.admin-password}' | base64 --decode
```

The ConfigMap-provisioned dashboard has four areas:

- Consumer lag by consumer group/topic, from the Strimzi Kafka exporter.
- Average processing seconds by consumer/topic, including failed/retried attempts.
- Acknowledged POSTs/second, including acknowledged idempotent retries, not unique order creation.
- Committed saga outcomes, with timeout included in FAILED. Replays and rollbacks add zero.

All six services expose `/actuator/prometheus` on their normal HTTP port. Only health and
Prometheus actuator endpoints are exposed. Metric labels never contain customer, event or order IDs.
Empty traffic means zero counters and no meaningful processing mean yet; run acceptance for data.
Prometheus has ephemeral two-hour retention; Grafana storage is ephemeral, but provisioning and
its Secret survive normal Helm upgrades. No public ingress or authentication for Prometheus is
provided. Keep port-forwards bound to localhost and do not deploy these local defaults publicly.

## Previous milestone bootstrap

`scripts/m3-up.sh` explicitly disables M5 resources and still requires only Strimzi, not KEDA or
Prometheus CRDs. `scripts/m3-acceptance.sh` is destructive and replaces the cluster; do not run it
while watching an M5 load test. The M5 bootstrap reuses the M3 image build/load and app chart
pattern, adding pinned observability operators before the chart install.

Full portfolio polish and the Angular UI remain future milestones.

## M6: project-local chaos experiments

Reuse the M5 stack with no artificial consumer delay. This runner never bootstraps or deletes a
cluster and never switches contexts. Select the intended context explicitly before running it:

```sh
scripts/m5-up.sh
kubectl config use-context kind-ticketrush
scripts/m6-chaos.sh --selftest             # offline; no kubectl/k6 required
scripts/m6-chaos-test.sh --offline         # unchanged test-writer evaluator contract
python3 scripts/m6-collector-test.py       # offline collector cleanup/freshness regressions
scripts/m6-chaos.sh                       # 3 runs of every fault, cleanup probes, single-show comparison
scripts/m6-chaos.sh --resume              # re-check prior live runs; collect missing repetitions only
# Or a bounded subset:
scripts/m6-chaos.sh --experiment broker-restart --runs 1
scripts/m6-chaos.sh --evaluate docs/evidence/m6/run-<run-id>.json
```

Faults target only context `kind-ticketrush`, namespace `ticketrush`. Every live kubectl call pins
both. SIGINT, SIGTERM and the per-run deadline attempt to restore effective settings; SIGKILL or
a host crash cannot run cleanup. A stalled runtime can also prevent reconciliation. The runner
persists before/after settings, command output, sampled stock/lag,
k6 response IDs and provider charge audits under `docs/evidence/m6/`. The committed verbatim
runtime logs are in [raw-logs.zip](docs/evidence/m6/raw-logs.zip), with a byte-count/SHA256
[index](docs/evidence/m6/raw-logs-index.json); JSON/JSONL measurements are also stored directly.
The ZIP is a snapshot of the first partial execution, not an automatically refreshed log sink.
The completed retry's [raw archive](docs/evidence/m6/retry-raw-logs.zip) and
[byte/SHA256 index](docs/evidence/m6/retry-raw-logs-index.json) preserve its actual runtime and
Maven output; all archive members were compared byte-for-byte with the original local files.
Never run concurrent chaos runners or bootstrap/acceptance scripts against the same cluster.
Python uses only its standard library, and all operators/images are the existing M5 pins.

Inventory and broker faults use normal `kubectl delete pod` (graceful termination), not a hard
process or node power loss. Postgres latency is a four-second `ACCESS EXCLUSIVE` lock on the
existing orders table in a bounded transaction, not network latency or a permanent DB change.
The provider is configured to 0.3 before its audit baseline, stays there throughout load, and is
restored only after reading its in-memory ledger. Its post-restoration window is deliberately
idle (0/s), not evidence of post-restoration capacity.

The ramp attempts inventory 1 -> 8 -> 1, then payment 1 -> 8, independently, not 8+8 simultaneously.
It readies one new pod at a time and waits for terminating pods before starting the next phase.
After Phuong increased the Docker VM from 8GB to 16GB on 2026-11-08, the runner stopped applying
artificially small heaps and now retains the chart's 384Mi memory requests, 512Mi limits and
`-XX:+UseSerialGC -XX:MaxRAMPercentage=60`. No M5 defaults are changed. The earlier 256Mi pods
were observed OOMKilled after Docker recovery; the 384Mi / 96Mi-heap incremental retry on the
8GB VM still timed out at four Ready / five desired inventory replicas. Those failed measurements
are preserved separately, not pooled with the larger-VM runs.

The collector restores resources, JVM options, replicas and full ScaledObject settings on normal
completion. The reused M5 purchase flow keeps its strict thresholds and 8/s offered rate. M6
preallocates 64 VUs, uses opt-in response-ID-only logs, and allows 360s for the incremental
sequential ramp (120s otherwise). Six CPUs remain unchanged; the memory increase is not a CPU
capacity increase or evidence of simultaneous 16-consumer performance.

### Measurement definitions

- Throughput is the number of terminal order transitions in the SQL `updated_at` window divided
  by the actual elapsed seconds, including CONFIRMED, FAILED and REJECTED. It is end-to-end
  outcome throughput, not a per-consumer processing rate or maximum service capacity.
- Recovery is a conservative fault-to-post-load-drained-lag/terminal-conservation interval.
  It includes the remaining offered load and polling delay, rather than isolating minimum MTTR.
  `resumed_at` separately records observed terminal progress after the fault. Provider-decline
  recovery also includes ledger collection and the restoration rollout.
- Exact acknowledged ID set equality, unique order IDs/idempotency tuples, per-event stock
  conservation and per-order charge counts are required. Failed orders must release held stock
  exactly once. A provider restart before its audit fails the run instead of erasing charges.
- Stock samples contain every isolated event's remaining stock. Lag is the existing exporter
  group/topic sum, which omits unknown offset partitions as documented in M5. Final SQL
  conservation is independently required. Timing is laptop/polling dependent.

### Measured results

Completed real audits: inventory-kill **6**, broker-restart **3**, payment-declines **3**,
Postgres-latency **3**, consumer-ramp **4** (three 24-show repetitions plus one single-show
comparison). The inventory count includes the two independently audited post-abort reruns.
All 19 completed runs are listed separately below. The original 13 audits were measured on
2026-11-04; the six new audits ran after the owner increased the VM to 16GB on 2026-11-08.
These environments are not pooled into an average or a capacity claim.

| Experiment | Fault | Invariants | Recovery seconds | Before / during / after terminal outcomes per second | Run | Date |
|---|---|---|---|---|---|---|
| broker-restart | one broker pod deletion | PASS | 99.83673596382141 | 7.958116877157418 / 8.000770827397252 / 7.722169192357605 | [raw run](docs/evidence/m6/run-m6-broker-restart-1793786582291791000.json) | 2026-11-04 |
| broker-restart | one broker pod deletion | PASS | 98.51445984840393 | 7.996811506235794 / 8.009661458195517 / 7.888682976823053 | [raw run](docs/evidence/m6/run-m6-broker-restart-1793786713567421000.json) | 2026-11-04 |
| broker-restart | one broker pod deletion | PASS | 100.24780082702637 | 7.985144099595496 / 8.031143871297072 / 7.624561639273277 | [raw run](docs/evidence/m6/run-m6-broker-restart-1793786843651282000.json) | 2026-11-04 |
| inventory-kill | inventory pod deletion | PASS | 100.40090799331665 | 7.9184490690696405 / 0.3240031897120214 / 11.558675019426442 | [raw run](docs/evidence/m6/run-m6-inventory-kill-1793785612760753000.json) | 2026-11-04 |
| inventory-kill | inventory pod deletion | PASS | 99.89213395118713 | 7.930233962827798 / 0.5882444927637638 / 9.406048223503381 | [raw run](docs/evidence/m6/run-m6-inventory-kill-1793786175805766000.json) | 2026-11-04 |
| inventory-kill | inventory pod deletion | PASS | 100.33470177650452 | 7.742950858409198 / 1.7506504898826114 / 8.711014379619941 | [raw run](docs/evidence/m6/run-m6-inventory-kill-1793786312088218000.json) | 2026-11-04 |
| inventory-kill | inventory pod deletion | PASS | 102.9268319606781 | 7.9168481492571905 / 0.5124147419368179 / 8.48208687963013 | [raw run](docs/evidence/m6/run-m6-inventory-kill-1793786446569085000.json) | 2026-11-04 |
| payment-declines | provider rate 0.3 | PASS | 114.43596386909485 | 7.973964284086212 / 7.001573147130719 / 0.0 | [raw run](docs/evidence/m6/run-m6-payment-declines-1793787117900791000.json) | 2026-11-04 |
| payment-declines | provider rate 0.3 | PASS | 114.30489230155945 | 7.964195831918248 / 6.998121269746247 / 0.0 | [raw run](docs/evidence/m6/run-m6-payment-declines-1793787407150655000.json) | 2026-11-04 |
| payment-declines | provider rate 0.3 | PASS | 118.12455892562866 | 7.946810343703649 / 6.98383440195554 / 0.0 | [raw run](docs/evidence/m6/run-m6-payment-declines-1793787555934552000.json) | 2026-11-04 |
| postgres-latency | 4s table lock | PASS | 99.0099709033966 | 7.968758703143042 / 1.123929637778963 / 8.146084871587771 | [raw run](docs/evidence/m6/run-m6-postgres-latency-1793787705384289000.json) | 2026-11-04 |
| postgres-latency | 4s table lock | PASS | 100.20807814598083 | 7.968703131968767 / 2.519447882597236 / 8.004480087882593 | [raw run](docs/evidence/m6/run-m6-postgres-latency-1793787838420505000.json) | 2026-11-04 |
| postgres-latency | 4s table lock | PASS | 101.07388591766357 | 7.984156179217333 / 1.5774957246344925 / 8.027990817777965 | [raw run](docs/evidence/m6/run-m6-postgres-latency-1793787970340420000.json) | 2026-11-04 |
| consumer-ramp | staged inventory/payment 1->8, then restore | PASS | 340.56781005859375 | 7.935919222191438 / 8.003876473464302 / 7.906196245522607 | [raw run](docs/evidence/m6/run-m6-consumer-ramp-1794135562105823000.json) | 2026-11-08 |
| consumer-ramp | staged inventory/payment 1->8, then restore | PASS | 339.5589737892151 | 7.984634497280194 / 7.995837685399253 / 7.940884004802306 | [raw run](docs/evidence/m6/run-m6-consumer-ramp-1794135942702742000.json) | 2026-11-08 |
| consumer-ramp | staged inventory/payment 1->8, then restore | PASS | 340.2294728755951 | 7.9776487615252805 / 7.995914077496762 / 7.865059752674633 | [raw run](docs/evidence/m6/run-m6-consumer-ramp-1794136322769364000.json) | 2026-11-08 |
| consumer-ramp | single-show staged inventory/payment 1->8 | PASS | 345.0931408405304 | 7.999579402694281 / 7.996894881265104 / 7.72771416400147 | [raw run](docs/evidence/m6/run-m6-consumer-ramp-1794137002893522000.json) | 2026-11-08 |
| inventory-kill | post-SIGINT inventory pod deletion | PASS | 98.6995952129364 | 7.9742217093620225 / 0.4918175496175186 / 8.937379497914712 | [raw run](docs/evidence/m6/run-m6-inventory-kill-1794136718792903000.json) | 2026-11-08 |
| inventory-kill | post-timeout inventory pod deletion | PASS | 99.85872197151184 | 7.998282194608636 / 0.2832771288938411 / 8.75041284157138 | [raw run](docs/evidence/m6/run-m6-inventory-kill-1794136871523725000.json) | 2026-11-08 |
| consumer-ramp | first inventory rollout toward 8 | NOT VERIFIED | not measured | no valid windows | [failed attempt](docs/evidence/m6/m6-consumer-ramp-1793788102587266000/) | 2026-11-04 |
| consumer-ramp | incremental retry, stopped at 4 Ready / 5 desired | NOT VERIFIED | not measured | no valid windows | [failed retry](docs/evidence/m6/m6-consumer-ramp-1794134769312914000/) | 2026-11-08 |

Payment outcomes, in the listed run order: 671 confirmed / 290 failed, 672 / 288, and 667 / 294.
Each confirmed order had one charge; each failed order had zero charges and its reservation was
released once. The first payment audit was initially rejected by a collector-only reason mismatch
(PROVIDER_DECLINED in SQL versus PAYMENT_FAILED in the synthetic release-reason fixture). The
unchanged measured JSON passed after a test-first evaluator correction and is counted once above.

The standard 24-show ramp repetitions each acknowledged 2,881 orders. Per-phase end-to-end
terminal-outcome rates below are measured while the named service has the stated Ready count;
they are not isolated per-consumer rates. The single-show run also acknowledged 2,881 orders.
Raw run IDs map to the links in the main table.

| Ramp run ID | Inventory 1 Ready | Inventory 8 Ready | Payment 1 Ready | Payment 8 Ready |
|---|---|---|---|---|
| m6-consumer-ramp-1794135562105823000 | 7.95498392906822 | 8.010592654731957 | 7.95498392906822 | 8.297111295594132 |
| m6-consumer-ramp-1794135942702742000 | 7.951054647555332 | 7.978460030525333 | 7.951054647555332 | 7.952851902904059 |
| m6-consumer-ramp-1794136322769364000 | 7.939652499960554 | 7.995722081090355 | 7.939652499960554 | 7.987268064865741 |
| m6-consumer-ramp-1794137002893522000 (single show) | 8.007817878304666 | 8.025785165011973 | 8.007817878304666 | 8.022036884171643 |

Both [SIGINT](docs/evidence/m6/cleanup-SIGINT.json) and
[timeout](docs/evidence/m6/cleanup-timeout.json) probes mutated scaler settings after acknowledged
load, exited nonzero as intended, restored the exact effective baseline, and completed a separately
audited inventory-kill rerun with disjoint order IDs. These are cleanup evidence, not accepted chaos
runs for the aborted first loads.

### Failed and interrupted attempts

- [Incremental ramp retry](docs/evidence/m6/m6-consumer-ramp-1794134769312914000/), 2026-11-08:
  963 acknowledged orders, 794 dropped iterations, 851 failed HTTP requests, 235 failed checks.
  Rollout timed out with four Ready / five desired inventory pods, before payment scaling.
  Kubelet log retrieval hit a TLS handshake timeout; VM memory available was 967Mi and swap used
  590Mi. These observations do not prove the cause of the node slowdown or any invariant violation.
  Cleanup commands completed; all pods subsequently became Ready. No complete invariant audit.
  Verbatim logs are also preserved in `docs/evidence/m6/retry-raw-logs.zip`.
- The preserved [earlier interrupted retry](docs/evidence/m6/m6-consumer-ramp-1793799411923024000/)
  has no completed audit and is not counted. It predates this run; its raw files were not modified.
- On 2026-11-08 `scripts/m5-up.sh` failed twice downloading its pinned monitoring chart (HTTP 503).
  The project stack already existed. Its original baseline was explicitly restored and read back
  before the incremental ramp; no replacement cluster or dependency upgrade was used.

- [Initial inventory attempt](docs/evidence/m6/m6-inventory-kill-1793785168604883000/): 948 acknowledgments,
  13 dropped k6 iterations, zero HTTP/check failures. No completed invariant audit.
- [Second inventory attempt](docs/evidence/m6/m6-inventory-kill-1793785408527003000/): 956 acknowledgments,
  5 dropped iterations, zero HTTP/check failures. No completed invariant audit. The later M6 wrapper
  preallocated 64 VUs without relaxing thresholds.
- [Intentionally interrupted inventory attempt](docs/evidence/m6/m6-inventory-kill-1793785755530496000/):
  959 acknowledgments in the k6 summary, but no completed invariant audit; not counted as a pass.
- [Provider rollout-baseline refusal](docs/evidence/m6/m6-payment-declines-1793786975652397000/):
  the terminating old pod caused a pre-load audit refusal. Fixed in the collector with a regression;
  no application change and no fabricated measurements.
- [First ramp attempt](docs/evidence/m6/m6-consumer-ramp-1793788102587266000/): rollout reported only
  5 of 8 new replicas updated, not eight Ready. k6 recorded 381 acknowledgments, 923 dropped
  iterations and 960 failed HTTP attempts. Kind API TLS handshakes and the node's containerd CRI
  then timed out. Recovery, invariant results and throughput-at-eight are NOT verified. Desired
  replica/resource/scaler settings received best-effort restoration patches, but rollout completion
  and the final baseline snapshot could not be verified. The collector was stopped; it is not left
  generating load. The tool approval gate refused a project-node stop/start at that time; operator
  recovery was required. The owner later recovered the runtime and increased VM memory. No
  lifecycle bypass was attempted.

### Bottlenecks and limits

- **Single-show inventory uses one Kafka partition.** Additional inventory replicas cannot process
  that key in parallel. The real [single-show comparison](docs/evidence/m6/bottleneck.json) measured
  7.999579402694281 terminal outcomes/s at one Ready inventory replica (175 completions) versus
  8.025785165011973/s at eight (124 completions in its shorter window). Both windows used the
  same offered 8/s. This shows no eightfold gain at this load, not a measured saturation ceiling
  or proof that inventory was the active bottleneck. Multi-show rates are likewise end-to-end
  outcomes at the offered load, not maximum inventory or payment capacity.
- Inventory deletion caused a real drop in completion rate during replacement, followed by backlog
  catch-up (the table records every run rather than hiding this behind an average). A single broker
  deletion kept completion rates near the offered load with RF=3 and min ISR=2.
- The three Postgres probes measured baseline/injected round-trip seconds of
  0.08587195799918845 / 4.016240458004177, 0.07906504100537859 / 3.7067007919831667, and
  0.07849620800698176 / 4.01285149998148. These include kubectl/psql overhead and represent a short
  table-lock stall, not general network latency or long DB outages/DLT recovery.
- The 8GB VM failed the earlier rollouts. Some 256Mi pods were specifically observed OOMKilled;
  the separate node slowdowns are not conclusively diagnosed. After the owner increased Docker
  memory to 15.64GiB, the staged chart-default ramp and both cleanup probes passed. Six CPUs
  were unchanged; CPU throttling and maximum sustainable arrival rates were not quantified.
  Fresh Java 21 `mvn verify` passed 213 tests (zero failures/errors/skips), the M3/M5 chart checks
  passed, and the full M6 harness passed (only its separately executed regression wrapper skipped).
  Independent review remains pending. M7/M8 are not started.
