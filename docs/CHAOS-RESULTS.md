# TicketRush - Chaos results

Raw evidence (JSON, k6 IDs, provider audits) is under
[evidence/chaos/](evidence/chaos/). Commands to reproduce are in [OPERATIONS.md](OPERATIONS.md).

## Measurement definitions

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
  group/topic sum, which omits unknown offset partitions as documented in autoscaling. Final SQL
  conservation is independently required. Timing is laptop/polling dependent.

## Measured results

Completed real audits: inventory-kill **6**, broker-restart **3**, payment-declines **3**,
Postgres-latency **3**, consumer-ramp **4** (three 24-show repetitions plus one single-show
comparison). The inventory count includes the two independently audited post-abort reruns.
All 19 completed runs are listed separately below. The original 13 audits were measured on
2026-11-04; the six new audits ran after the owner increased the VM to 16GB on 2026-11-08.
These environments are not pooled into an average or a capacity claim.

| Experiment | Fault | Invariants | Recovery seconds | Before / during / after terminal outcomes per second | Run | Date |
|---|---|---|---|---|---|---|
| broker-restart | one broker pod deletion | PASS | 99.83673596382141 | 7.958116877157418 / 8.000770827397252 / 7.722169192357605 | [raw run](evidence/chaos/run-m6-broker-restart-1793786582291791000.json) | 2026-11-04 |
| broker-restart | one broker pod deletion | PASS | 98.51445984840393 | 7.996811506235794 / 8.009661458195517 / 7.888682976823053 | [raw run](evidence/chaos/run-m6-broker-restart-1793786713567421000.json) | 2026-11-04 |
| broker-restart | one broker pod deletion | PASS | 100.24780082702637 | 7.985144099595496 / 8.031143871297072 / 7.624561639273277 | [raw run](evidence/chaos/run-m6-broker-restart-1793786843651282000.json) | 2026-11-04 |
| inventory-kill | inventory pod deletion | PASS | 100.40090799331665 | 7.9184490690696405 / 0.3240031897120214 / 11.558675019426442 | [raw run](evidence/chaos/run-m6-inventory-kill-1793785612760753000.json) | 2026-11-04 |
| inventory-kill | inventory pod deletion | PASS | 99.89213395118713 | 7.930233962827798 / 0.5882444927637638 / 9.406048223503381 | [raw run](evidence/chaos/run-m6-inventory-kill-1793786175805766000.json) | 2026-11-04 |
| inventory-kill | inventory pod deletion | PASS | 100.33470177650452 | 7.742950858409198 / 1.7506504898826114 / 8.711014379619941 | [raw run](evidence/chaos/run-m6-inventory-kill-1793786312088218000.json) | 2026-11-04 |
| inventory-kill | inventory pod deletion | PASS | 102.9268319606781 | 7.9168481492571905 / 0.5124147419368179 / 8.48208687963013 | [raw run](evidence/chaos/run-m6-inventory-kill-1793786446569085000.json) | 2026-11-04 |
| payment-declines | provider rate 0.3 | PASS | 114.43596386909485 | 7.973964284086212 / 7.001573147130719 / 0.0 | [raw run](evidence/chaos/run-m6-payment-declines-1793787117900791000.json) | 2026-11-04 |
| payment-declines | provider rate 0.3 | PASS | 114.30489230155945 | 7.964195831918248 / 6.998121269746247 / 0.0 | [raw run](evidence/chaos/run-m6-payment-declines-1793787407150655000.json) | 2026-11-04 |
| payment-declines | provider rate 0.3 | PASS | 118.12455892562866 | 7.946810343703649 / 6.98383440195554 / 0.0 | [raw run](evidence/chaos/run-m6-payment-declines-1793787555934552000.json) | 2026-11-04 |
| postgres-latency | 4s table lock | PASS | 99.0099709033966 | 7.968758703143042 / 1.123929637778963 / 8.146084871587771 | [raw run](evidence/chaos/run-m6-postgres-latency-1793787705384289000.json) | 2026-11-04 |
| postgres-latency | 4s table lock | PASS | 100.20807814598083 | 7.968703131968767 / 2.519447882597236 / 8.004480087882593 | [raw run](evidence/chaos/run-m6-postgres-latency-1793787838420505000.json) | 2026-11-04 |
| postgres-latency | 4s table lock | PASS | 101.07388591766357 | 7.984156179217333 / 1.5774957246344925 / 8.027990817777965 | [raw run](evidence/chaos/run-m6-postgres-latency-1793787970340420000.json) | 2026-11-04 |
| consumer-ramp | staged inventory/payment 1->8, then restore | PASS | 340.56781005859375 | 7.935919222191438 / 8.003876473464302 / 7.906196245522607 | [raw run](evidence/chaos/run-m6-consumer-ramp-1794135562105823000.json) | 2026-11-08 |
| consumer-ramp | staged inventory/payment 1->8, then restore | PASS | 339.5589737892151 | 7.984634497280194 / 7.995837685399253 / 7.940884004802306 | [raw run](evidence/chaos/run-m6-consumer-ramp-1794135942702742000.json) | 2026-11-08 |
| consumer-ramp | staged inventory/payment 1->8, then restore | PASS | 340.2294728755951 | 7.9776487615252805 / 7.995914077496762 / 7.865059752674633 | [raw run](evidence/chaos/run-m6-consumer-ramp-1794136322769364000.json) | 2026-11-08 |
| consumer-ramp | single-show staged inventory/payment 1->8 | PASS | 345.0931408405304 | 7.999579402694281 / 7.996894881265104 / 7.72771416400147 | [raw run](evidence/chaos/run-m6-consumer-ramp-1794137002893522000.json) | 2026-11-08 |
| inventory-kill | post-SIGINT inventory pod deletion | PASS | 98.6995952129364 | 7.9742217093620225 / 0.4918175496175186 / 8.937379497914712 | [raw run](evidence/chaos/run-m6-inventory-kill-1794136718792903000.json) | 2026-11-08 |
| inventory-kill | post-timeout inventory pod deletion | PASS | 99.85872197151184 | 7.998282194608636 / 0.2832771288938411 / 8.75041284157138 | [raw run](evidence/chaos/run-m6-inventory-kill-1794136871523725000.json) | 2026-11-08 |
| consumer-ramp | first inventory rollout toward 8 | NOT VERIFIED | not measured | no valid windows | [failed attempt](evidence/chaos/m6-consumer-ramp-1793788102587266000/) | 2026-11-04 |
| consumer-ramp | incremental retry, stopped at 4 Ready / 5 desired | NOT VERIFIED | not measured | no valid windows | [failed retry](evidence/chaos/m6-consumer-ramp-1794134769312914000/) | 2026-11-08 |

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

Both [SIGINT](evidence/chaos/cleanup-SIGINT.json) and
[timeout](evidence/chaos/cleanup-timeout.json) probes mutated scaler settings after acknowledged
load, exited nonzero as intended, restored the exact effective baseline, and completed a separately
audited inventory-kill rerun with disjoint order IDs. These are cleanup evidence, not accepted chaos
runs for the aborted first loads.

## Failed and interrupted attempts

- [Incremental ramp retry](evidence/chaos/m6-consumer-ramp-1794134769312914000/), 2026-11-08:
  963 acknowledged orders, 794 dropped iterations, 851 failed HTTP requests, 235 failed checks.
  Rollout timed out with four Ready / five desired inventory pods, before payment scaling.
  Kubelet log retrieval hit a TLS handshake timeout; VM memory available was 967Mi and swap used
  590Mi. These observations do not prove the cause of the node slowdown or any invariant violation.
  Cleanup commands completed; all pods subsequently became Ready. No complete invariant audit.
  Verbatim logs are also preserved in `docs/evidence/chaos/retry-raw-logs.zip`.
- The preserved [earlier interrupted retry](evidence/chaos/m6-consumer-ramp-1793799411923024000/)
  has no completed audit and is not counted. It predates this run; its raw files were not modified.
- On 2026-11-08 `scripts/observability-up.sh` failed twice downloading its pinned monitoring chart (HTTP 503).
  The project stack already existed. Its original baseline was explicitly restored and read back
  before the incremental ramp; no replacement cluster or dependency upgrade was used.

- [Initial inventory attempt](evidence/chaos/m6-inventory-kill-1793785168604883000/): 948 acknowledgments,
  13 dropped k6 iterations, zero HTTP/check failures. No completed invariant audit.
- [Second inventory attempt](evidence/chaos/m6-inventory-kill-1793785408527003000/): 956 acknowledgments,
  5 dropped iterations, zero HTTP/check failures. No completed invariant audit. The later chaos wrapper
  preallocated 64 VUs without relaxing thresholds.
- [Intentionally interrupted inventory attempt](evidence/chaos/m6-inventory-kill-1793785755530496000/):
  959 acknowledgments in the k6 summary, but no completed invariant audit; not counted as a pass.
- [Provider rollout-baseline refusal](evidence/chaos/m6-payment-declines-1793786975652397000/):
  the terminating old pod caused a pre-load audit refusal. Fixed in the collector with a regression;
  no application change and no fabricated measurements.
- [First ramp attempt](evidence/chaos/m6-consumer-ramp-1793788102587266000/): rollout reported only
  5 of 8 new replicas updated, not eight Ready. k6 recorded 381 acknowledgments, 923 dropped
  iterations and 960 failed HTTP attempts. Kind API TLS handshakes and the node's containerd CRI
  then timed out. Recovery, invariant results and throughput-at-eight are NOT verified. Desired
  replica/resource/scaler settings received best-effort restoration patches, but rollout completion
  and the final baseline snapshot could not be verified. The collector was stopped; it is not left
  generating load. The tool approval gate refused a project-node stop/start at that time; operator
  recovery was required. The owner later recovered the runtime and increased VM memory. No
  lifecycle bypass was attempted.

## Bottlenecks and limits

- **Single-show inventory uses one Kafka partition.** Additional inventory replicas cannot process
  that key in parallel. The real [single-show comparison](evidence/chaos/bottleneck.json) measured
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
  Fresh Java 21 `mvn verify` passed 213 tests (zero failures/errors/skips), the base and observability chart checks
  passed, and the full chaos harness passed (only its separately executed regression wrapper skipped).
  Independent review remains pending.
