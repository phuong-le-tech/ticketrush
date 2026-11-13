# M5 live acceptance evidence

Captured on 2026-10-31 from the real local Docker/kind stack, not evaluator fixtures.
Implementation code head: `0dddd8c` on `milestone-5`. Later commits add documentation/evidence only.

Command:

```sh
M5_CONSUMER_PROCESSING_DELAY_MS=200 scripts/m5-acceptance.sh
```

Exit status: 0. Elapsed: 492.626 seconds. The command deletes the kind cluster, bootstraps
twice, verifies stable node/Grafana Secret fingerprints, performs one isolated warm-up purchase,
then runs k6 for 120 seconds at 8 orders/second over 24 generated events.

- `acceptance.log`: raw timestamped stdout, including scale-up, drain, scale-down and final verdict.
- `samples.jsonl`: raw Prometheus lag and Kubernetes ready-replica samples, epoch seconds.
- `evidence.json`: full measured stack/query/scaling/stock/order/charge evidence. Customer and event
  identifiers are generated test data. Credentials are SHA256 fingerprints, never passwords.
- `k6-summary.json`: k6's raw summary export. Its threshold boolean means threshold failure, so
  `false` is the passing state. `http_req_failed.value=0`, `checks.value=1`, zero dropped iterations.
- `maven-summary.txt`: per-class results and final reactor output from the separate Java 21 root
  verification in the same implementation run.

Measured result: 960 acknowledged distinct orders, 960 CONFIRMED, one charge each. Across 24
seeded events, initial stock 12000 = remaining 11040 + sold 960. Both services scale 1 -> 2 -> 1.
The payment provider stayed on the same pod without a restart through the charge audit.
All four provisioned Grafana dashboard areas returned nonempty finite Prometheus vectors.

Offline re-evaluation checks the recorded evidence but does not rerun the cluster:

```sh
scripts/m5-acceptance.sh --evaluate docs/evidence/m5/evidence.json
```

Limits: one laptop-sized cluster, cached service builds, artificial processing delay, 960 orders.
This proves the autoscaling demonstration, not production capacity or failure recovery under load.
The warm-up purchase is separate from the 960 audited k6 orders and the 24 audited events.
