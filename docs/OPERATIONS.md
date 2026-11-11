# TicketRush - Operations (kind, observability, chaos runner)

Moved out of the README in M8. How to bootstrap the kind cluster, open Grafana and run the chaos
experiments. Measured results are in [CHAOS-RESULTS.md](CHAOS-RESULTS.md).

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
runtime logs are in [raw-logs.zip](evidence/m6/raw-logs.zip), with a byte-count/SHA256
[index](evidence/m6/raw-logs-index.json); JSON/JSONL measurements are also stored directly.
The ZIP is a snapshot of the first partial execution, not an automatically refreshed log sink.
The completed retry's [raw archive](evidence/m6/retry-raw-logs.zip) and
[byte/SHA256 index](evidence/m6/retry-raw-logs-index.json) preserve its actual runtime and
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

