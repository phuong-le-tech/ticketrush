# TicketRush

A flash-sale ticketing platform built with Java 21, Spring Boot, Kafka (KRaft, Strimzi),
PostgreSQL, Redis and Kubernetes. The invariant is no oversell, duplicate order or duplicate charge.

**Status:** M5 implemented and locally verified; independent review pending. See
[verification and limitations](docs/PROGRESS.md),
[spec](docs/SPEC.md) and [design decisions](docs/DESIGN.md). M6-M8 are not started.

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

Full portfolio polish, chaos experiments and the Angular UI remain future milestones.
