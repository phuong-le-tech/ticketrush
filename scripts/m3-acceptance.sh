#!/usr/bin/env bash
# Milestone 3 acceptance (AC4, AC5, AC6). Needs docker, kind, helm, kubectl, curl, jq. Takes several minutes.
#
#   scripts/m3-acceptance.sh
#
# Steps, each one fails loud (non-zero exit, message "FAIL <name>: <reason>"):
#   AC4_clean_up              deletes the kind cluster (skip with M3_KEEP_CLUSTER=1), runs scripts/m3-up.sh: exit 0,
#                             every pod Ready
#   AC4_second_run            runs scripts/m3-up.sh again: exit 0, same kind node container, no new pod restarts
#   AC5_order_confirmed       POST /orders through a port-forward to the gateway: 202 and a body with orderId; within
#                             60 s that orderId is CONFIRMED in orders.orders (Postgres) AND present on orders.confirmed
#   AC6_graceful_restart      follows the order-service log, runs kubectl rollout restart, rollout status exit 0, and the
#                             followed log of the old pod contains "Graceful shutdown complete" (a SIGKILLed JVM cannot
#                             log it, so this is also the "no SIGKILL" proof)
#
# Contract with the chart and m3-up.sh (decided by the test plan, the code-writer implements it):
#   kind cluster "ticketrush" (node container ticketrush-control-plane), namespace "ticketrush",
#   Deployments and Services named after the modules (api-gateway, order-service, ...) with the Service port equal to
#   the container port (gateway 8080), StatefulSet "postgres" (pod postgres-0, container env POSTGRES_USER, database
#   ticketrush), Strimzi Kafka CR "ticketrush" (broker pods carry the label strimzi.io/cluster=ticketrush,
#   plain internal listener on 9092), seeded event "concert-1" (INVENTORY_SEED_EVENT_ID default) with stock available.
set -u
cd "$(dirname "$0")/.."

CLUSTER="${M3_CLUSTER:-ticketrush}"
NS="${M3_NAMESPACE:-ticketrush}"
LOCAL_PORT="${M3_GATEWAY_LOCAL_PORT:-18080}"
CONFIRM_TIMEOUT_S="${M3_CONFIRM_TIMEOUT_S:-60}"
WORK="$(mktemp -d "${TMPDIR:-/tmp}/m3-acceptance.XXXXXX")"
PF_PID=""
LOG_PID=""

cleanup() {
  [ -z "$PF_PID" ] || kill "$PF_PID" 2> /dev/null
  [ -z "$LOG_PID" ] || kill "$LOG_PID" 2> /dev/null
  rm -rf "$WORK"
}
trap cleanup EXIT

die() { echo "FAIL $1: $2"; exit 1; }
pass() { echo "PASS $1${2:+ - $2}"; }

for t in docker kind helm kubectl curl jq; do
  command -v "$t" > /dev/null 2>&1 || die prerequisites "missing tool: $t (brew install kind helm jq)"
done

# Number of pods that are not Ready, and the sum of all container restart counts.
not_ready_pods() {
  kubectl -n "$NS" get pods -o json \
    | jq '[.items[] | select(([.status.conditions[]? | select(.type == "Ready" and .status == "True")] | length) == 0 and .status.phase != "Succeeded")] | length'
}
total_restarts() {
  kubectl -n "$NS" get pods -o json | jq '[.items[].status.containerStatuses[]?.restartCount] | add // 0'
}
pod_count() {
  kubectl -n "$NS" get pods -o json | jq '.items | length'
}
node_container_id() {
  docker inspect "${CLUSTER}-control-plane" --format '{{.Id}}' 2> /dev/null
}

# ---- AC4: clean start ----
if [ "${M3_KEEP_CLUSTER:-0}" != "1" ]; then
  kind delete cluster --name "$CLUSTER" > /dev/null 2>&1
fi
scripts/m3-up.sh || die AC4_clean_up "scripts/m3-up.sh exited non-zero"
pods=$(pod_count)
[ "$pods" -gt 0 ] || die AC4_clean_up "no pods in namespace $NS after m3-up.sh"
bad=$(not_ready_pods)
[ "$bad" = "0" ] || die AC4_clean_up "$bad pod(s) not Ready: $(kubectl -n "$NS" get pods --no-headers | grep -v ' Running ' | tr '\n' ';')"
pass AC4_clean_up "$pods pods Ready"

# ---- AC4: second run is a no-op ----
node_before=$(node_container_id)
restarts_before=$(total_restarts)
[ -n "$node_before" ] || die AC4_second_run "kind node container ${CLUSTER}-control-plane not found"
scripts/m3-up.sh || die AC4_second_run "second scripts/m3-up.sh exited non-zero"
node_after=$(node_container_id)
[ "$node_before" = "$node_after" ] || die AC4_second_run "kind node container changed: cluster was recreated"
restarts_after=$(total_restarts)
[ "$restarts_before" = "$restarts_after" ] || die AC4_second_run "pod restarts went from $restarts_before to $restarts_after"
bad=$(not_ready_pods)
[ "$bad" = "0" ] || die AC4_second_run "$bad pod(s) not Ready after second run"
pass AC4_second_run "same node, $restarts_after restarts"

# ---- AC5: order reaches CONFIRMED in the cluster ----
kubectl -n "$NS" port-forward svc/api-gateway "${LOCAL_PORT}:8080" > "$WORK/port-forward.log" 2>&1 &
PF_PID=$!
for _ in $(seq 1 30); do
  curl -s -o /dev/null "http://localhost:${LOCAL_PORT}/actuator/health/readiness" && break
  kill -0 "$PF_PID" 2> /dev/null || die AC5_order_confirmed "port-forward died: $(cat "$WORK/port-forward.log")"
  sleep 1
done

idem_key="m3-$(uuidgen)"
status=$(curl -s -o "$WORK/order.json" -w '%{http_code}' -X POST "http://localhost:${LOCAL_PORT}/orders" \
  -H 'Content-Type: application/json' -H "Idempotency-Key: ${idem_key}" \
  -d '{"eventId":"concert-1","customerId":"m3-customer","quantity":1}')
[ "$status" = "202" ] || die AC5_order_confirmed "POST /orders returned HTTP ${status}, expected 202: $(cat "$WORK/order.json")"
order_id=$(jq -r '.orderId // empty' "$WORK/order.json")
[ -n "$order_id" ] || die AC5_order_confirmed "202 response without orderId: $(cat "$WORK/order.json")"

pg_status=""
deadline=$(( $(date +%s) + CONFIRM_TIMEOUT_S ))
while [ "$(date +%s)" -lt "$deadline" ]; do
  pg_status=$(kubectl -n "$NS" exec postgres-0 -- sh -c \
    "psql -U \"\$POSTGRES_USER\" -d ticketrush -tA -c \"select status from orders.orders where order_id = '${order_id}'\"" 2> /dev/null | tr -d '[:space:]')
  [ "$pg_status" = "CONFIRMED" ] && break
  sleep 2
done
[ "$pg_status" = "CONFIRMED" ] || die AC5_order_confirmed "order ${order_id} not CONFIRMED in Postgres within ${CONFIRM_TIMEOUT_S}s (status: '${pg_status}')"

broker=$(kubectl -n "$NS" get pods -l strimzi.io/cluster=ticketrush,strimzi.io/kind=Kafka -o jsonpath='{.items[0].metadata.name}')
[ -n "$broker" ] || die AC5_order_confirmed "no Kafka broker pod found"
kubectl -n "$NS" exec "$broker" -- /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic orders.confirmed --from-beginning --timeout-ms 30000 \
  > "$WORK/confirmed.txt" 2> /dev/null
grep -q "$order_id" "$WORK/confirmed.txt" || die AC5_order_confirmed "order ${order_id} not found on orders.confirmed"
pass AC5_order_confirmed "POST 202, ${order_id} CONFIRMED in Postgres and on orders.confirmed"

# ---- AC6: graceful shutdown on rollout restart ----
kubectl -n "$NS" logs -f deploy/order-service > "$WORK/order-service.log" 2>&1 &
LOG_PID=$!
sleep 2
kubectl -n "$NS" rollout restart deploy/order-service > /dev/null || die AC6_graceful_restart "rollout restart failed"
kubectl -n "$NS" rollout status deploy/order-service --timeout=180s > /dev/null || die AC6_graceful_restart "rollout status did not complete"
# logs -f ends by itself when the old pod terminates; give it a bounded time to flush.
for _ in $(seq 1 40); do
  kill -0 "$LOG_PID" 2> /dev/null || break
  sleep 1
done
grep -q "Graceful shutdown complete" "$WORK/order-service.log" \
  || die AC6_graceful_restart "old order-service pod did not log 'Graceful shutdown complete' (killed instead of drained?)"
bad=$(not_ready_pods)
[ "$bad" = "0" ] || die AC6_graceful_restart "$bad pod(s) not Ready after the restart"
pass AC6_graceful_restart "old pod logged 'Graceful shutdown complete', rollout completed"

echo "RESULT: m3-acceptance passed"
