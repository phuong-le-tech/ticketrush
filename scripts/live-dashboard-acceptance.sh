#!/usr/bin/env bash
# Live dashboard acceptance: proves by execution that the dashboard is deployed on kind,
# serves live Prometheus data through its same-origin routes, and that load never breaks
# the central invariant (never sell more tickets than exist).
#
#   scripts/live-dashboard-acceptance.sh [--with-compose]
#
# Needs the kind stack up (scripts/kind-up.sh or scripts/observability-up.sh), plus docker,
# kubectl, k6, jq, node. Checks, in order (one PASS/FAIL line each, exit 0 only if all pass):
#   LD1_unit                  dashboard npm ci, tests, production build in the pinned Node image
#   LD2_deployed              deploy/dashboard rolled out on kind-ticketrush, port-forward works
#   LD3_proxy_boundaries      POST metrics 403, unknown metrics paths 404, /actuator 404
#   LD4_live_queries          60 s of k6 load; all 8 dashboard QUERIES succeed non-empty during it
#   LD5_reads                 stock read >= 0, activity read 200
#   LD6_invariant             stock never negative, confirmed tickets never above the seed
#   LD7_compose_still_passes  only with --with-compose: scripts/dashboard-acceptance.sh
#                             (resets the Compose Postgres volume, so it is opt-in)
# kubectl always uses --context kind-ticketrush; the current context is asserted unchanged.
set -uo pipefail

cd "$(dirname "$0")/.."

WITH_COMPOSE=0
case "${1:-}" in
  "") ;;
  --with-compose) WITH_COMPOSE=1 ;;
  *) echo "usage: $0 [--with-compose]" >&2; exit 2 ;;
esac

CTX="kind-ticketrush"
NS="ticketrush"
NODE_IMAGE="${NODE_IMAGE:-node:24.22.2}"
EVENT_ID="${EVENT_ID:-concert-1}"
LOAD_DURATION_SECONDS="${LOAD_DURATION_SECONDS:-60}"
LOAD_RATE="${LOAD_RATE:-20}"
LOAD_EVENTS=3
LOAD_EVENT_STOCK=300

K() { kubectl --context "$CTX" -n "$NS" "$@"; }

TMP_DIR="$(mktemp -d)"
CONTEXT_BEFORE="$(kubectl config current-context 2>/dev/null || echo none)"
PIDS=()
RUN_ID="ld-$(date +%s)"
FAILED=0
REACHED_END=0 # set only after the final summary, so an interrupted run never exits 0
LOAD_EVENT_IDS=()

record() { # status name detail
  printf '%s %s - %s\n' "$1" "$2" "$3"
  [ "$1" = "PASS" ] || FAILED=1
}

cleanup() {
  local pid
  for pid in "${PIDS[@]:-}"; do
    [ -n "$pid" ] && kill "$pid" 2>/dev/null
  done
  # Remove the load events even on an early exit.
  local ev
  for ev in "${LOAD_EVENT_IDS[@]:-}"; do
    [ -n "$ev" ] && kubectl --context "$CTX" -n "$NS" exec redis-0 -- redis-cli DEL "inventory:event:$ev" >/dev/null 2>&1
  done
  [ "$REACHED_END" = 1 ] || FAILED=1
  if [ "$(kubectl config current-context 2>/dev/null || echo none)" != "$CONTEXT_BEFORE" ]; then
    echo "FAIL context_unchanged - kubectl current context changed from $CONTEXT_BEFORE" >&2
    FAILED=1
  fi
  rm -rf "$TMP_DIR"
  exit "$FAILED"
}
trap cleanup EXIT
trap 'exit 130' INT TERM

# service port -> sets FWD_URL to http://127.0.0.1:<local>; kubectl picks a free local port.
# Not run in a $(...) subshell, so the PID lands in PIDS and cleanup can kill it.
forward() {
  local log="$TMP_DIR/pf-$1.log" i port
  kubectl --context "$CTX" -n "$NS" port-forward "svc/$1" ":$2" --address=127.0.0.1 >"$log" 2>&1 &
  PIDS+=("$!")
  for i in $(seq 1 50); do
    port="$(sed -n 's/.*Forwarding from 127\.0\.0\.1:\([0-9]*\).*/\1/p' "$log" | head -1)"
    [ -n "$port" ] && { FWD_URL="http://127.0.0.1:$port"; return 0; }
    sleep 0.2
  done
  return 1
}

code_of() { # method url -> http status
  curl -sS -o /dev/null -w '%{http_code}' --max-time 15 -X "$1" "$2" || echo 000
}

psql_q() { # sql -> rows (read-only use)
  K exec postgres-0 -- sh -c 'exec psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d ticketrush -tA -c "$1"' sh "$1"
}

# ---------------------------------------------------------------------------
# LD1: unit tests and production build in the pinned Node image
if docker run --rm \
  -u "$(id -u):$(id -g)" \
  -e HOME=/tmp -e npm_config_cache=/tmp/npm-cache -e NG_CLI_ANALYTICS=false -e NO_COLOR=1 \
  -v "$PWD/dashboard":/app -w /app \
  "$NODE_IMAGE" sh -c 'npm ci --no-audit --no-fund && npm test && npm run build' \
  >"$TMP_DIR/dashboard.log" 2>&1; then
  record PASS LD1_unit "$(grep -E 'Tests +[0-9]' "$TMP_DIR/dashboard.log" | tail -1 | sed 's/^ *//'); production build ok"
else
  tail -40 "$TMP_DIR/dashboard.log" >&2
  record FAIL LD1_unit "npm ci/test/build failed in $NODE_IMAGE"
fi

# ---------------------------------------------------------------------------
# LD2: deployed on kind
DASH=""
GW=""
if K rollout status deploy/dashboard --timeout=120s >"$TMP_DIR/rollout.log" 2>&1 \
  && forward dashboard 8080 && DASH="$FWD_URL" \
  && forward api-gateway 8080 && GW="$FWD_URL" \
  && [ "$(code_of GET "$DASH/healthz")" = "200" ]; then
  record PASS LD2_deployed "deploy/dashboard rolled out on $CTX, /healthz 200 via $DASH"
else
  cat "$TMP_DIR/rollout.log" >&2
  record FAIL LD2_deployed "dashboard not rolled out or not reachable on $CTX"
fi

# ---------------------------------------------------------------------------
# LD3: nginx boundaries
if [ -n "$DASH" ]; then
  r1="$(code_of POST "$DASH/api/metrics/query")"
  r2="$(code_of GET "$DASH/api/metrics/admin/tsdb")"
  r3="$(code_of GET "$DASH/api/metrics/")"
  r4="$(code_of GET "$DASH/api/metrics/querymore")"
  r5="$(code_of GET "$DASH/actuator/prometheus")"
  if [ "$r1 $r2 $r3 $r4 $r5" = "403 404 404 404 404" ]; then
    record PASS LD3_proxy_boundaries "POST query 403; admin/tsdb, metrics/, querymore, actuator 404"
  else
    record FAIL LD3_proxy_boundaries "got POST query=$r1 admin/tsdb=$r2 metrics/=$r3 querymore=$r4 actuator=$r5, want 403 404 404 404 404"
  fi
else
  record FAIL LD3_proxy_boundaries "no dashboard origin (LD2 failed)"
fi

# ---------------------------------------------------------------------------
# LD4: live queries during a k6 load
# The eight query strings come from the dashboard source so the two cannot drift.
node -e '
const src = require("fs").readFileSync("dashboard/src/app/metrics.ts", "utf8");
const groups = src.match(/const GROUPS =\s*(\x27[^\x27]*\x27)/)[1];
const block = src.match(/export const QUERIES[^=]*=\s*(\{[\s\S]*?\n\});/)[1];
const GROUPS = eval(groups);
const q = eval("(" + block + ")");
console.log(JSON.stringify(q));
' >"$TMP_DIR/queries.json" 2>"$TMP_DIR/queries.err" || true
NQ="$(jq 'length' "$TMP_DIR/queries.json" 2>/dev/null || echo 0)"

query_ok() { # id query -> 0 when status success, non-empty, and the id-specific rule holds
  local id="$1" q="$2" out="$TMP_DIR/q-$1.json"
  curl -sS --max-time 15 -G --data-urlencode "query=$q" "$DASH/api/metrics/query" -o "$out" || return 1
  jq -e '.status == "success" and (.data.result | length) > 0' "$out" >/dev/null 2>&1 || return 1
  case "$id" in
    ordersRate) jq -e '[.data.result[].value[1] | tonumber] | add > 0' "$out" >/dev/null 2>&1 ;;
    readyPods)
      jq -e '[.data.result[] | select(.metric.job == "inventory-service" or .metric.job == "payment-service")
              | select((.value[1] | tonumber) >= 1)] | length == 2' "$out" >/dev/null 2>&1 ;;
    *) return 0 ;;
  esac
}

LD4_DETAIL=""
LD4_OK=0
if [ "$NQ" -lt 8 ]; then
  LD4_DETAIL="parsed $NQ queries from dashboard/src/app/metrics.ts, need 8 ($(cat "$TMP_DIR/queries.err" 2>/dev/null))"
elif [ -z "$DASH" ] || [ -z "$GW" ]; then
  LD4_DETAIL="no dashboard or gateway port-forward (LD2 failed)"
else
  for i in $(seq 0 $((LOAD_EVENTS - 1))); do
    ev="$RUN_ID-$i"
    LOAD_EVENT_IDS+=("$ev")
    K exec redis-0 -- redis-cli HSET "inventory:event:$ev" stock "$LOAD_EVENT_STOCK" priceCents 100 currency EUR >/dev/null
  done
  EVENT_JSON="$(printf '%s\n' "${LOAD_EVENT_IDS[@]}" | jq -R . | jq -sc .)"
  # Warm the cold gateway path first: slow first requests make k6 drop its first iteration, which
  # fails the load script's dropped_iterations threshold. Keys start with RUN_ID so the audit counts them.
  for i in $(seq 1 20); do
    curl -sS -o /dev/null --max-time 15 -X POST "$GW/orders" -H 'Content-Type: application/json' \
      -H "Idempotency-Key: $RUN_ID-warm-$i" \
      -d "{\"eventId\":\"${LOAD_EVENT_IDS[0]}\",\"customerId\":\"$RUN_ID-warm-$i\",\"quantity\":1}" || true
  done
  k6 run -e "BASE_URL=$GW" -e "EVENT_IDS=$EVENT_JSON" -e "RUN_ID=$RUN_ID" \
    -e "LOAD_RATE=$LOAD_RATE" -e "LOAD_DURATION=${LOAD_DURATION_SECONDS}s" \
    scripts/autoscale-load.js >"$TMP_DIR/k6.log" 2>&1 &
  K6_PID=$!
  PIDS+=("$K6_PID")
  # Sample during the load: after a short ramp, retry every 5 s until all pass or the load ends.
  sleep 15
  FAILING=""
  while kill -0 "$K6_PID" 2>/dev/null; do
    FAILING=""
    for id in $(jq -r 'keys[]' "$TMP_DIR/queries.json"); do
      query_ok "$id" "$(jq -r --arg id "$id" '.[$id]' "$TMP_DIR/queries.json")" || FAILING="$FAILING $id"
    done
    [ -z "$FAILING" ] && break
    sleep 5
  done
  K6_EXIT=0
  wait "$K6_PID" || K6_EXIT=$?
  if [ -n "$FAILING" ] || [ "$(ls "$TMP_DIR"/q-*.json 2>/dev/null | wc -l | tr -d ' ')" -lt 8 ]; then
    LD4_DETAIL="queries failing during load:${FAILING:- (none sampled)}"
  elif [ "$K6_EXIT" != 0 ]; then
    tail -30 "$TMP_DIR/k6.log" >&2
    LD4_DETAIL="k6 exited $K6_EXIT"
  else
    LD4_OK=1
    RATE="$(jq -r '.data.result[0].value[1]' "$TMP_DIR/q-ordersRate.json" | cut -c1-6)"
    LD4_DETAIL="all 8 queries succeeded non-empty during ${LOAD_DURATION_SECONDS}s at ${LOAD_RATE} orders/s; ordersRate=$RATE; readyPods>=1 for inventory and payment; k6 ok"
  fi
fi
if [ "$LD4_OK" = 1 ]; then record PASS LD4_live_queries "$LD4_DETAIL"; else record FAIL LD4_live_queries "$LD4_DETAIL"; fi

# ---------------------------------------------------------------------------
# LD5: read APIs through the dashboard origin
if [ -n "$DASH" ]; then
  s_code="$(curl -sS -o "$TMP_DIR/stock.json" -w '%{http_code}' --max-time 15 "$DASH/api/events/$EVENT_ID/stock" || echo 000)"
  a_code="$(code_of GET "$DASH/api/orders/activity")"
  STOCK_NOW="$(jq -r '.stock' "$TMP_DIR/stock.json" 2>/dev/null || echo null)"
  if [ "$s_code" = 200 ] && [[ "$STOCK_NOW" =~ ^[0-9]+$ ]] && [ "$a_code" = 200 ]; then
    record PASS LD5_reads "stock($EVENT_ID)=$STOCK_NOW, activity 200"
  else
    record FAIL LD5_reads "stock status=$s_code stock=$STOCK_NOW activity status=$a_code"
  fi
else
  record FAIL LD5_reads "no dashboard origin (LD2 failed)"
fi

# ---------------------------------------------------------------------------
# LD6: the central invariant, same audit idea as autoscale-acceptance.sh
LD6_OK=0
if [ "${#LOAD_EVENT_IDS[@]}" -eq 0 ]; then
  LD6_DETAIL="no load ran (LD4 did not start it)"
else
  literals="$(printf "'%s'," "${LOAD_EVENT_IDS[@]}")"; literals="${literals%,}"
  # Wait until every order of this run has settled.
  for i in $(seq 1 60); do
    open="$(psql_q "SELECT count(*) FROM orders.orders WHERE event_id IN ($literals) AND status NOT IN ('CONFIRMED','FAILED','REJECTED')" 2>/dev/null || echo x)"
    [ "$open" = 0 ] && break
    sleep 2
  done
  bad=""
  total_orders="$(psql_q "SELECT count(*) FROM orders.orders WHERE event_id IN ($literals)")"
  # One purchase attempt = one Idempotency-Key "<RUN_ID>-<iteration>" (see autoscale-load.js), so no key may repeat.
  distinct_keys="$(psql_q "SELECT count(DISTINCT idempotency_key) FROM orders.orders WHERE event_id IN ($literals) AND idempotency_key LIKE '$RUN_ID-%'")"
  distinct_ids="$(psql_q "SELECT count(DISTINCT order_id) FROM orders.orders WHERE event_id IN ($literals)")"
  total_sold=0
  for ev in "${LOAD_EVENT_IDS[@]}"; do
    remaining="$(K exec redis-0 -- redis-cli --raw HGET "inventory:event:$ev" stock | tr -d '\r')"
    sold="$(psql_q "SELECT coalesce(sum(quantity),0) FROM orders.orders WHERE event_id='$ev' AND status='CONFIRMED'")"
    if ! [[ "$remaining" =~ ^[0-9]+$ ]] || [ "$remaining" -lt 0 ] || [ $((remaining + sold)) -ne "$LOAD_EVENT_STOCK" ]; then
      bad="$bad $ev(remaining=$remaining sold=$sold)"
    else
      total_sold=$((total_sold + sold))
    fi
  done
  main_stock="$(jq -r '.stock' "$TMP_DIR/stock.json" 2>/dev/null || echo null)"
  K6_ITER="$(sed -n 's/^ *iterations[ .]*: *\([0-9]*\).*/\1/p' "$TMP_DIR/k6.log" | head -1)"
  K6_ACCEPTED="$(sed -n 's/^ *accepted_orders[ .]*: *\([0-9]*\).*/\1/p' "$TMP_DIR/k6.log" | head -1)"
  if [ "${open:-x}" != 0 ]; then
    LD6_DETAIL="orders still unsettled after 120 s: $open"
  elif [ -n "$bad" ]; then
    LD6_DETAIL="stock not conserved for:$bad"
  elif [ "$total_sold" -le 0 ]; then
    LD6_DETAIL="no ticket was confirmed under load (confirmed=$total_sold)"
  elif [ "$distinct_keys" != "$total_orders" ] || [ "$distinct_ids" != "$total_orders" ]; then
    LD6_DETAIL="duplicate orders: orders=$total_orders distinct idempotency keys=$distinct_keys distinct order ids=$distinct_ids"
  elif ! [[ "$main_stock" =~ ^[0-9]+$ ]]; then
    LD6_DETAIL="$EVENT_ID stock=$main_stock is not a non-negative number"
  else
    LD6_OK=1
    LD6_DETAIL="$total_orders orders (k6 iterations=${K6_ITER:-?}, accepted=${K6_ACCEPTED:-?}) settled, none duplicated; load events: confirmed $total_sold of $((LOAD_EVENTS * LOAD_EVENT_STOCK)) tickets, remaining+sold=$LOAD_EVENT_STOCK each, never negative; $EVENT_ID stock=$main_stock >= 0"
  fi
  for ev in "${LOAD_EVENT_IDS[@]}"; do K exec redis-0 -- redis-cli DEL "inventory:event:$ev" >/dev/null 2>&1; done
fi
if [ "$LD6_OK" = 1 ]; then record PASS LD6_invariant "$LD6_DETAIL"; else record FAIL LD6_invariant "$LD6_DETAIL"; fi

# ---------------------------------------------------------------------------
# LD7: Compose acceptance still passes (opt-in, resets the Compose Postgres volume)
if [ "$WITH_COMPOSE" = 1 ]; then
  if scripts/dashboard-acceptance.sh >"$TMP_DIR/compose-acceptance.log" 2>&1; then
    record PASS LD7_compose_still_passes "$(tail -3 "$TMP_DIR/compose-acceptance.log" | head -1)"
  else
    tail -40 "$TMP_DIR/compose-acceptance.log" >&2
    record FAIL LD7_compose_still_passes "scripts/dashboard-acceptance.sh failed"
  fi
else
  echo "SKIP LD7_compose_still_passes - run with --with-compose (resets the Compose Postgres volume)"
fi

REACHED_END=1
if [ "$FAILED" = 0 ]; then echo "ALL LIVE DASHBOARD CHECKS PASSED"; else echo "LIVE DASHBOARD CHECKS FAILED"; fi
