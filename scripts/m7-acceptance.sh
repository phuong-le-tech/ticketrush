#!/usr/bin/env bash
# M7 acceptance: proves the dashboard and the service-owned read APIs by execution.
#
#   scripts/m7-acceptance.sh
#
# Steps, in order:
#   1. preflight: Docker daemon, Java 21, the pinned Node image (DESIGN.md section 1)
#   2. backend module tests: inventory-service + order-service with -am (Testcontainers Redis/Postgres)
#   3. dashboard: npm ci, Vitest (plus the Node proxy test) and the production build inside node:24.22.2
#   4. Compose up with the dashboard image: resets the local anonymous Postgres
#      volume, rebuilds, waits for health, then waits for both read APIs through
#      the dashboard proxy (the Java services have no Compose healthcheck)
#   5. end-to-end through the nginx dashboard on http://localhost:4200:
#      /healthz 200, stock read, POST /api/orders 202, that order CONFIRMED in
#      /api/orders/activity within 60 s, stock decreased by the bought quantity
#
# The stack is left running on success so the page can be inspected or captured;
# stop it with: docker compose -f deploy/compose/docker-compose.yml down
# kind, Helm and k6 are intentionally NOT used: the M7 Helm chart is out of scope.
set -euo pipefail

cd "$(dirname "$0")/.."

NODE_IMAGE="${NODE_IMAGE:-node:24.22.2}"
COMPOSE_FILE="deploy/compose/docker-compose.yml"
BASE_URL="${BASE_URL:-http://localhost:4200}"
EVENT_ID="${EVENT_ID:-concert-1}"
QTY="${QTY:-1}"
CONFIRM_TIMEOUT_SECONDS="${CONFIRM_TIMEOUT_SECONDS:-60}"
POLL_SECONDS="${POLL_SECONDS:-2}"

TMP_DIR="$(mktemp -d)"
cleanup() { rm -rf "$TMP_DIR"; }
trap cleanup EXIT

step() { printf '\n==> %s\n' "$*"; }
ok() { printf '    PASS: %s\n' "$*"; }
die() { printf '\nFAIL: %s\n' "$*" >&2; exit 1; }

http_get() { # url outfile -> prints status code
  curl -sS -o "$2" -w '%{http_code}' --max-time 15 "$1" || echo "000"
}

ready_wait() { # url label timeout_seconds
  local url="$1" label="$2" timeout="${3:-180}"
  local deadline=$(( $(date +%s) + timeout )) code
  while [ "$(date +%s)" -lt "$deadline" ]; do
    code="$(http_get "$url" "$TMP_DIR/ready.json")"
    if [ "$code" = "200" ]; then
      printf '    ready: %s -> 200\n' "$label"
      return 0
    fi
    sleep 2
  done
  step_fail "$label did not answer 200 through $BASE_URL within ${timeout}s"
}

step_fail() {
  printf '\nFAIL: %s\n' "$*" >&2
  printf '\n--- compose ps ---\n' >&2
  docker compose -f "$COMPOSE_FILE" ps >&2 || true
  printf '\n--- last 40 lines per service ---\n' >&2
  docker compose -f "$COMPOSE_FILE" logs --tail=40 --no-color >&2 || true
  exit 1
}

# ---------------------------------------------------------------------------
step "Step 1/5: preflight (Docker, Java 21, pinned Node image $NODE_IMAGE)"
# ---------------------------------------------------------------------------
docker info --format '    Docker server {{.ServerVersion}}' \
  || die "Docker daemon unreachable; start Docker.app and rerun"
JAVA_HOME="${JAVA_HOME:-$(/usr/libexec/java_home -v 21 2>/dev/null || true)}"
[ -n "$JAVA_HOME" ] || die "Java 21 not found (/usr/libexec/java_home -v 21)"
export JAVA_HOME
printf '    Java: %s\n' "$("$JAVA_HOME/bin/java" -version 2>&1 | head -1)"
"$JAVA_HOME/bin/java" -version 2>&1 | head -1 | grep -q 'version "21' \
  || die "Java 21 required, found: $("$JAVA_HOME/bin/java" -version 2>&1 | head -1)"
NODE_VERSION="$(docker run --rm "$NODE_IMAGE" node --version)"
printf '    Node in container: %s\n' "$NODE_VERSION"
case "$NODE_VERSION" in
  v24.1[5-9].*|v24.[2-9][0-9].*|v[3-9][0-9].*) ;;
  *) die "Node in $NODE_IMAGE is $NODE_VERSION, Angular 22 needs >= 24.15" ;;
esac
ok "toolchain ready"

# ---------------------------------------------------------------------------
step "Step 2/5: backend module tests (inventory-service, order-service)"
# ---------------------------------------------------------------------------
mvn -B verify -pl inventory-service,order-service -am >"$TMP_DIR/mvn.log" 2>&1 \
  || { tail -80 "$TMP_DIR/mvn.log" >&2; step_fail "mvn verify failed for inventory-service/order-service"; }
grep -E '^\[INFO\] (BUILD SUCCESS|Total time)' "$TMP_DIR/mvn.log" | sed 's/^/    /'
REPORTS="$(find inventory-service/target order-service/target -name 'TEST-*.xml' -o -name '*-results.xml' 2>/dev/null | wc -l | tr -d ' ')"
printf '    Surefire/Failsafe report files: %s\n' "$REPORTS"
[ "$REPORTS" -gt 0 ] || step_fail "no test reports produced"
ok "backend module tests green"

# ---------------------------------------------------------------------------
step "Step 3/5: dashboard npm ci + Vitest + production build in $NODE_IMAGE"
# ---------------------------------------------------------------------------
docker run --rm \
  -u "$(id -u):$(id -g)" \
  -e HOME=/tmp -e npm_config_cache=/tmp/npm-cache -e NG_CLI_ANALYTICS=false -e NO_COLOR=1 \
  -v "$PWD/dashboard":/app -w /app \
  "$NODE_IMAGE" sh -c 'npm ci --no-audit --no-fund && npm test && npm run build' \
  >"$TMP_DIR/dashboard.log" 2>&1 \
  || { tail -60 "$TMP_DIR/dashboard.log" >&2; step_fail "dashboard npm ci/test/build failed"; }
grep -E 'Test Files|Tests +[0-9]|Application bundle generation complete|Build at' "$TMP_DIR/dashboard.log" | sed 's/^/    /' || true
ok "dashboard tests and production build green"

# ---------------------------------------------------------------------------
step "Step 4/5: compose up with the dashboard (clean local data, build images, wait for health)"
# The Compose stack keeps Postgres data in an anonymous volume, so a container
# recreated from an older milestone keeps an old Flyway history. Reset it: this
# is the local development stack only (no real data), and acceptance needs the
# seeded concert-1 event and a clean saga history.
docker compose -f "$COMPOSE_FILE" down --volumes --remove-orphans >"$TMP_DIR/compose-down.log" 2>&1 || true
docker compose -f "$COMPOSE_FILE" up -d --build --wait --wait-timeout 900 >"$TMP_DIR/compose.log" 2>&1 \
  || { tail -60 "$TMP_DIR/compose.log" >&2; step_fail "docker compose up --wait failed"; }
docker compose -f "$COMPOSE_FILE" ps --format '{{.Service}} {{.State}} {{.Status}}' | sed 's/^/    /'
DASHBOARD_UID="$(docker compose -f "$COMPOSE_FILE" exec -T dashboard id -u | tr -d '\r')"
printf '    dashboard container uid: %s\n' "$DASHBOARD_UID"
[ "$DASHBOARD_UID" != "0" ] || step_fail "dashboard container runs as root (uid 0)"
# compose --wait only covers services with a healthcheck; the Java services have
# none, so wait on the two read APIs through the dashboard proxy.
ready_wait "$BASE_URL/api/events/$EVENT_ID/stock" "GET /api/events/$EVENT_ID/stock" 240
ready_wait "$BASE_URL/api/orders/activity?limit=20" "GET /api/orders/activity" 240
ok "stack healthy, dashboard non-root (uid $DASHBOARD_UID), read APIs answering"

# ---------------------------------------------------------------------------
step "Step 5/5: end-to-end through $BASE_URL"
# ---------------------------------------------------------------------------
HEALTH_CODE="$(http_get "$BASE_URL/healthz" "$TMP_DIR/healthz.txt")"
printf '    GET /healthz -> %s (%s)\n' "$HEALTH_CODE" "$(tr -d '\n' <"$TMP_DIR/healthz.txt")"
[ "$HEALTH_CODE" = "200" ] || step_fail "/healthz returned $HEALTH_CODE"

STOCK_CODE="$(http_get "$BASE_URL/api/events/$EVENT_ID/stock" "$TMP_DIR/stock-before.json")"
STOCK_BEFORE="$(jq -r '.stock' "$TMP_DIR/stock-before.json" 2>/dev/null || echo null)"
printf '    GET /api/events/%s/stock -> %s %s\n' "$EVENT_ID" "$STOCK_CODE" "$(cat "$TMP_DIR/stock-before.json")"
[ "$STOCK_CODE" = "200" ] || step_fail "stock read returned $STOCK_CODE"
[ "$STOCK_BEFORE" != "null" ] || step_fail "stock read has no numeric stock field"

ACTIVITY_CODE="$(http_get "$BASE_URL/api/orders/activity?limit=20" "$TMP_DIR/activity-before.json")"
[ "$ACTIVITY_CODE" = "200" ] || step_fail "/api/orders/activity returned $ACTIVITY_CODE"
jq -e 'has("windowSeconds") and has("ordersInWindow") and (.orders | type == "array")' \
  "$TMP_DIR/activity-before.json" >/dev/null \
  || step_fail "activity payload missing windowSeconds/ordersInWindow/orders"
if jq -e '.orders | any(has("customerId") or has("idempotencyKey"))' "$TMP_DIR/activity-before.json" >/dev/null; then
  step_fail "activity payload leaks customerId or idempotencyKey"
fi
printf '    GET /api/orders/activity -> %s (windowSeconds=%s, ordersInWindow=%s, orders=%s)\n' \
  "$ACTIVITY_CODE" \
  "$(jq -r '.windowSeconds' "$TMP_DIR/activity-before.json")" \
  "$(jq -r '.ordersInWindow' "$TMP_DIR/activity-before.json")" \
  "$(jq -r '.orders | length' "$TMP_DIR/activity-before.json")"
ok "dashboard serves, stock read works, activity payload is public"

KEY="$(uuidgen | tr '[:upper:]' '[:lower:]')-m7-acceptance"
CUSTOMER="web-$(uuidgen | tr '[:upper:]' '[:lower:]')"
printf '    POST /api/orders eventId=%s quantity=%s customerId=%s Idempotency-Key=%s\n' \
  "$EVENT_ID" "$QTY" "$CUSTOMER" "$KEY"
POST_CODE="$(curl -sS -o "$TMP_DIR/post.json" -w '%{http_code}' --max-time 20 \
  -X POST "$BASE_URL/api/orders" \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: $KEY" \
  -d "{\"eventId\":\"$EVENT_ID\",\"customerId\":\"$CUSTOMER\",\"quantity\":$QTY}" || echo "000")"
printf '    POST /api/orders -> %s %s\n' "$POST_CODE" "$(cat "$TMP_DIR/post.json")"
[ "$POST_CODE" = "202" ] || step_fail "POST /api/orders returned $POST_CODE, expected 202"
ORDER_ID="$(jq -r '.orderId' "$TMP_DIR/post.json")"
[ -n "$ORDER_ID" ] && [ "$ORDER_ID" != "null" ] || step_fail "202 response has no orderId"

printf '    polling activity for order %s to become CONFIRMED (timeout %ss)\n' "$ORDER_ID" "$CONFIRM_TIMEOUT_SECONDS"
DEADLINE=$(( $(date +%s) + CONFIRM_TIMEOUT_SECONDS ))
STATUS=""
while [ "$(date +%s)" -lt "$DEADLINE" ]; do
  CODE="$(http_get "$BASE_URL/api/orders/activity?limit=20" "$TMP_DIR/activity.json")"
  if [ "$CODE" = "200" ]; then
    STATUS="$(jq -r --arg id "$ORDER_ID" '.orders[] | select(.orderId == $id) | .status' "$TMP_DIR/activity.json" | head -1)"
    printf '    %s order %s -> %s\n' "$(date +%H:%M:%S)" "$ORDER_ID" "${STATUS:-absent from last 20}"
    [ "$STATUS" = "CONFIRMED" ] && break
    if [ "${STATUS:-}" = "FAILED" ] || [ "${STATUS:-}" = "REJECTED" ]; then
      jq -r --arg id "$ORDER_ID" '.orders[] | select(.orderId == $id) | "    failureReason: \(.failureReason)"' "$TMP_DIR/activity.json" >&2
      step_fail "order $ORDER_ID reached terminal status $STATUS"
    fi
  else
    printf '    %s activity read returned %s, retrying\n' "$(date +%H:%M:%S)" "$CODE"
  fi
  sleep "$POLL_SECONDS"
done
[ "$STATUS" = "CONFIRMED" ] || step_fail "order $ORDER_ID not CONFIRMED within ${CONFIRM_TIMEOUT_SECONDS}s"
jq -r --arg id "$ORDER_ID" '.orders[] | select(.orderId == $id) |
  "    saga row: orderId=\(.orderId) eventId=\(.eventId) quantity=\(.quantity) status=\(.status) amountCents=\(.amountCents) \(.currency)"' \
  "$TMP_DIR/activity.json"
ok "order $ORDER_ID CONFIRMED within ${CONFIRM_TIMEOUT_SECONDS}s"

STOCK_CODE="$(http_get "$BASE_URL/api/events/$EVENT_ID/stock" "$TMP_DIR/stock-after.json")"
STOCK_AFTER="$(jq -r '.stock' "$TMP_DIR/stock-after.json" 2>/dev/null || echo null)"
printf '    GET /api/events/%s/stock -> %s %s\n' "$EVENT_ID" "$STOCK_CODE" "$(cat "$TMP_DIR/stock-after.json")"
[ "$STOCK_CODE" = "200" ] || step_fail "post-purchase stock read returned $STOCK_CODE"
EXPECTED=$(( STOCK_BEFORE - QTY ))
[ "$STOCK_AFTER" = "$EXPECTED" ] \
  || step_fail "stock is $STOCK_AFTER, expected $EXPECTED ($STOCK_BEFORE - $QTY)"
ok "stock decreased by $QTY ($STOCK_BEFORE -> $STOCK_AFTER)"

printf '\nALL M7 ACCEPTANCE CHECKS PASSED\n'
printf 'Dashboard still running at %s\n' "$BASE_URL"
printf 'Stop it with: docker compose -f %s down\n' "$COMPOSE_FILE"
