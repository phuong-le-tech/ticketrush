#!/usr/bin/env bash
# Fresh-clone check: the README "Run it" section works from a fresh clone.
#
#   scripts/fresh-clone-check.sh                 # clones the committed HEAD of this repo
#   REPO_URL=https://github.com/phuong-le-tech/ticketrush.git REF=main scripts/fresh-clone-check.sh
#
# Clones into a temp dir (so uncommitted files cannot hide a missing one), starts the Compose stack
# under its own project name, waits for the read API, buys one ticket, waits for CONFIRMED, checks
# that stock dropped by one, then removes the stack and its volumes.
# Needs Docker, curl, git and python3. Ports 8080 and 4200 must be free.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
REPO_URL="${REPO_URL:-$ROOT}"
REF="${REF:-HEAD}"
EVENT_ID="${EVENT_ID:-concert-1}"
BASE_URL="${BASE_URL:-http://localhost:4200}"
READY_TIMEOUT_SECONDS="${READY_TIMEOUT_SECONDS:-180}"
CONFIRM_TIMEOUT_SECONDS="${CONFIRM_TIMEOUT_SECONDS:-60}"
PROJECT="ticketrush-fresh"

fail() { echo "FAIL: $*" >&2; exit 1; }
stock() { curl -fs "$BASE_URL/api/events/$EVENT_ID/stock" | python3 -c 'import json,sys; print(json.load(sys.stdin)["stock"])'; }

echo "==> preflight"
docker info >/dev/null 2>&1 || fail "Docker daemon not reachable"
for port in 8080 4200; do
  if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then fail "port $port already in use (stop the running stack first)"; fi
done

WORK="$(mktemp -d)"
cleanup() {
  [ -d "$WORK/ticketrush" ] && docker compose -p "$PROJECT" -f "$WORK/ticketrush/deploy/compose/docker-compose.yml" down -v >/dev/null 2>&1
  rm -rf "$WORK"
}
trap cleanup EXIT

echo "==> fresh clone of $REPO_URL ($REF) into $WORK"
git clone --quiet "$REPO_URL" "$WORK/ticketrush"
git -C "$WORK/ticketrush" checkout --quiet "$REF"
cd "$WORK/ticketrush"

echo "==> docker compose up -d --build (first run builds six images, expect a few minutes)"
docker compose -p "$PROJECT" -f deploy/compose/docker-compose.yml up -d --build >/dev/null

echo "==> waiting for $BASE_URL/api/events/$EVENT_ID/stock (limit ${READY_TIMEOUT_SECONDS}s)"
deadline=$((SECONDS + READY_TIMEOUT_SECONDS))
until before="$(stock 2>/dev/null)"; do
  [ "$SECONDS" -lt "$deadline" ] || fail "read API not ready after ${READY_TIMEOUT_SECONDS}s"
  sleep 2
done
echo "    stock before: $before"

echo "==> POST /orders"
key="$(uuidgen)"
body="$(curl -fs -X POST localhost:8080/orders -H 'Content-Type: application/json' \
  -H "Idempotency-Key: $key" \
  -d "{\"eventId\":\"$EVENT_ID\",\"customerId\":\"fresh-clone-check\",\"quantity\":1}")" || fail "POST /orders did not return 2xx"
order_id="$(printf '%s' "$body" | python3 -c 'import json,sys; print(json.load(sys.stdin)["orderId"])')"
echo "    orderId: $order_id"

echo "==> waiting for CONFIRMED (limit ${CONFIRM_TIMEOUT_SECONDS}s)"
deadline=$((SECONDS + CONFIRM_TIMEOUT_SECONDS))
status="absent"
while [ "$SECONDS" -lt "$deadline" ]; do
  status="$(curl -fs "$BASE_URL/api/orders/activity?limit=50" | python3 -c '
import json, sys
oid = sys.argv[1]
rows = json.load(sys.stdin).get("orders", [])
print(next((r.get("status", "?") for r in rows if r.get("orderId") == oid), "absent"))' "$order_id" || echo "absent")"
  echo "    $(date +%H:%M:%S) $status"
  [ "$status" = "CONFIRMED" ] && break
  case "$status" in FAILED|REJECTED) fail "order ended $status";; esac
  sleep 2
done
[ "$status" = "CONFIRMED" ] || fail "order not CONFIRMED within ${CONFIRM_TIMEOUT_SECONDS}s (last: $status)"

after="$(stock)"
echo "    stock after: $after"
[ "$after" -eq $((before - 1)) ] || fail "stock went $before -> $after, expected a drop of exactly 1"

echo "ALL FRESH-CLONE CHECKS PASSED (fresh clone, order $order_id CONFIRMED, stock $before -> $after)"
