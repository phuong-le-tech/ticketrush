#!/usr/bin/env bash
# Verifies M1.1 infra and repo hygiene: compose stack, topics, schemas, .env.example, dashes.
# Usage: scripts/verify-scaffold.sh   (requires Docker)
set -u
cd "$(dirname "$0")/.."

COMPOSE="deploy/compose/docker-compose.yml"
FAILS=0
fail() { echo "FAIL: $*"; FAILS=$((FAILS + 1)); }
ok() { echo "OK: $*"; }

# --- .env.example and git hygiene (no Docker needed) ---
EXPECTED_VARS="KAFKA_BOOTSTRAP_SERVERS POSTGRES_URL POSTGRES_USER POSTGRES_PASSWORD REDIS_HOST REDIS_PORT PAYMENT_PROVIDER_URL PAYMENT_PROVIDER_TIMEOUT_MS INVENTORY_SEED_EVENT_ID INVENTORY_SEED_STOCK INVENTORY_SEED_PRICE_CENTS INVENTORY_SEED_CURRENCY RESERVATION_TTL_SECONDS"

if [ -f .env.example ]; then
  ACTUAL_VARS=$(grep -E '^[A-Z][A-Z0-9_]*=' .env.example | cut -d= -f1 | sort)
  WANT_VARS=$(printf '%s\n' $EXPECTED_VARS | sort)
  if [ "$ACTUAL_VARS" = "$WANT_VARS" ]; then ok ".env.example has exactly the 13 variables"; else
    fail ".env.example variables differ"; diff <(echo "$WANT_VARS") <(echo "$ACTUAL_VARS"); fi
  for p in 8080 8081 8082 8083 8084 8085; do
    grep -E '^#.*'"$p" .env.example >/dev/null || fail "port $p not documented in a comment of .env.example"
  done
else
  fail ".env.example missing"
fi

if git ls-files | grep -x '.env' >/dev/null; then fail ".env is tracked"; else ok "no .env tracked"; fi
git check-ignore -q .env && ok ".env is gitignored" || fail ".env not in .gitignore"

DASHES=$(git ls-files -z | xargs -0 grep -l -P '\x{2013}|\x{2014}' 2>/dev/null || true)
if [ -z "$DASHES" ]; then ok "no en/em dash in tracked files"; else fail "en/em dash found in: $DASHES"; fi

# --- Compose stack ---
if [ ! -f "$COMPOSE" ]; then
  fail "$COMPOSE missing"
  echo "RESULT: $FAILS failure(s)"; exit 1
fi

cleanup() { docker compose -f "$COMPOSE" down -v >/dev/null 2>&1; }
trap cleanup EXIT

docker compose -f "$COMPOSE" up -d --wait --wait-timeout 180 || fail "compose up/--wait failed"
docker compose -f "$COMPOSE" ps -a

# The init service must have exited with code 0.
INIT_CODES=$(docker compose -f "$COMPOSE" ps -a --format '{{.Service}} {{.ExitCode}} {{.State}}' | grep -i init)
echo "init services: $INIT_CODES"
[ -n "$INIT_CODES" ] || fail "no init service found"
echo "$INIT_CODES" | grep -v ' 0 exited' && fail "an init service did not exit 0"

KAFKA=$(docker compose -f "$COMPOSE" ps -q kafka)
DESC=$(docker exec "$KAFKA" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe 2>&1)
echo "$DESC"
for t in orders.requested tickets.reserved orders.rejected payments.result orders.confirmed tickets.released; do
  LINE=$(echo "$DESC" | grep -E "^Topic: $t[[:space:]]")
  echo "$LINE" | grep -q 'PartitionCount: 12' || fail "$t: PartitionCount != 12"
  echo "$LINE" | grep -q 'ReplicationFactor: 1' || fail "$t: ReplicationFactor != 1"
done
[ "$(echo "$DESC" | grep -cE '^Topic: ')" = "6" ] || fail "expected exactly 6 topics"

PG=$(docker compose -f "$COMPOSE" ps -q postgres)
SCHEMAS=$(docker exec "$PG" sh -c 'psql -U "$POSTGRES_USER" -d "${POSTGRES_DB:-$POSTGRES_USER}" -Atc "select schema_name from information_schema.schemata"' 2>&1)
echo "schemas: $SCHEMAS"
for s in orders payments inventory; do
  echo "$SCHEMAS" | grep -qx "$s" || fail "schema $s missing"
done

echo "RESULT: $FAILS failure(s)"
[ "$FAILS" = 0 ]
