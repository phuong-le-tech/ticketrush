#!/usr/bin/env bash
# Milestone 2 acceptance: builds once, then runs OversellE2ETest and ReplayE2ETest in 10 separate
# Maven runs (fresh JVM and fresh containers each time). Prints pass/fail and duration per run and
# exits non-zero if any run fails. Usage: scripts/m2-acceptance.sh   (requires Docker)
set -u
cd "$(dirname "$0")/.."

RUNS=10
TESTS="OversellE2ETest,ReplayE2ETest"
LOG_DIR="e2e-tests/target/m2-acceptance"

echo "Building once (mvn install -DskipTests)..."
if ! mvn -q install -DskipTests; then
  echo "RESULT: build failed"
  exit 1
fi
rm -rf "$LOG_DIR"
mkdir -p "$LOG_DIR"

FAILS=0
for run in $(seq 1 "$RUNS"); do
  log="$LOG_DIR/run-$run.log"
  started=$(date +%s)
  # surefire:test alone reuses the compiled classes and the installed modules: no rebuild per run.
  if mvn -pl e2e-tests surefire:test -Dtest="$TESTS" > "$log" 2>&1; then
    status="PASS"
  else
    status="FAIL"
    FAILS=$((FAILS + 1))
  fi
  elapsed=$(( $(date +%s) - started ))
  summary=$(grep -E 'Tests run: [0-9]+, Failures' "$log" | tail -1 | sed 's/^\[[A-Z]*\] *//')
  echo "run $run/$RUNS: $status in ${elapsed}s - ${summary:-no test summary} (log: $log)"
done

echo "RESULT: $((RUNS - FAILS))/$RUNS runs passed"
[ "$FAILS" = 0 ]
