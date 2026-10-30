#!/usr/bin/env bash
# M3 static chart checks (AC1, AC2, AC3). Needs helm, yq (mikefarah v4) and jq; no cluster.
#
# Usage:
#   scripts/m3-chart-check.sh                  helm lint + helm template of deploy/helm/ticketrush, then checks
#   scripts/m3-chart-check.sh --manifest FILE  checks only, on a pre-rendered manifest (YAML or JSON)
#   scripts/m3-chart-check.sh --selftest [BASE_MANIFEST]
#                                              proves each check fails on a mutated copy of the rendered chart
#                                              (or of BASE_MANIFEST when given)
#
# Checks (a failing check prints "FAIL <name>: <reason>" and the script exits non-zero):
#   check_AC1_kafka      Kafka CR: 3 brokers (KafkaNodePool), min.insync.replicas=2, default.replication.factor=3,
#                        offsets and transaction-state RF 3, transaction-state min ISR 2, auto-create off
#   check_AC1_topics     exactly 12 topics: six sources and their .DLT topics, partitions 12 and RF 3
#   check_AC8_dltTopology  Helm DLT topology plus Compose init: the same 12 topics, partitions 12 and RF 1
#   check_AC2_hardening  every app Deployment: readiness and liveness probe paths, cpu/memory requests and
#                        limits, runAsNonRoot, terminationGracePeriodSeconds > 30 (exactly 6 Deployments)
#   check_AC3_no_plaintext  credential-looking env vars come from a Secret, never a literal; the Secret exists
set -u
cd "$(dirname "$0")/.."

CHART="deploy/helm/ticketrush"
RELEASE="ticketrush"
SOURCE_TOPICS='["orders.requested","tickets.reserved","orders.rejected","payments.result","orders.confirmed","tickets.released"]'
EXPECTED_TOPICS="" # Derived after require_tools, so a missing jq is reported cleanly.
SCRATCH="$(mktemp -d "${TMPDIR:-/tmp}/m3-chart-check.XXXXXX")"
trap 'rm -rf "$SCRATCH"' EXIT

fail() { echo "FAIL $1: $2"; FAILED=1; }

require_tools() {
  local missing=0
  for t in helm yq jq; do
    command -v "$t" > /dev/null 2>&1 || { echo "missing tool: $t (brew install helm yq jq)"; missing=1; }
  done
  return $missing
}

# Normalise any manifest (multi-doc YAML, or a JSON array) to one JSON array of documents.
normalise() {
  [ -s "$1" ] || return 1
  yq -o=json -I=0 '.' "$1" > "$SCRATCH/normalise.tmp" || return 1
  jq -s 'map(if type == "array" then .[] else . end) | map(select(. != null))' "$SCRATCH/normalise.tmp"
}

check_AC1_kafka() {
  local docs="$1" out
  out=$(jq -r '
    . as $d
    | ($d | map(select(.kind == "Kafka"))) as $kafka
    | ($d | map(select(.kind == "KafkaNodePool" and ((.spec.roles // []) | index("broker"))))) as $pools
    | ($kafka[0].spec.kafka.config // {}) as $c
    | [ (if ($kafka | length) != 1 then "expected exactly 1 Kafka CR, found \($kafka | length)" else empty end),
        (if ($pools | map(.spec.replicas) | add // 0) != 3 then "expected 3 brokers in KafkaNodePool, found \($pools | map(.spec.replicas) | add // 0)" else empty end),
        (if ($c["min.insync.replicas"] | tostring) != "2" then "min.insync.replicas must be 2" else empty end),
        (if ($c["default.replication.factor"] | tostring) != "3" then "default.replication.factor must be 3" else empty end),
        (if ($c["offsets.topic.replication.factor"] | tostring) != "3" then "offsets.topic.replication.factor must be 3" else empty end),
        (if ($c["transaction.state.log.replication.factor"] | tostring) != "3" then "transaction.state.log.replication.factor must be 3" else empty end),
        (if ($c["transaction.state.log.min.isr"] | tostring) != "2" then "transaction.state.log.min.isr must be 2" else empty end),
        (if ($c["auto.create.topics.enable"] | tostring) != "false" then "auto.create.topics.enable must be false" else empty end)
      ] | .[]' "$docs")
  [ -z "$out" ] || while IFS= read -r line; do fail check_AC1_kafka "$line"; done <<< "$out"
}

check_AC1_topics() {
  local docs="$1" out
  out=$(jq -r --argjson expected "$EXPECTED_TOPICS" '
    map(select(.kind == "KafkaTopic")) as $t
    | ($t | map(.spec.topicName // .metadata.name) | sort) as $names
    | [ (if $names != ($expected | sort) then "topics are \($names | join(",")), expected \($expected | sort | join(","))" else empty end),
        ($t[] | select(.spec.partitions != 12) | "topic \(.spec.topicName // .metadata.name) has \(.spec.partitions) partitions, expected 12"),
        ($t[] | select(.spec.replicas != 3) | "topic \(.spec.topicName // .metadata.name) has replicas \(.spec.replicas), expected 3")
      ] | .[]' "$docs")
  [ -z "$out" ] || while IFS= read -r line; do fail "${2:-check_AC1_topics}" "$line"; done <<< "$out"
}

# Exercise the repository's trusted init shell with Kafka CLI calls captured, not sent to a broker.
# This checks behavior, allowing an explicit topic list or source loops constructing "$t.DLT".
# Compose escapes shell dollars as $$; decode them just as Compose does.
check_AC8_dltTopology() {
  local docs="$1" init_file="$2" init out
  check_AC1_topics "$docs" check_AC8_dltTopology
  init=$(< "$init_file")
  init=${init//\$\$/\$}
  init=${init//\/opt\/kafka\/bin\/kafka-topics.sh/kafka-topics.sh}
  : > "$SCRATCH/compose-topics.tsv"
  if ! (
    kafka-topics.sh() {
      local topic="" partitions="" replicas="" create=0
      while [ "$#" -gt 0 ]; do
        case "$1" in
          --create) create=1; shift ;;
          --topic=*) topic="${1#*=}"; shift ;;
          --partitions=*) partitions="${1#*=}"; shift ;;
          --replication-factor=*) replicas="${1#*=}"; shift ;;
          --topic) topic="${2:-}"; shift 2 || return 1 ;;
          --partitions) partitions="${2:-}"; shift 2 || return 1 ;;
          --replication-factor) replicas="${2:-}"; shift 2 || return 1 ;;
          *) shift ;;
        esac
      done
      [ "$create" = 1 ] || return 0
      printf '%s\t%s\t%s\n' "$topic" "$partitions" "$replicas" >> "$TOPIC_CAPTURE"
    }
    export -f kafka-topics.sh
    export TOPIC_CAPTURE="$SCRATCH/compose-topics.tsv"
    bash -eu -c "$init"
  ) > "$SCRATCH/compose-init.log" 2>&1; then
    fail check_AC8_dltTopology "Compose kafka-init could not be evaluated with the recording Kafka CLI (no broker used)"
    return
  fi
  out=$(jq -Rnr --argjson expected "$EXPECTED_TOPICS" '
    [inputs | split("\t") | {name: .[0], partitions: .[1], replicas: .[2]}] as $t
    | ($t | map(.name) | sort) as $names
    | [ (if $names != ($expected | sort) then "Compose init topics are \($names | join(",")), expected exactly the six sources and their .DLT topics" else empty end),
        ($t[] | select(.partitions != "12") | "Compose topic \(.name) has partitions \(.partitions), expected 12"),
        ($t[] | select(.replicas != "1") | "Compose topic \(.name) has RF \(.replicas), expected 1")
      ] | .[]' < "$SCRATCH/compose-topics.tsv")
  [ -z "$out" ] || while IFS= read -r line; do fail check_AC8_dltTopology "$line"; done <<< "$out"
}

check_AC2_hardening() {
  local docs="$1" out
  out=$(jq -r '
    map(select(.kind == "Deployment")) as $dep
    | [ (if ($dep | length) != 6 then "expected 6 app Deployments, found \($dep | length)" else empty end),
        ($dep[] | .metadata.name as $n | .spec.template.spec as $p
          | ($p.terminationGracePeriodSeconds // 30) as $grace
          | (if $grace <= 30 then "\($n): terminationGracePeriodSeconds is \($grace), must be > 30" else empty end),
            ($p.containers[] | . as $c
              | (if $c.readinessProbe.httpGet.path != "/actuator/health/readiness" then "\($n): readiness probe path must be /actuator/health/readiness" else empty end),
                (if $c.livenessProbe.httpGet.path != "/actuator/health/liveness" then "\($n): liveness probe path must be /actuator/health/liveness" else empty end),
                (if ($c.resources.requests.cpu // "") == "" or ($c.resources.requests.memory // "") == "" then "\($n): cpu and memory requests are required" else empty end),
                (if ($c.resources.limits.cpu // "") == "" or ($c.resources.limits.memory // "") == "" then "\($n): cpu and memory limits are required" else empty end),
                (if (($p.securityContext.runAsNonRoot // false) or ($c.securityContext.runAsNonRoot // false)) != true then "\($n): runAsNonRoot must be true" else empty end)))
      ] | .[]' "$docs")
  [ -z "$out" ] || while IFS= read -r line; do fail check_AC2_hardening "$line"; done <<< "$out"
}

check_AC3_no_plaintext() {
  local docs="$1" out
  out=$(jq -r '
    (map(select(.kind == "Secret")) | map((.stringData // {}) + (.data // {}) | to_entries | map(.value | tostring)) | add // []
      | map(select(length >= 4))) as $secretValues
    | [ (if (map(select(.kind == "Secret")) | length) == 0 then "no Secret rendered" else empty end),
        (.[] | select(.kind == "Deployment" or .kind == "StatefulSet") | .metadata.name as $n
          | .spec.template.spec.containers[] | (.env // [])[]
          | select(.value != null and .value != "") as $e
          | (if ($e.name | test("PASSWORD|SECRET|USER|TOKEN|KEY"; "i")) then "\($n): env \($e.name) is a literal, must use valueFrom.secretKeyRef" else empty end),
            (if ($secretValues | index($e.value)) != null then "\($n): env \($e.name) repeats a Secret value as a literal" else empty end)),
        (.[] | select(.kind == "Deployment") | .metadata.name as $n
          | .spec.template.spec.containers[] | (.env // [])[]
          | select(.name | test("PASSWORD|SECRET|USER|TOKEN|KEY"; "i"))
          | select(.valueFrom.secretKeyRef == null)
          | "\($n): env \(.name) has no valueFrom.secretKeyRef")
      ] | .[]' "$docs")
  [ -z "$out" ] || while IFS= read -r line; do fail check_AC3_no_plaintext "$line"; done <<< "$out"
}

run_checks() {
  FAILED=0
  check_AC1_kafka "$1"
  check_AC1_topics "$1"
  check_AC2_hardening "$1"
  check_AC3_no_plaintext "$1"
  check_AC8_dltTopology "$1" "${2:-$SCRATCH/compose-init.sh}"
  return $FAILED
}

# Mutation self-test: a check that cannot fail is worthless. Each mutated copy must make the named check fail.
expect_failure() {  # name, expected-check, jq filter
  local name="$1" check="$2" filter="$3" mutated="$SCRATCH/mutated.json" output
  jq "$filter" "$SCRATCH/docs.json" > "$mutated"
  output=$(run_checks "$mutated")
  if echo "$output" | grep -q "^FAIL $check:"; then
    echo "ok   $name (rejected by $check)"
  else
    echo "FAIL $name: mutation was not rejected by $check"
    SELFTEST_FAILED=1
  fi
}

expect_compose_failure() { # name, jq string mutation
  local name="$1" filter="$2" output
  if ! jq -Rs -j "$filter" "$SCRATCH/compose-init.sh" > "$SCRATCH/mutated-init.sh"; then
    echo "FAIL $name: cannot construct Compose mutation"
    SELFTEST_FAILED=1
    return
  fi
  if cmp -s "$SCRATCH/compose-init.sh" "$SCRATCH/mutated-init.sh"; then
    echo "FAIL $name: Compose mutation made no change"
    SELFTEST_FAILED=1
    return
  fi
  output=$(run_checks "$SCRATCH/docs.json" "$SCRATCH/mutated-init.sh")
  if echo "$output" | grep -q '^FAIL check_AC8_dltTopology:'; then
    echo "ok   $name (rejected by check_AC8_dltTopology)"
  else
    echo "FAIL $name: mutation was not rejected by check_AC8_dltTopology"
    SELFTEST_FAILED=1
  fi
}

selftest() {
  SELFTEST_FAILED=0
  expect_failure selftest_AC8_missing_DLT check_AC8_dltTopology \
    '(map(select(.kind == "KafkaTopic" and ((.spec.topicName // .metadata.name) | endswith(".DLT")))) | .[0].metadata.name) as $n | map(select(.kind != "KafkaTopic" or .metadata.name != $n))'
  expect_failure selftest_AC8_DLT_bad_partitions check_AC8_dltTopology \
    'map(if .kind == "KafkaTopic" and ((.spec.topicName // .metadata.name) | endswith(".DLT")) then .spec.partitions = 6 else . end)'
  expect_failure selftest_AC8_DLT_bad_RF check_AC8_dltTopology \
    'map(if .kind == "KafkaTopic" and ((.spec.topicName // .metadata.name) | endswith(".DLT")) then .spec.replicas = 1 else . end)'
  expect_compose_failure selftest_AC8_compose_missing_DLT 'gsub("\\.DLT"; ".notDLT")'
  expect_compose_failure selftest_AC8_compose_bad_partitions 'gsub("--partitions[ =]+12"; "--partitions 6")'
  expect_compose_failure selftest_AC8_compose_bad_RF 'gsub("--replication-factor[ =]+1"; "--replication-factor 3")'
  expect_failure selftest_AC1_mutation_fails_one_broker check_AC1_kafka \
    'map(if .kind == "KafkaNodePool" then .spec.replicas = 1 else . end)'
  expect_failure selftest_AC1_mutation_fails_min_isr check_AC1_kafka \
    'map(if .kind == "Kafka" then .spec.kafka.config["min.insync.replicas"] = 1 else . end)'
  expect_failure selftest_AC1_mutation_fails_topic_6_partitions check_AC1_topics \
    '(map(select(.kind == "KafkaTopic")) | .[0].metadata.name) as $n | map(if .kind == "KafkaTopic" and .metadata.name == $n then .spec.partitions = 6 else . end)'
  expect_failure selftest_AC2_mutation_fails_no_readiness_probe check_AC2_hardening \
    '(map(select(.kind == "Deployment")) | .[0].metadata.name) as $n | map(if .kind == "Deployment" and .metadata.name == $n then del(.spec.template.spec.containers[0].readinessProbe) else . end)'
  expect_failure selftest_AC2_mutation_fails_grace_30 check_AC2_hardening \
    '(map(select(.kind == "Deployment")) | .[0].metadata.name) as $n | map(if .kind == "Deployment" and .metadata.name == $n then .spec.template.spec.terminationGracePeriodSeconds = 30 else . end)'
  expect_failure selftest_AC3_mutation_fails_literal_password check_AC3_no_plaintext \
    '(map(select(.kind == "Deployment")) | .[0].metadata.name) as $n | map(if .kind == "Deployment" and .metadata.name == $n then .spec.template.spec.containers[0].env += [{"name": "DB_PASSWORD", "value": "hunter2-literal"}] else . end)'
  return $SELFTEST_FAILED
}

require_tools || exit 2
EXPECTED_TOPICS=$(jq -cn --argjson sources "$SOURCE_TOPICS" '$sources + ($sources | map(. + ".DLT"))')
# Read only the init command, not the environment or the Docker daemon. A string or argv list is supported.
yq -o=json '.services."kafka-init".command' deploy/compose/docker-compose.yml \
  | jq -er 'if type == "array" then join("\n") elif type == "string" then . else error("missing kafka-init command") end' \
  > "$SCRATCH/compose-init.sh" || { echo "FAIL check_AC8_dltTopology: cannot read Compose kafka-init command"; exit 1; }

MODE="chart"
MANIFEST=""
case "${1:-}" in
  --manifest) MODE="manifest"; MANIFEST="${2:?--manifest needs a file}" ;;
  --selftest) MODE="selftest"; MANIFEST="${2:-}" ;;
  "") ;;
  *) echo "usage: $0 [--manifest FILE | --selftest [BASE_MANIFEST]]"; exit 2 ;;
esac

if [ -n "$MANIFEST" ]; then
  normalise "$MANIFEST" > "$SCRATCH/docs.json" || { echo "FAIL manifest: cannot parse $MANIFEST"; exit 1; }
else
  if ! helm lint "$CHART" --strict; then
    echo "FAIL helm_lint: helm lint reported problems"
    exit 1
  fi
  if ! helm template "$RELEASE" "$CHART" > "$SCRATCH/rendered.yaml"; then
    echo "FAIL helm_template: helm template failed"
    exit 1
  fi
  normalise "$SCRATCH/rendered.yaml" > "$SCRATCH/docs.json"
fi

if [ "$MODE" = "selftest" ]; then
  run_checks "$SCRATCH/docs.json" || { echo "FAIL selftest: the unmutated chart does not pass the checks"; exit 1; }
  selftest || exit 1
  echo "RESULT: m3-chart-check selftest passed"
  exit 0
fi

if run_checks "$SCRATCH/docs.json"; then
  echo "RESULT: m3-chart-check passed"
else
  echo "RESULT: m3-chart-check failed"
  exit 1
fi
