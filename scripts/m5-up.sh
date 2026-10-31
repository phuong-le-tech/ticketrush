#!/usr/bin/env bash
# M5 bootstrap: kind, pinned operators, images, monitoring and Kafka lag autoscaling.
# scripts/m5-up.sh
# M3_CLUSTER/M3_NAMESPACE keep the M3 local cluster contract.
# Set M5_CONSUMER_PROCESSING_DELAY_MS for a bounded k6 scaling demonstration; default off.
set -euo pipefail
cd "$(dirname "$0")/.."

CLUSTER="${M3_CLUSTER:-ticketrush}"
NS="${M3_NAMESPACE:-ticketrush}"
CONTEXT="kind-${CLUSTER}"
STRIMZI_CHART="oci://quay.io/strimzi-helm/strimzi-kafka-operator"
STRIMZI_VERSION="1.3.0"
CHART="deploy/helm/ticketrush"
IMAGE_REPOSITORY="ticketrush"
SERVICES=(api-gateway fake-payment-provider inventory-service notification-service order-service payment-service)
BUILD_PARALLELISM=3
INFRA_TIMEOUT="900s"
SCRATCH="$(mktemp -d "${TMPDIR:-/tmp}/m3-up.XXXXXX")"
trap 'rm -rf "$SCRATCH"' EXIT

log() { echo "==> $*"; }
kc() { kubectl --context "$CONTEXT" -n "$NS" "$@"; }

for tool in docker kind helm kubectl; do
  command -v "$tool" > /dev/null 2>&1 || { echo "missing tool: $tool (brew install kind helm)"; exit 2; }
done

# The tag is the image id, so an unchanged build keeps the same tag and helm sees no change.
build_image() {
  local service="$1" id tag
  # No provenance or SBOM attestation: they embed build time in the image id, which would change the tag on every run.
  id="$(docker build -q --provenance=false --sbom=false -f "${service}/Dockerfile" -t "${IMAGE_REPOSITORY}/${service}:build" .)"
  tag="$(echo "${id#sha256:}" | cut -c1-12)"
  docker tag "${IMAGE_REPOSITORY}/${service}:build" "${IMAGE_REPOSITORY}/${service}:${tag}"
  echo "$tag" > "${SCRATCH}/${service}.tag"
}

# Batches of BUILD_PARALLELISM (portable to the bash 3.2 that macOS ships, which has no `wait -n`).
build_images() {
  local failed=0 service pid pids=()
  for service in "${SERVICES[@]}"; do
    build_image "$service" > "${SCRATCH}/${service}.log" 2>&1 &
    pids+=($!)
    if [ "${#pids[@]}" -ge "$BUILD_PARALLELISM" ]; then
      for pid in "${pids[@]}"; do wait "$pid" || failed=1; done
      pids=()
    fi
  done
  for pid in ${pids[@]+"${pids[@]}"}; do wait "$pid" || failed=1; done
  if [ "$failed" -ne 0 ]; then
    for service in "${SERVICES[@]}"; do
      [ -s "${SCRATCH}/${service}.tag" ] || { echo "image build failed: ${service}"; tail -n 30 "${SCRATCH}/${service}.log"; }
    done
    exit 1
  fi
}

load_images() {
  local service tag image
  for service in "${SERVICES[@]}"; do
    tag="$(cat "${SCRATCH}/${service}.tag")"
    image="${IMAGE_REPOSITORY}/${service}:${tag}"
    docker exec "${CLUSTER}-control-plane" crictl inspecti "docker.io/${image}" > /dev/null 2>&1 \
      || kind load docker-image --name "$CLUSTER" "$image"
  done
}

log "kind cluster ${CLUSTER}"
if kind get clusters 2> /dev/null | grep -qx "$CLUSTER"; then
  echo "cluster already exists"
else
  kind create cluster --name "$CLUSTER" --image kindest/node:v1.36.4@sha256:099e049362a1526b2db71494e1947aae99bd16290d7c895f2b7ea312e3cbfaed --wait 180s
fi

log "namespace ${NS}"
kubectl --context "$CONTEXT" create namespace "$NS" --dry-run=client -o yaml | kubectl --context "$CONTEXT" apply -f -

# Official pinned chart releases; no dependency build is needed for the application chart.
log "Prometheus Operator 0.94.2 (kube-prometheus-stack 92.16.1)"
helm --kube-context "$CONTEXT" upgrade --install monitoring-operator \
  https://github.com/prometheus-community/helm-charts/releases/download/kube-prometheus-stack-92.16.1/kube-prometheus-stack-92.16.1.tgz \
  --namespace "$NS" -f deploy/helm/monitoring-operator-values.yaml --wait --timeout 300s
log "KEDA 2.21.0"
helm --kube-context "$CONTEXT" upgrade --install keda https://kedacore.github.io/charts/keda-2.21.0.tgz \
  --namespace "$NS" --wait --timeout 300s
kubectl --context "$CONTEXT" wait --for=condition=Established --timeout=120s \
  crd/servicemonitors.monitoring.coreos.com crd/podmonitors.monitoring.coreos.com \
  crd/prometheuses.monitoring.coreos.com crd/scaledobjects.keda.sh

log "Strimzi operator ${STRIMZI_VERSION}"
helm --kube-context "$CONTEXT" upgrade --install strimzi "$STRIMZI_CHART" --version "$STRIMZI_VERSION" \
  --namespace "$NS" --wait --timeout 300s

log "building service images"
build_images
load_images

log "installing the ticketrush chart"
tag_args=()
for service in "${SERVICES[@]}"; do
  tag_args+=(--set-string "services.${service}.tag=$(cat "${SCRATCH}/${service}.tag")")
done
DELAY="${M5_CONSUMER_PROCESSING_DELAY_MS:-0}"
case "$DELAY" in ''|*[!0-9]*) echo 'M5_CONSUMER_PROCESSING_DELAY_MS must be a nonnegative integer'; exit 2 ;; esac
helm --kube-context "$CONTEXT" upgrade --install ticketrush "$CHART" --namespace "$NS" "${tag_args[@]}" \
  --set-string "services.inventory-service.env.CONSUMER_PROCESSING_DELAY_MS=$DELAY" \
  --set-string "services.payment-service.env.CONSUMER_PROCESSING_DELAY_MS=$DELAY" \
  --wait --timeout "$INFRA_TIMEOUT"

log "waiting for Kafka, topics and pods"
kc wait kafka/ticketrush --for=condition=Ready --timeout="$INFRA_TIMEOUT"
kc wait kafkatopic --all --for=condition=Ready --timeout=300s
kc wait --for=create statefulset/prometheus-ticketrush --timeout=180s
kc rollout status statefulset/prometheus-ticketrush --timeout=300s
kc wait scaledobject --all --for=condition=Ready --timeout=180s
kc wait pod --all --for=condition=Ready --timeout=600s

log "ticketrush is up (context ${CONTEXT}, namespace ${NS})"
kc get pods
