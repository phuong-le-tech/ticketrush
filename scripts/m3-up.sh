#!/usr/bin/env bash
# Skeleton (test-writer): the code-writer implements this. Contract: idempotent; create the kind cluster
# "ticketrush", install Strimzi 1.3.0 with Helm, build and kind-load the 6 images, helm upgrade --install the chart
# deploy/helm/ticketrush into namespace "ticketrush", wait until every pod is Ready. Exit 0 on success, on a rerun too.
echo "scripts/m3-up.sh is not implemented yet" >&2
exit 1
