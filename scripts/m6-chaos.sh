#!/usr/bin/env bash
# Offline evaluation never discovers tools or contacts Kubernetes.
set -euo pipefail
cd "$(dirname "$0")/.."
exec python3 scripts/m6_chaos.py "$@"
