#!/usr/bin/env bash
# TEST ONLY: M5 AC3_templatesAndMutations and AC6_hygiene. No cluster or writes to the chart.
# Needs helm, yq (mikefarah v4), jq and Python 3 (stdlib only).
# Usage: scripts/m5-chart-check.sh [--manifest FILE | --selftest [BASE_MANIFEST]]
# --selftest alone uses an explicitly synthetic test fixture, never acceptance evidence.
set -u
set -o pipefail
cd "$(dirname "$0")/.." || exit 2
for tool in helm yq jq python3 git; do
  command -v "$tool" >/dev/null 2>&1 || { echo "missing tool: $tool"; exit 2; }
done
MODE=chart
MANIFEST=""
case "${1:-}" in
  --manifest) [ "$#" = 2 ] || { echo '--manifest needs exactly one file'; exit 2; }; MODE=manifest; MANIFEST="$2" ;;
  --selftest) [ "$#" -le 2 ] || exit 2; MODE=selftest; MANIFEST="${2:-}" ;;
  '') [ "$#" = 0 ] || exit 2 ;;
  *) echo "usage: $0 [--manifest FILE | --selftest [BASE_MANIFEST]]"; exit 2 ;;
esac
SCRATCH=$(mktemp -d "${TMPDIR:-/tmp}/m5-chart-check.XXXXXX") || exit 2
trap 'rm -rf "$SCRATCH"' EXIT
if [ "$MODE" = chart ]; then
  helm lint deploy/helm/ticketrush --strict || { echo 'FAIL helm_lint: chart lint failed'; exit 1; }
  helm template ticketrush deploy/helm/ticketrush > "$SCRATCH/rendered.yaml" || { echo 'FAIL helm_template: render failed'; exit 1; }
  MANIFEST="$SCRATCH/rendered.yaml"
fi
if [ -n "$MANIFEST" ]; then
  [ -s "$MANIFEST" ] || { echo 'FAIL manifest: missing or empty file'; exit 1; }
  yq -o=json -I=0 '.' "$MANIFEST" | jq -s 'map(if type == "array" then .[] else . end) | map(select(. != null))' > "$SCRATCH/docs.json" || {
    echo 'FAIL manifest: cannot parse manifest'; exit 1;
  }
else
  printf '[]\n' > "$SCRATCH/docs.json"
fi
python3 - "$MODE" "$SCRATCH/docs.json" "$MANIFEST" <<'PY'
"""Static assertions and mutation tests, not deployment implementation."""
import copy
import json
import pathlib
import re
import subprocess
import sys

MODE, DOCS, BASE = sys.argv[1:]
# Immutable last pre-M5 implementation commit. Local main can lag behind M3/M4.
# Compare env additions against this milestone boundary, not a moving local branch.
M5_BASELINE = '6a5a3b010fe323041705067efb9dd467c4122c33'
APPS = ('api-gateway', 'inventory-service', 'payment-service', 'fake-payment-provider',
        'order-service', 'notification-service')
EXPECTED = {'inventory-service': 'orders.requested', 'payment-service': 'tickets.reserved'}


def integer(value):
    return type(value) is int


def metadata(doc):
    return doc.get('metadata') or {}


def namespace(doc):
    return metadata(doc).get('namespace', 'default')


def pod(doc):
    spec = doc.get('spec') or {}
    if doc.get('kind') == 'Pod':
        return spec
    if doc.get('kind') == 'CronJob':
        return spec.get('jobTemplate', {}).get('spec', {}).get('template', {}).get('spec', {})
    return spec.get('template', {}).get('spec', {})


def containers(doc):
    p = pod(doc)
    return p.get('containers', []) + p.get('initContainers', [])


def matches(labels, selector):
    if not selector or not (selector.get('matchLabels') or selector.get('matchExpressions')):
        return False
    if any(labels.get(k) != v for k, v in selector.get('matchLabels', {}).items()):
        return False
    for exp in selector.get('matchExpressions', []):
        key, op, values = exp.get('key'), exp.get('operator'), exp.get('values', [])
        if op == 'In' and (key not in labels or labels[key] not in values):
            return False
        if op == 'NotIn' and key in labels and labels[key] in values:
            return False
        if op == 'Exists' and key not in labels:
            return False
        if op == 'DoesNotExist' and key in labels:
            return False
        if op not in ('In', 'NotIn', 'Exists', 'DoesNotExist'):
            return False
    return True


def checks(docs):
    errors = []
    def fail(check, reason):
        errors.append('FAIL ' + check + ': ' + reason)
    def resources(kind):
        return [d for d in docs if d.get('kind') == kind]

    scaled = resources('ScaledObject')
    if len(scaled) != 2:
        fail('check_AC3_scaledObjects', 'expected exactly two ScaledObjects for inventory/payment')
    for app, topic in EXPECTED.items():
        objects = [d for d in scaled if d.get('spec', {}).get('scaleTargetRef', {}).get('name') == app]
        if len(objects) != 1:
            fail('check_AC3_scaledObjects', app + ': missing or duplicate scale target')
            continue
        obj = objects[0]
        spec = obj['spec']
        ref = spec['scaleTargetRef']
        deps = [d for d in resources('Deployment') if metadata(d).get('name') == app and namespace(d) == namespace(obj)]
        if len(deps) != 1 or ref.get('kind', 'Deployment') != 'Deployment' or ref.get('apiVersion', 'apps/v1') != 'apps/v1':
            fail('check_AC3_scaledObjects', app + ': target must resolve to an actual Deployment')
        if type(spec.get('minReplicaCount')) is not int or spec.get('minReplicaCount') != 1:
            fail('check_AC3_scaledObjects', app + ': minReplicaCount must be integer 1')
        maximum = spec.get('maxReplicaCount')
        if not integer(maximum) or not 1 <= maximum <= 12:
            fail('check_AC3_scaledObjects', app + ': maxReplicaCount must be integer 1..12')
        cooldown = spec.get('cooldownPeriod')
        if not integer(cooldown) or cooldown <= 0:
            fail('check_AC3_scaledObjects', app + ': cooldownPeriod must be a positive integer')
        triggers = [t for t in spec.get('triggers', []) if t.get('type') == 'kafka']
        if len(triggers) != 1:
            fail('check_AC3_scaledObjects', app + ': expected one Kafka trigger')
            continue
        trigger = triggers[0].get('metadata', {})
        if trigger.get('consumerGroup') != app or trigger.get('topic') != topic:
            fail('check_AC3_scaledObjects', app + ': wrong exact consumerGroup/topic')
        bootstraps = [e.get('value') for d in deps for c in containers(d) for e in c.get('env', []) if e.get('name') == 'KAFKA_BOOTSTRAP_SERVERS']
        kafka = resources('Kafka')
        cluster_bootstrap = {metadata(k).get('name', '') + '-kafka-bootstrap:9092' for k in kafka}
        bootstrap = trigger.get('bootstrapServers')
        if not bootstraps or any(b != bootstrap for b in bootstraps) or bootstrap not in cluster_bootstrap:
            fail('check_AC3_scaledObjects', app + ': bootstrap must match app env and rendered Kafka cluster')
        threshold = trigger.get('lagThreshold')
        if not isinstance(threshold, str) or not threshold.isdigit() or int(threshold) <= 0:
            fail('check_AC3_scaledObjects', app + ': Kafka lagThreshold must be a positive integer string')

    monitors = resources('ServiceMonitor')
    if len(monitors) != 6:
        fail('check_AC3_serviceMonitors', 'expected six app ServiceMonitors')
    selected_monitor_names = []
    for app in APPS:
        services = [s for s in resources('Service') if metadata(s).get('name') == app]
        if len(services) != 1:
            fail('check_AC3_serviceMonitors', app + ': actual Service missing or duplicated')
            continue
        service = services[0]
        selected = []
        for monitor in monitors:
            spec = monitor.get('spec', {})
            ns = spec.get('namespaceSelector') or {}
            ns_ok = ns.get('any') is True or namespace(service) in ns.get('matchNames', []) or (not ns and namespace(service) == namespace(monitor))
            if ns_ok and matches(metadata(service).get('labels', {}), spec.get('selector', {})):
                selected.append(monitor)
        if len(selected) != 1:
            fail('check_AC3_serviceMonitors', app + ': exactly one monitor must select actual Service labels/namespace')
            continue
        selected_monitor_names.append((namespace(selected[0]), metadata(selected[0]).get('name')))
        ports = {p.get('name') for p in service.get('spec', {}).get('ports', []) if p.get('name')}
        endpoints = selected[0].get('spec', {}).get('endpoints', [])
        if not any(e.get('path') == '/actuator/prometheus' and isinstance(e.get('port'), str) and e['port'] in ports for e in endpoints):
            fail('check_AC3_serviceMonitors', app + ': endpoint must use a named Service port and /actuator/prometheus')
        # Service selectors must resolve to the app pods, not merely have matching monitor labels.
        deps = [d for d in resources('Deployment') if metadata(d).get('name') == app and namespace(d) == namespace(service)]
        selector = service.get('spec', {}).get('selector', {})
        if len(deps) != 1 or not selector or not matches(deps[0].get('spec', {}).get('template', {}).get('metadata', {}).get('labels', {}), {'matchLabels': selector}):
            fail('check_AC3_serviceMonitors', app + ': Service selector does not resolve to app pods')

    if len(selected_monitor_names) == 6 and len(set(selected_monitor_names)) != 6:
        fail('check_AC3_serviceMonitors', 'each of the six monitors must select a distinct app Service')

    grafana = [(d, c) for d in docs for c in containers(d) if re.search(r'(^|/)grafana(?:/grafana)?(?::|@|$)', c.get('image', '')) or any(e.get('name', '').startswith('GF_') for e in c.get('env', []))]
    if not grafana:
        fail('check_AC6_secrets', 'Grafana workload missing')
    for workload, container in grafana:
        passwords = [e for e in container.get('env', []) if e.get('name') == 'GF_SECURITY_ADMIN_PASSWORD']
        if len(passwords) != 1:
            fail('check_AC6_secrets', 'Grafana admin password must use an explicit env secretKeyRef')
        for env in passwords:
            ref = env.get('valueFrom', {}).get('secretKeyRef', {})
            secrets = [s for s in resources('Secret') if metadata(s).get('name') == ref.get('name') and namespace(s) == namespace(workload)]
            if 'value' in env or ref.get('optional') is True or len(secrets) != 1 or ref.get('key') not in (secrets[0].get('data', {}) | secrets[0].get('stringData', {})):
                fail('check_AC6_secrets', 'Grafana password is literal or Secret/key cannot resolve')
    for workload in docs:
        for container in containers(workload):
            for env in container.get('env', []):
                if re.search(r'PASSWORD|SECRET|TOKEN|API_KEY', env.get('name', ''), re.I) and 'value' in env:
                    fail('check_AC6_secrets', 'credential env must not contain a literal: ' + env['name'])

    dashboards = []
    for cm in resources('ConfigMap'):
        for key, value in cm.get('data', {}).items():
            if not key.endswith('.json'):
                continue
            try:
                dashboard = json.loads(value)
            except (ValueError, TypeError):
                fail('check_AC3_dashboard', 'invalid dashboard JSON: ' + key)
                continue
            if not isinstance(dashboard, dict) or 'panels' not in dashboard:
                continue
            dashboards.append(dashboard)
            direct = any(v.get('configMap', {}).get('name') == metadata(cm).get('name') and any(m.get('name') == v.get('name') and '/dashboards' in m.get('mountPath', '') for m in c.get('volumeMounts', [])) for d, c in grafana if namespace(d) == namespace(cm) for v in pod(d).get('volumes', []))
            sidecar = any(e.get('name') == 'LABEL' and e.get('value') in metadata(cm).get('labels', {}) for d, _ in grafana for c in containers(d) for e in c.get('env', []))
            provider = any(
                'providers:' in str(value) and 'path:' in str(value) and '/dashboards' in str(value)
                and any(v.get('configMap', {}).get('name') == metadata(config).get('name')
                        and any(m.get('name') == v.get('name') and '/provisioning/dashboards' in m.get('mountPath', '') for m in c.get('volumeMounts', []))
                        for d, c in grafana if namespace(d) == namespace(config) for v in pod(d).get('volumes', []))
                for config in resources('ConfigMap') for value in config.get('data', {}).values())
            if not (sidecar or (direct and provider)):
                fail('check_AC3_dashboard', 'dashboard JSON is not provisioned by Grafana mount/provider or labeled sidecar')
    if not dashboards:
        fail('check_AC3_dashboard', 'provisioned dashboard JSON missing')
    def queries(node):
        if isinstance(node, dict):
            for target in node.get('targets', []):
                if not target.get('hide') and isinstance(target.get('expr'), str) and target['expr'].strip():
                    yield target['expr'].strip()
            for key, value in node.items():
                if key != 'targets':
                    yield from queries(value)
        elif isinstance(node, list):
            for value in node:
                yield from queries(value)
    expressions = set(q for d in dashboards for q in queries(d.get('panels', [])))
    categories = {
        'consumer lag': r'\b[a-zA-Z_:][a-zA-Z0-9_:]*lag[a-zA-Z0-9_:]*\b',
        'processing time': r'\bticketrush_consumer_processing_seconds_(?:sum|count|bucket)\b',
        'orders/sec': r'\b(?:rate|irate)\s*\([^)]*\bticketrush_orders_total\b',
        'saga outcomes': r'\bticketrush_saga_outcomes_total\b',
    }
    if len(expressions) < 4:
        fail('check_AC3_dashboard', 'expected at least four distinct nonempty visible PromQL queries')
    for name, pattern in categories.items():
        if not any(re.search(pattern, q) for q in expressions):
            fail('check_AC3_dashboard', 'missing meaningful query for ' + name)
    return errors


def git(*args):
    return subprocess.run(['git', *args], text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True).stdout


def env_refs(text):
    patterns = (r'\$\{([A-Z][A-Z0-9_]*)(?=[:}])', r'^\s*([A-Z][A-Z0-9_]*)\s*:',
                r'\bname:\s*[\"\']?([A-Z][A-Z0-9_]*)', r'System\.getenv\(\s*"([A-Z][A-Z0-9_]*)"')
    return set().union(*(set(re.findall(p, text, re.M)) for p in patterns))


def missing_envs(current, baseline, documented):
    return sorted(current - baseline - documented)


def hygiene(docs):
    errors = []
    def fail(reason):
        errors.append('FAIL check_AC6_hygiene: ' + reason)
    try:
        subprocess.run(['git', 'diff', '--check', M5_BASELINE], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        diff = git('diff', '--no-ext-diff', '--unified=0', M5_BASELINE, '--', '.')
        added = '\n'.join(line[1:] for line in diff.splitlines() if line.startswith('+') and not line.startswith('+++'))
        if re.search('[\u2013\u2014]', added):
            fail('added lines contain a forbidden long dash')
        if re.search(r'-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----|\bAKIA[A-Z0-9]{16}\b|\bgh[pousr]_[A-Za-z0-9]{30,}', added):
            fail('added lines contain a recognizable secret token/private key')
        baseline, current = set(), set()
        names = set(git('ls-files', '--cached', '--others', '--exclude-standard').splitlines())
        # git diff omits new files until staging; scan those too without staging anything.
        for name in git('ls-files', '--others', '--exclude-standard').splitlines():
            path = pathlib.Path(name)
            if not path.is_file():
                continue
            try:
                text = path.read_text()
            except (UnicodeError, OSError):
                continue
            if any(line.rstrip(' \t') != line for line in text.splitlines()):
                fail('trailing whitespace in new file: ' + name)
            if re.search('[\u2013\u2014]', text):
                fail('forbidden long dash in new file: ' + name)
            if re.search(r'-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----|\bAKIA[A-Z0-9]{16}\b|\bgh[pousr]_[A-Za-z0-9]{30,}', text):
                fail('recognizable secret in new file: ' + name)
        for name in names:
            if not ('/src/main/' in '/' + name or name.startswith('deploy/') or (name.startswith('scripts/') and name.endswith('-up.sh'))):
                continue
            path = pathlib.Path(name)
            if not path.is_file() or path.suffix not in ('.yml', '.yaml', '.java', '.properties', '.sh', '.tpl'):
                continue
            current.update(env_refs(path.read_text(errors='replace')))
            old = subprocess.run(['git', 'show', M5_BASELINE + ':' + name], text=True, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
            if old.returncode == 0:
                baseline.update(env_refs(old.stdout))
        current.update(e['name'] for d in docs for c in containers(d) for e in c.get('env', []) if 'name' in e)
        documented = set(re.findall(r'^([A-Z][A-Z0-9_]*)=', pathlib.Path('.env.example').read_text(), re.M))
        for name in missing_envs(current, baseline, documented):
            fail('new env missing from .env.example: ' + name)
    except (OSError, subprocess.CalledProcessError) as exc:
        fail('cannot complete repository hygiene scan: ' + str(exc))
    return errors


def fixture():
    # Explicitly synthetic VALID TEST FIXTURE, not helm output or milestone evidence.
    docs = [{'kind': 'Kafka', 'metadata': {'name': 'ticketrush'}}]
    for app in APPS:
        docs += [
            {'kind': 'Deployment', 'metadata': {'name': app}, 'spec': {'template': {'metadata': {'labels': {'app': app}}, 'spec': {'containers': [{'name': app, 'env': [{'name': 'KAFKA_BOOTSTRAP_SERVERS', 'value': 'ticketrush-kafka-bootstrap:9092'}]}]}}}},
            {'kind': 'Service', 'metadata': {'name': app, 'labels': {'app': app}}, 'spec': {'selector': {'app': app}, 'ports': [{'name': 'http', 'port': 8080}]}},
            {'kind': 'ServiceMonitor', 'metadata': {'name': app}, 'spec': {'selector': {'matchLabels': {'app': app}}, 'endpoints': [{'port': 'http', 'path': '/actuator/prometheus'}]}}]
    for app, topic in EXPECTED.items():
        docs.append({'kind': 'ScaledObject', 'metadata': {'name': app}, 'spec': {'scaleTargetRef': {'name': app}, 'minReplicaCount': 1, 'maxReplicaCount': 12, 'cooldownPeriod': 30, 'triggers': [{'type': 'kafka', 'metadata': {'consumerGroup': app, 'topic': topic, 'bootstrapServers': 'ticketrush-kafka-bootstrap:9092', 'lagThreshold': '10'}}]}})
    dashboard = {'panels': [{'targets': [{'expr': q}]} for q in (
        'sum(kafka_consumergroup_lag)', 'rate(ticketrush_consumer_processing_seconds_sum[5m]) / rate(ticketrush_consumer_processing_seconds_count[5m])',
        'sum(rate(ticketrush_orders_total[5m]))', 'sum by (outcome) (ticketrush_saga_outcomes_total)')]}
    docs += [
        {'kind': 'Secret', 'metadata': {'name': 'test-grafana-secret'}, 'stringData': {'password': 'synthetic-fixture-only'}},
        {'kind': 'ConfigMap', 'metadata': {'name': 'test-dashboard'}, 'data': {'dashboard.json': json.dumps(dashboard)}},
        {'kind': 'ConfigMap', 'metadata': {'name': 'test-provider'}, 'data': {'provider.yaml': 'providers:\n- options:\n    path: /var/lib/grafana/dashboards'}},
        {'kind': 'Deployment', 'metadata': {'name': 'arbitrary-observability-name'}, 'spec': {'template': {'spec': {'volumes': [{'name': 'dashboard', 'configMap': {'name': 'test-dashboard'}}, {'name': 'provider', 'configMap': {'name': 'test-provider'}}], 'containers': [{'name': 'ui', 'image': 'grafana/grafana:fixture', 'env': [{'name': 'GF_SECURITY_ADMIN_PASSWORD', 'valueFrom': {'secretKeyRef': {'name': 'test-grafana-secret', 'key': 'password'}}}], 'volumeMounts': [{'name': 'dashboard', 'mountPath': '/var/lib/grafana/dashboards'}, {'name': 'provider', 'mountPath': '/etc/grafana/provisioning/dashboards'}]}]}}}}]
    return docs


def selftest(docs):
    errors = checks(docs)
    if errors:
        print('\n'.join(errors))
        print('FAIL selftest: unmutated base does not pass structural checks')
        return 1
    print('ok   valid base fixture/manifest passes structural checks')
    mutations = []
    def add(name, check, mutate):
        mutations.append((name, check, mutate))
    def each(kind, mutate):
        def apply(ds):
            for d in ds:
                if d.get('kind') == kind:
                    mutate(d)
        return apply
    for kind, check in [('ScaledObject', 'scaledObjects'), ('ServiceMonitor', 'serviceMonitors'), ('ConfigMap', 'dashboard'), ('Deployment', 'secrets')]:
        add('missing_' + kind, check, lambda ds, k=kind: ds.__setitem__(slice(None), [d for d in ds if d.get('kind') != k]))
    for app in EXPECTED:
        add('missing_' + app + '_scaledObject', 'scaledObjects', lambda ds, a=app: ds.__setitem__(slice(None), [d for d in ds if not (d.get('kind') == 'ScaledObject' and d['spec']['scaleTargetRef']['name'] == a)]))
    for maximum in (0, 13, None, '12', True):
        add('max_' + repr(maximum), 'scaledObjects', each('ScaledObject', lambda d, m=maximum: d['spec'].__setitem__('maxReplicaCount', m)))
    for field, value in [('consumerGroup', 'wrong-group'), ('topic', 'wrong.topic'), ('bootstrapServers', 'wrong:9092')]:
        add('wrong_' + field, 'scaledObjects', each('ScaledObject', lambda d, f=field, v=value: d['spec']['triggers'][0]['metadata'].__setitem__(f, v)))
    for field, value in [('minReplicaCount', 0), ('cooldownPeriod', 0), ('scaleTargetRef', {'name': 'nonexistent'})]:
        add('bad_' + field, 'scaledObjects', each('ScaledObject', lambda d, f=field, v=value: d['spec'].__setitem__(f, v)))
    add('bad_monitor_selector', 'serviceMonitors', each('ServiceMonitor', lambda d: d['spec'].__setitem__('selector', {'matchLabels': {'missing': 'no-service'}})))
    add('bad_service_selector', 'serviceMonitors', each('Service', lambda d: d['spec'].__setitem__('selector', {'missing': 'no-pod'})))
    add('bad_monitor_namespace', 'serviceMonitors', each('ServiceMonitor', lambda d: d['spec'].__setitem__('namespaceSelector', {'matchNames': ['wrong']})))
    add('missing_named_service_port', 'serviceMonitors', each('Service', lambda d: [p.pop('name', None) for p in d['spec'].get('ports', [])]))
    for field, value in [('port', 'nonexistent'), ('path', '/actuator/health')]:
        add('wrong_endpoint_' + field, 'serviceMonitors', each('ServiceMonitor', lambda d, f=field, v=value: [e.__setitem__(f, v) for e in d['spec'].get('endpoints', [])]))
    def literal(ds):
        for d in ds:
            for c in containers(d):
                for e in c.get('env', []):
                    if e.get('name') == 'GF_SECURITY_ADMIN_PASSWORD':
                        e.clear()
                        e.update(name='GF_SECURITY_ADMIN_PASSWORD', value='synthetic-literal-password')
    add('literal_grafana_password', 'secrets', literal)
    add('missing_secret', 'secrets', lambda ds: ds.__setitem__(slice(None), [d for d in ds if d.get('kind') != 'Secret']))
    def change_dashboard(ds, replacement=None):
        for d in ds:
            if d.get('kind') != 'ConfigMap':
                continue
            for key, value in d.get('data', {}).items():
                if not key.endswith('.json'):
                    continue
                obj = json.loads(value)
                def change(node):
                    if isinstance(node, dict):
                        if node.get('targets'):
                            if replacement is None:
                                node['targets'] = []
                            else:
                                for t in node['targets']:
                                    t['expr'] = replacement
                            return True
                        return any(change(v) for v in node.values())
                    if isinstance(node, list):
                        return any(change(v) for v in node)
                    return False
                change(obj)
                d['data'][key] = json.dumps(obj)
    add('dashboard_missing_query', 'dashboard', change_dashboard)
    add('dashboard_constant_query', 'dashboard', lambda ds: change_dashboard(ds, 'vector(1)'))
    failed = False
    for name, check, mutate in mutations:
        mutated = copy.deepcopy(docs)
        mutate(mutated)
        result = checks(mutated)
        if mutated == docs or not any(e.startswith('FAIL check_AC3_' + check + ':') or e.startswith('FAIL check_AC6_' + check + ':') for e in result):
            print('FAIL selftest_' + name + ': mutation unchanged or not rejected by expected check')
            failed = True
        else:
            print('ok   selftest_' + name + ' (rejected by ' + check + ')')
    # Both ends of the allowed range must pass, including the exact partition cap.
    for maximum in (1, 12):
        valid = copy.deepcopy(docs)
        each('ScaledObject', lambda d: d['spec'].__setitem__('maxReplicaCount', maximum))(valid)
        if checks(valid):
            print('FAIL selftest: valid maxReplicaCount ' + str(maximum) + ' rejected')
            failed = True
        else:
            print('ok   valid maxReplicaCount ' + str(maximum))
    # Exercise the same env documentation decision without changing repository files.
    new_env = env_refs('value: ${M5_NEW_ENV:default}')
    if new_env != {'M5_NEW_ENV'} or missing_envs(new_env, set(), set()) != ['M5_NEW_ENV'] or missing_envs(new_env, set(), new_env):
        print('FAIL selftest: undocumented env accepted or documented env rejected')
        failed = True
    else:
        print('ok   selftest_undocumented_env rejected; documented env accepted')
    print('RESULT: m5-chart-check selftest ' + ('failed' if failed else 'passed') + ' (checker evidence only)')
    return int(failed)


try:
    docs = json.loads(pathlib.Path(DOCS).read_text())
    if not isinstance(docs, list) or any(not isinstance(d, dict) for d in docs):
        raise ValueError('manifest must contain resource objects')
    if MODE == 'selftest':
        if not BASE:
            print('TEST FIXTURE: synthetic valid M5 resources, not rendered chart acceptance')
            docs = fixture()
        sys.exit(selftest(docs))
    errors = checks(docs) + hygiene(docs)
    if errors:
        print('\n'.join(errors))
    print('RESULT: m5-chart-check ' + ('failed' if errors else 'passed'))
    sys.exit(1 if errors else 0)
except (ValueError, TypeError, KeyError, AttributeError) as exc:
    print('FAIL manifest: malformed resource: ' + str(exc))
    sys.exit(1)
PY
