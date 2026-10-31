#!/usr/bin/env bash
# Test only: AC4_cleanAndRepeat, AC5_scaleAndConserve, AC5_selftestRejectsFalsePass.
# Real mode DELETES kind cluster ticketrush, then calls m5-up twice. No production
# bootstrap/load implementation lives here. Missing m5-up is an assertion BEFORE
# tool discovery or cluster mutation. --evaluate FILE and --selftest are offline.
#
# Reversible code-writer contract (not an approved DESIGN change):
# - scripts/m5-up.sh uses M3's kind/namespace contract, enables monitoring + KEDA.
# - scripts/m5-load.js consumes k6 -e BASE_URL, EVENT_IDS (JSON array), RUN_ID;
#   uses quantity=1, unique customer/idempotency pairs, retries with the same key,
#   and exports accepted_orders Counter ONCE per distinct acknowledged order.
#   Its thresholds must fail on request errors. The test seeds 24 isolated events.
# - Grafana dashboard ConfigMap label grafana_dashboard=1, JSON contains queries
#   for lag, processing time, orders/sec and saga outcomes; queries must be concrete
#   PromQL (no unresolved Grafana variables). Grafana Secret has admin-user/password.
# - Default lag exporter contract: kafka_consumergroup_lag_sum{consumergroup,topic}.
#   Exporter choice is NOT frozen: --config JSON can override lag_query using
#   {group}/{topic} placeholders, service/Secret names, ports and test bounds.
#   Group/topic/threshold/cooldown are read from each real Kafka ScaledObject.
# - Config keys/defaults are in defaults() below. No new environment variables.
#
# JSON evidence v1 (--evaluate FILE): exact types, finite epoch-second timestamps.
# {version:1, timed_out:false, started_at:number, finished_at:number,
#  timeout_seconds:number, cooldown_seconds:{inventory-service:number,payment-service:number},
#  stack_before:[{name,ready:true}], stack_after:[{name,ready:true}],
#  node_before:string,node_after:string, credentials_before:string,credentials_after:string,
#  dashboard_queries:[{kind:'lag|processing|orders|saga',query:string,
#                      response:<raw Prometheus instant query JSON>}],
#  k6:{exit_code:0,accepted_orders:positive integer,started_at:number,finished_at:number},
#  services:{inventory-service:{group,topic,threshold},payment-service:{group,topic,threshold}},
#  samples:[{at:number,service,group,topic,lag:nonnegative number,ready_replicas:integer}],
#  invariants:{events:[{event_id,initial_stock,remaining_stock}],
#              orders:[{order_id,event_id,customer_id,idempotency_key,quantity,status}],
#              charges:[{order_id,count:integer}],provider_stable:true}}
# Fingerprints are SHA256 of Secret .data, never plaintext credentials. Samples must
# prove FOR BOTH services baseline 1, later lag > threshold at 1 during load, later
# >1 while lag remains high, then drained lag=0, then 1 after cooldown/load end.
# Every accepted order must settle, every CONFIRMED order must have one charge,
# every other order zero; exact per-event stock conservation is required.
set -euo pipefail
cd "$(dirname "$0")/.."
if [ "$#" -eq 0 ] && [ ! -f scripts/m5-up.sh ]; then
  printf '%s\n' 'FAIL AC4_cleanAndRepeat: required scripts/m5-up.sh is missing (M5 bootstrap behavior not implemented)'
  exit 1
fi
exec python3 - "$@" <<'PY'
import copy
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import urllib.parse
import urllib.request

SERVICES = {'inventory-service': 'orders.requested', 'payment-service': 'tickets.reserved'}
APPS = {'api-gateway', 'inventory-service', 'payment-service', 'order-service',
        'notification-service', 'fake-payment-provider'}

class Failure(Exception):
    pass

def check(condition, message):
    if not condition:
        raise Failure(message)

def number(value):
    return type(value) in (int, float) and math.isfinite(value)

def integer(value):
    return type(value) is int

def nonempty(value):
    return isinstance(value, str) and bool(value.strip())

def vector(response):
    check(isinstance(response, dict) and response.get('status') == 'success', 'Prometheus query failed')
    data = response['data']
    check(data['resultType'] == 'vector' and isinstance(data['result'], list) and data['result'],
          'query returned no instant-vector data')
    values = []
    for item in data['result']:
        pair = item['value']
        check(isinstance(pair, list) and len(pair) == 2 and number(pair[0]), 'malformed query sample')
        value = float(pair[1])
        check(math.isfinite(value), 'non-finite query value')
        values.append(value)
    return values

def evaluate(e):
    check(e['version'] == 1 and e['timed_out'] is False, 'timeout or unsupported evidence version')
    start, end, limit = e['started_at'], e['finished_at'], e['timeout_seconds']
    check(all(number(x) for x in (start, end, limit)) and 0 < end - start <= limit,
          'invalid run timestamps or acceptance timeout exceeded')
    for key in ('stack_before', 'stack_after'):
        stack = e[key]
        check(isinstance(stack, list) and stack, 'empty ready stack')
        check(all(nonempty(p['name']) and p['ready'] is True for p in stack), 'stack not Ready')
        check(all(any(p['name'].startswith(app + '-') for p in stack) for app in APPS),
              'ready stack missing application')
    for key in ('node', 'credentials'):
        check(nonempty(e[key + '_before']) and e[key + '_before'] == e[key + '_after'],
              key + ' changed on repeat or absent')
    queries = e['dashboard_queries']
    check(isinstance(queries, list) and {q['kind'] for q in queries} == {'lag', 'processing', 'orders', 'saga'},
          'four dashboard query kinds required')
    for q in queries:
        check(nonempty(q['query']), 'empty dashboard query')
        vector(q['response'])
    k6 = e['k6']
    check(integer(k6['exit_code']) and k6['exit_code'] == 0, 'k6 failed')
    check(integer(k6['accepted_orders']) and k6['accepted_orders'] > 0, 'no acknowledged load')
    check(number(k6['started_at']) and number(k6['finished_at']) and
          start <= k6['started_at'] < k6['finished_at'] <= end, 'invalid load timestamps')
    evaluate_scaling(e)
    evaluate_invariants(e)

def evaluate_scaling(e):
    start, end = e['started_at'], e['finished_at']
    k6 = e['k6']
    check(set(e['services']) == set(SERVICES), 'both service contracts required')
    samples = e['samples']
    check(isinstance(samples, list) and samples, 'empty samples')
    for s in samples:
        check(s['service'] in SERVICES, 'unknown service sample')
        c = e['services'][s['service']]
        check(nonempty(c['group']) and c['topic'] == SERVICES[s['service']], 'wrong consumer group/topic')
        check(number(c['threshold']) and c['threshold'] > 0, 'invalid threshold')
        check(s['group'] == c['group'] and s['topic'] == c['topic'], 'sample group/topic mismatch')
        check(number(s['at']) and start <= s['at'] <= end, 'malformed sample timestamp')
        check(number(s['lag']) and s['lag'] >= 0, 'absent or malformed lag')
        check(integer(s['ready_replicas']) and 1 <= s['ready_replicas'] <= 12, 'invalid ready replicas')
    for service in SERVICES:
        ss = [s for s in samples if s['service'] == service]
        check(len(ss) >= 5 and all(a['at'] < b['at'] for a, b in zip(ss, ss[1:])),
              service + ': missing or unordered timestamped samples')
        threshold = e['services'][service]['threshold']
        cooldown = e['cooldown_seconds'][service]
        check(number(cooldown) and cooldown > 0, 'invalid cooldown')
        check(ss[0]['ready_replicas'] == 1 and ss[0]['at'] <= k6['started_at'], service + ': no baseline 1')
        high = [s for s in ss if k6['started_at'] <= s['at'] <= k6['finished_at'] and
                s['lag'] > threshold and s['ready_replicas'] == 1 and s['at'] > ss[0]['at']]
        ups = [s for s in ss if s['ready_replicas'] > 1 and s['lag'] > threshold and
               any(h['at'] < s['at'] for h in high)]
        check(ups, service + ': no causal above-threshold 1 -> >1 scaling')
        downs = []
        for drain in ss:
            if drain['lag'] != 0 or not any(up['at'] < drain['at'] for up in ups):
                continue
            for down in ss:
                if (down['at'] >= drain['at'] + cooldown and down['at'] >= k6['finished_at'] and
                    down['ready_replicas'] == 1 and down['lag'] == 0 and
                    all(s['lag'] == 0 for s in ss if drain['at'] <= s['at'] <= down['at'])):
                    downs.append(down)
        check(downs, service + ': no drained scale-down after cooldown and load')

def evaluate_invariants(e):
    k6 = e['k6']
    inv = e['invariants']
    check(inv['provider_stable'] is True, 'provider restarted; charge evidence lost')
    events, orders, charges = inv['events'], inv['orders'], inv['charges']
    check(isinstance(events, list) and events and isinstance(orders, list) and orders and
          isinstance(charges, list), 'missing stock/order/charge evidence')
    check(len(orders) == k6['accepted_orders'], 'acknowledged orders missing or duplicated')
    ids = [o['order_id'] for o in orders]
    keys = [(o['event_id'], o['customer_id'], o['idempotency_key']) for o in orders]
    check(all(nonempty(i) for i in ids) and len(set(ids)) == len(ids) and len(set(keys)) == len(keys),
          'duplicate order or idempotency tuple')
    event_ids = [x['event_id'] for x in events]
    check(len(set(event_ids)) == len(event_ids), 'duplicate event stock evidence')
    check({o['event_id'] for o in orders} <= set(event_ids), 'order has unknown event')
    check(all(o['status'] in ('CONFIRMED', 'FAILED', 'REJECTED') and integer(o['quantity']) and
              1 <= o['quantity'] <= 4 and nonempty(o['customer_id']) and nonempty(o['idempotency_key'])
              for o in orders), 'unsettled or malformed orders')
    check(len(charges) == len(ids) and {c['order_id'] for c in charges} == set(ids), 'incomplete charge evidence')
    by_id = {c['order_id']: c['count'] for c in charges}
    for o in orders:
        count = by_id[o['order_id']]
        check(integer(count) and count == (1 if o['status'] == 'CONFIRMED' else 0),
              'duplicate/missing charge or charge for nonconfirmed order')
    sold_total = 0
    for event in events:
        initial, remaining = event['initial_stock'], event['remaining_stock']
        sold = sum(o['quantity'] for o in orders if o['event_id'] == event['event_id'] and o['status'] == 'CONFIRMED')
        check(integer(initial) and integer(remaining) and 0 <= remaining <= initial and
              sold <= initial and remaining + sold == initial, 'oversell or stock not conserved')
        sold_total += sold
    check(sold_total > 0, 'no successful sale under load')

def fixture():
    # Synthetic evaluator input ONLY. Never used by real mode or reported as cluster evidence.
    response = {'status': 'success', 'data': {'resultType': 'vector',
                 'result': [{'metric': {}, 'value': [10, '1']}]}}
    e = dict(version=1, timed_out=False, started_at=1, finished_at=100, timeout_seconds=120,
             stack_before=[{'name': app + '-abc', 'ready': True} for app in sorted(APPS)],
             node_before='node-id', node_after='node-id', credentials_before='digest', credentials_after='digest',
             dashboard_queries=[dict(kind=k, query=k, response=copy.deepcopy(response))
                                for k in ('lag', 'processing', 'orders', 'saga')],
             k6=dict(exit_code=0, accepted_orders=1, started_at=10, finished_at=40),
             services={}, cooldown_seconds={}, samples=[],
             invariants=dict(provider_stable=True,
                 events=[dict(event_id='show', initial_stock=10, remaining_stock=9)],
                 orders=[dict(order_id='order', event_id='show', customer_id='buyer',
                              idempotency_key='key', quantity=1, status='CONFIRMED')],
                 charges=[dict(order_id='order', count=1)]))
    e['stack_after'] = copy.deepcopy(e['stack_before'])
    for service, topic in SERVICES.items():
        e['services'][service] = dict(group=service, topic=topic, threshold=10)
        e['cooldown_seconds'][service] = 30
        for at, lag, replicas in [(5, 0, 1), (15, 11, 1), (20, 20, 2), (45, 0, 2), (80, 0, 1)]:
            e['samples'].append(dict(at=at, service=service, group=service, topic=topic,
                                     lag=lag, ready_replicas=replicas))
    return e

def selftest():
    valid = fixture()
    evaluate(valid)
    print('PASS AC5_selftestRejectsFalsePass valid fixture accepted (offline only)')
    mutations = {}
    def add(name, mutate):
        e = copy.deepcopy(valid)
        mutate(e)
        mutations[name] = e
    add('empty samples', lambda e: e.update(samples=[]))
    add('malformed samples', lambda e: e.update(samples=[{}]))
    add('absent lag', lambda e: e['samples'][1].pop('lag'))
    add('equal threshold', lambda e: [s.update(lag=10) for s in e['samples'] if s['lag'] > 0])
    add('no up', lambda e: [s.update(ready_replicas=1) for s in e['samples']])
    add('no down', lambda e: [s.update(ready_replicas=2) for s in e['samples'] if s['at'] == 80])
    add('timeout', lambda e: e.update(timed_out=True))
    add('elapsed timeout', lambda e: e.update(timeout_seconds=20))
    add('k6 failure', lambda e: e['k6'].update(exit_code=99))
    add('query failure', lambda e: e['dashboard_queries'][0]['response'].update(status='error'))
    add('query empty', lambda e: e['dashboard_queries'][0]['response']['data'].update(result=[]))
    add('query missing', lambda e: e['dashboard_queries'].pop())
    add('oversell', lambda e: e['invariants']['events'][0].update(remaining_stock=-1))
    add('stock leak', lambda e: e['invariants']['events'][0].update(remaining_stock=8))
    add('double charge', lambda e: e['invariants']['charges'][0].update(count=2))
    add('missing charge', lambda e: e['invariants'].update(charges=[]))
    add('duplicate order', lambda e: e['invariants']['orders'].append(copy.deepcopy(e['invariants']['orders'][0])))
    add('lost acknowledgment', lambda e: e['k6'].update(accepted_orders=2))
    add('empty stack', lambda e: e.update(stack_before=[]))
    add('not Ready', lambda e: e['stack_after'][0].update(ready=False))
    add('changed node', lambda e: e.update(node_after='new'))
    add('changed credentials', lambda e: e.update(credentials_after='new'))
    add('one service only', lambda e: e.update(samples=[s for s in e['samples'] if s['service'] == 'inventory-service']))
    add('up before high', lambda e: [s.update(at=12) for s in e['samples'] if s['at'] == 20])
    add('early down', lambda e: [s.update(at=50) for s in e['samples'] if s['at'] == 80])
    add('wrong topic', lambda e: e['samples'][1].update(topic='wrong'))
    add('NaN lag', lambda e: e['samples'][1].update(lag=float('nan')))
    add('replica string', lambda e: e['samples'][1].update(ready_replicas='1'))
    add('provider restart', lambda e: e['invariants'].update(provider_stable=False))
    for service in SERVICES:
        add(service + ' no high lag', lambda e, service=service:
            [s.update(lag=0) for s in e['samples'] if s['service'] == service])
        add(service + ' no up', lambda e, service=service:
            [s.update(ready_replicas=1) for s in e['samples'] if s['service'] == service])
        add(service + ' no down', lambda e, service=service:
            [s.update(ready_replicas=2) for s in e['samples'] if s['service'] == service and s['at'] == 80])
    for name, e in mutations.items():
        try:
            evaluate(e)
        except (Failure, KeyError, TypeError, ValueError):
            print('PASS rejects ' + name)
        else:
            raise Failure('selftest falsely accepted ' + name)
    # Exercise public --evaluate, including the JSON parser, using temporary inputs.
    with tempfile.TemporaryDirectory(prefix='m5-evaluator-', dir=os.environ.get('TMPDIR', '.')) as work:
        path = Path(work) / 'evidence.json'
        for text, expected in [(json.dumps(valid), 0), ('', 1), ('{', 1), ('null', 1), ('[]', 1)]:
            path.write_text(text)
            result = subprocess.run(['bash', 'scripts/m5-acceptance.sh', '--evaluate', str(path)],
                                    capture_output=True, text=True, timeout=10)
            check(result.returncode == expected, 'public --evaluate returned unexpected status: ' + result.stdout + result.stderr)
    print('PASS public --evaluate accepts valid JSON and rejects malformed evidence')
    print('RESULT: m5-acceptance selftest passed')

def defaults():
    return dict(cluster='ticketrush', namespace='ticketrush', prometheus_service='prometheus',
                prometheus_port=9090, grafana_service='grafana', grafana_port=80,
                grafana_secret='ticketrush-grafana', lag_query='kafka_consumergroup_lag_sum{consumergroup="{group}",topic="{topic}"}',
                timeout_seconds=1500, command_timeout=60, up_timeout=900,
                poll_seconds=5, load_timeout=300, stock_per_event=500,
                load_script='scripts/m5-load.js')

class Live:
    def __init__(self, cfg, work):
        self.c, self.work = cfg, Path(work)
        self.started = time.time()
        self.deadline = self.started + cfg['timeout_seconds']
        self.children = []
        self.env = dict(os.environ, M3_CLUSTER=cfg['cluster'], M3_NAMESPACE=cfg['namespace'])

    def run(self, args, timeout=None):
        left = self.deadline - time.time()
        check(left > 0, 'acceptance timeout exceeded')
        proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                                env=self.env, start_new_session=True)
        try:
            stdout, stderr = proc.communicate(timeout=min(timeout or self.c['command_timeout'], left))
        except subprocess.TimeoutExpired:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.communicate(timeout=3)
            raise Failure('command timeout: ' + ' '.join(args[:6]))
        check(proc.returncode == 0, 'command failed: ' + ' '.join(args[:6]) + ': ' + stderr[-1500:])
        return stdout

    def kc(self, *args):
        return self.run(['kubectl', '--context', 'kind-' + self.c['cluster'], '--request-timeout=30s',
                         '-n', self.c['namespace'], *args])

    def get(self, *args):
        return json.loads(self.kc('get', *args, '-o', 'json'))

    def ready(self):
        pods = self.get('pods')['items']
        result = [{'name': p['metadata']['name'], 'ready': any(c['type'] == 'Ready' and c['status'] == 'True'
                  for c in p.get('status', {}).get('conditions', []))} for p in pods
                  if p.get('status', {}).get('phase') != 'Succeeded']
        check(result and all(p['ready'] for p in result), 'empty or non-Ready stack')
        check(all(any(p['name'].startswith(a + '-') for p in result) for a in APPS), 'missing app pods')
        return result

    def credentials(self):
        data = self.get('secret', self.c['grafana_secret'])['data']
        check(data.get('admin-user') and data.get('admin-password'), 'Grafana Secret missing credentials')
        return hashlib.sha256(json.dumps(data, sort_keys=True).encode()).hexdigest()

    def http(self, url):
        with urllib.request.urlopen(url, timeout=min(15, max(1, self.deadline - time.time()))) as r:
            return json.load(r)

    def forward(self, service, remote):
        # Let kubectl allocate a free local port. No ambient kubeconfig mutation.
        import re
        log = open(self.work / (service + '.forward.log'), 'w+')
        proc = subprocess.Popen(['kubectl', '--context', 'kind-' + self.c['cluster'], '-n',
            self.c['namespace'], 'port-forward', 'svc/' + service, ':' + str(remote), '--address=127.0.0.1'],
            stdout=log, stderr=log, start_new_session=True)
        self.children.append(proc)
        until = min(self.deadline, time.time() + 30)
        while time.time() < until:
            log.seek(0)
            match = re.search(r'Forwarding from 127\.0\.0\.1:(\d+)', log.read())
            if match:
                return 'http://127.0.0.1:' + match.group(1)
            check(proc.poll() is None, 'port-forward failed: ' + service)
            time.sleep(0.2)
        raise Failure('port-forward timeout: ' + service)

    def query(self, query):
        response = self.http(self.prom + '/api/v1/query?' + urllib.parse.urlencode({'query': query}))
        vector(response)
        return response

    def lag_query(self, c):
        template = self.c['lag_query']
        if template == 'kafka_consumergroup_lag':
            return 'kafka_consumergroup_lag{consumergroup=' + json.dumps(c['group']) + ',topic=' + json.dumps(c['topic']) + '}'
        return template.replace('{group}', c['group']).replace('{topic}', c['topic'])

    def warm_up(self, gateway, evidence):
        # Kafka exporter has no group/topic series until the first real offset commit.
        event = 'm5-warmup-' + str(time.time_ns())
        self.kc('exec', 'redis-0', '--', 'redis-cli', 'HSET', 'inventory:event:' + event,
                'stock', '1', 'priceCents', '100', 'currency', 'EUR')
        request = urllib.request.Request(gateway + '/orders', method='POST',
            data=json.dumps(dict(eventId=event, customerId=event, quantity=1)).encode(),
            headers={'Content-Type': 'application/json', 'Idempotency-Key': event})
        with urllib.request.urlopen(request, timeout=15) as response:
            check(response.status == 202, 'warm-up order not acknowledged')
            order_id = json.load(response)['orderId']
        until = min(self.deadline, time.time() + 120)
        while time.time() < until:
            rows = self.orders([event])
            settled = len(rows) == 1 and rows[0]['order_id'] == order_id and rows[0]['status'] == 'CONFIRMED'
            try:
                drained = all(sum(vector(self.query(self.lag_query(c)))) == 0
                              for c in evidence['services'].values())
            except Failure:
                drained = False
            if settled and drained:
                print('PASS warm-up - real confirmed order and committed, drained lag for both groups', flush=True)
                return
            time.sleep(self.c['poll_seconds'])
        raise Failure('cold-start warm-up did not settle with real drained lag')

    def sample(self, evidence):
        for service, c in evidence['services'].items():
            response = self.query(self.lag_query(c))
            result = response['data']['result']
            check(all(x['metric'].get('consumergroup') == c['group'] and
                      x['metric'].get('topic') == c['topic'] for x in result), 'lag query lost group/topic labels')
            lag = sum(vector(response))
            dep = self.get('deployment', service)
            ready = dep.get('status', {}).get('readyReplicas', 0)
            evidence['samples'].append(dict(at=time.time(), service=service, group=c['group'],
                topic=c['topic'], lag=lag, ready_replicas=ready))
            with (self.work / 'samples.jsonl').open('a') as log:
                log.write(json.dumps(evidence['samples'][-1]) + '\n')
            print('SAMPLE ' + json.dumps(evidence['samples'][-1]), flush=True)

    def orders(self, events):
        literals = ','.join("'" + x + "'" for x in events)
        sql = "SELECT coalesce(json_agg(t),'[]'::json) FROM (SELECT order_id,event_id,customer_id,idempotency_key,quantity,status FROM orders.orders WHERE event_id IN (" + literals + ')) t'
        return json.loads(self.kc('exec', 'postgres-0', '--', 'sh', '-c',
             'exec psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d ticketrush -tA -c "$1"', 'sh', sql))

    def provider_stamp(self):
        pods = self.get('pods', '-l', 'app.kubernetes.io/name=fake-payment-provider')['items']
        # Fall back to deployment pod selector, never an empty success.
        if not pods:
            selector = self.get('deployment', 'fake-payment-provider')['spec']['selector']['matchLabels']
            pods = self.get('pods', '-l', ','.join(k + '=' + v for k, v in selector.items()))['items']
        check(len(pods) == 1, 'expected one stable fake provider for charge audit')
        p = pods[0]
        return (p['metadata']['uid'], [(x['name'], x['restartCount']) for x in p['status']['containerStatuses']])

    def execute(self):
        e = dict(version=1, timed_out=False, started_at=self.started,
                 timeout_seconds=self.c['timeout_seconds'], services={}, cooldown_seconds={}, samples=[])
        self.run(['kind', 'delete', 'cluster', '--name', self.c['cluster']])
        self.run(['bash', 'scripts/m5-up.sh'], self.c['up_timeout'])
        e['stack_before'] = self.ready()
        e['node_before'] = self.run(['docker', 'inspect', self.c['cluster'] + '-control-plane', '--format', '{{.Id}}']).strip()
        e['credentials_before'] = self.credentials()
        self.run(['bash', 'scripts/m5-up.sh'], self.c['up_timeout'])
        e['stack_after'] = self.ready()
        e['node_after'] = self.run(['docker', 'inspect', self.c['cluster'] + '-control-plane', '--format', '{{.Id}}']).strip()
        e['credentials_after'] = self.credentials()
        check(e['node_before'] == e['node_after'] and e['credentials_before'] == e['credentials_after'],
              'repeat changed kind node or Grafana credentials')
        self.prom = self.forward(self.c['prometheus_service'], self.c['prometheus_port'])
        grafana = self.forward(self.c['grafana_service'], self.c['grafana_port'])
        check(self.http(grafana + '/api/health')['database'] == 'ok', 'Grafana not healthy')
        gateway = self.forward('api-gateway', 8080)
        provider = self.forward('fake-payment-provider', 8083)
        for so in self.get('scaledobjects')['items']:
            service = so['spec']['scaleTargetRef']['name']
            if service not in SERVICES:
                continue
            triggers = [t['metadata'] for t in so['spec']['triggers'] if t['type'] == 'kafka' and
                        t['metadata'].get('topic') == SERVICES[service]]
            check(len(triggers) == 1 and service not in e['services'], 'ambiguous Kafka scaling contract')
            m = triggers[0]
            e['services'][service] = dict(group=m['consumerGroup'], topic=m['topic'], threshold=float(m['lagThreshold']))
            e['cooldown_seconds'][service] = so['spec'].get('cooldownPeriod', 300)
        check(set(e['services']) == set(SERVICES), 'both Kafka ScaledObjects required')
        self.warm_up(gateway, e)
        self.sample(e)
        run_id = 'm5-' + str(time.time_ns())
        events = [run_id + '-' + str(i) for i in range(24)]
        stock = self.c['stock_per_event']
        for event in events:
            self.kc('exec', 'redis-0', '--', 'redis-cli', 'HSET', 'inventory:event:' + event,
                    'stock', str(stock), 'priceCents', '100', 'currency', 'EUR')
        provider_before = self.provider_stamp()
        summary = self.work / 'k6-summary.json'
        load_log = open(self.work / 'k6.log', 'w')
        load_start = time.time()
        proc = subprocess.Popen(['k6', 'run', '--summary-export', str(summary), '-e', 'BASE_URL=' + gateway,
            '-e', 'EVENT_IDS=' + json.dumps(events), '-e', 'RUN_ID=' + run_id, self.c['load_script']],
            stdout=load_log, stderr=load_log, start_new_session=True)
        self.children.append(proc)
        load_end = None
        while time.time() < self.deadline:
            self.sample(e)
            if proc.poll() is not None and load_end is None:
                load_end = time.time()
                check(proc.returncode == 0, 'k6 exited nonzero; see ' + str(self.work / 'k6.log'))
            check(load_end is not None or time.time() - load_start < self.c['load_timeout'], 'k6 timeout')
            if load_end is not None:
                counts = json.loads(summary.read_text())['metrics']['accepted_orders']
                accepted = counts.get('count', counts.get('values', {}).get('count'))
                e['k6'] = dict(exit_code=proc.returncode, accepted_orders=accepted, started_at=load_start, finished_at=load_end)
                rows = self.orders(events)
                if len(rows) == accepted and all(o['status'] in ('CONFIRMED', 'REJECTED', 'FAILED') for o in rows):
                    # Only attempt the bounded audit once both scaling timelines are complete.
                    candidate = copy.deepcopy(e)
                    candidate['finished_at'] = time.time()
                    try:
                        evaluate_scaling(candidate)
                    except Failure:
                        pass
                    else:
                        break
            time.sleep(min(self.c['poll_seconds'], max(0, self.deadline - time.time())))
        else:
            raise Failure('timeout waiting for both services to scale and drain')
        # Provisioned queries are executed after traffic so all four families have data.
        queries = {}
        cms = self.get('configmaps', '-l', 'grafana_dashboard=1')['items']
        def targets(node):
            if isinstance(node, dict):
                if isinstance(node.get('expr'), str):
                    yield node['expr']
                for value in node.values():
                    yield from targets(value)
            elif isinstance(node, list):
                for value in node:
                    yield from targets(value)
        for cm in cms:
            for name, text in cm.get('data', {}).items():
                if not name.endswith('.json'):
                    continue
                dashboard = json.loads(text)
                # Confirm Grafana really provisioned it, not merely a ConfigMap on disk.
                uid = dashboard['uid']
                import base64
                data = self.get('secret', self.c['grafana_secret'])['data']
                auth = base64.b64encode(base64.b64decode(data['admin-user']) + b':' +
                                        base64.b64decode(data['admin-password'])).decode()
                request = urllib.request.Request(grafana + '/api/dashboards/uid/' + urllib.parse.quote(uid),
                                                  headers={'Authorization': 'Basic ' + auth})
                with urllib.request.urlopen(request, timeout=15) as response:
                    provisioned = json.load(response)
                check(provisioned['meta']['provisioned'] is True, 'dashboard not provisioned')
                for query in targets(provisioned['dashboard']):
                    for kind, token in [('lag', 'lag'), ('processing', 'ticketrush_consumer_processing_seconds'),
                                        ('orders', 'ticketrush_orders_total'), ('saga', 'ticketrush_saga_outcomes_total')]:
                        if token in query:
                            check('$' not in query, 'dashboard query needs concrete variable-free acceptance query')
                            queries[kind] = dict(kind=kind, query=query, response=self.query(query))
        e['dashboard_queries'] = list(queries.values())
        event_evidence = []
        for event in events:
            remaining = self.kc('exec', 'redis-0', '--', 'redis-cli', '--raw', 'HGET', 'inventory:event:' + event, 'stock')
            event_evidence.append(dict(event_id=event, initial_stock=stock, remaining_stock=int(remaining.strip())))
        charges = []
        for order in rows:
            result = self.http(provider + '/charges?' + urllib.parse.urlencode({'idempotencyKey': order['order_id']}))
            check(isinstance(result, list) and all(c['orderId'] == order['order_id'] for c in result), 'malformed provider audit')
            charges.append(dict(order_id=order['order_id'], count=len(result)))
        e['invariants'] = dict(events=event_evidence, orders=rows, charges=charges,
                               provider_stable=provider_before == self.provider_stamp())
        self.ready()
        e['finished_at'] = time.time()
        (self.work / 'evidence.json').write_text(json.dumps(e, indent=2))
        evaluate(e)
        print('PASS AC4_cleanAndRepeat - Ready stack, stable node/Secret, four provisioned queries return data')
        print('PASS AC5_scaleAndConserve - both services causal 1 -> >1 -> 1; stock/orders/charges conserved')
        print('RESULT: m5-acceptance passed; evidence: ' + str(self.work / 'evidence.json'))

    def close(self):
        for proc in self.children:
            if proc.poll() is None:
                os.killpg(proc.pid, signal.SIGTERM)
                try:
                    proc.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    os.killpg(proc.pid, signal.SIGKILL)
                    proc.wait(timeout=3)


def main():
    args = sys.argv[1:]
    if args == ['--selftest']:
        selftest()
        return
    if len(args) == 2 and args[0] == '--evaluate':
        evaluate(json.loads(Path(args[1]).read_text()))
        print('PASS AC4_cleanAndRepeat / AC5_scaleAndConserve evidence evaluation (offline only)')
        return
    cfg = defaults()
    if len(args) == 2 and args[0] == '--config':
        overrides = json.loads(Path(args[1]).read_text())
        check(isinstance(overrides, dict) and set(overrides) <= set(cfg), 'unknown config key')
        cfg.update(overrides)
    else:
        check(not args, 'usage: scripts/m5-acceptance.sh [--selftest | --evaluate FILE | --config FILE]')
    check(Path('scripts/m5-up.sh').is_file(), 'AC4_cleanAndRepeat: required scripts/m5-up.sh is missing (M5 bootstrap behavior not implemented)')
    check(Path(cfg['load_script']).is_file(), 'AC5_scaleAndConserve: required k6 load script missing: ' + cfg['load_script'])
    for key in ('timeout_seconds', 'command_timeout', 'up_timeout', 'poll_seconds', 'load_timeout', 'stock_per_event'):
        check(number(cfg[key]) and cfg[key] > 0, 'invalid bound: ' + key)
    for tool in ('docker', 'kind', 'kubectl', 'k6'):
        check(shutil.which(tool), 'missing prerequisite: ' + tool)
    work = tempfile.mkdtemp(prefix='m5-acceptance-', dir=os.environ.get('TMPDIR', '.'))
    print('Evidence/log directory: ' + work, flush=True)
    live = Live(cfg, work)
    try:
        live.execute()
    finally:
        live.close()

try:
    main()
except (Failure, KeyError, TypeError, ValueError, OSError, subprocess.SubprocessError) as exc:
    print('FAIL m5-acceptance: ' + str(exc), file=sys.stderr)
    sys.exit(1)
PY
