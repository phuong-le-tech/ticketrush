"""Live collectors for M6. All Kubernetes calls pin the project context and namespace."""
import argparse
import datetime
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import time
import urllib.parse
import urllib.request

from chaos_runner import CONTEXT, NAMESPACE, SERVICES, EXPERIMENTS, TERMINAL, EVIDENCE, Failure, check, evaluate


class Live:
    def __init__(self, work, timeout=600):
        self.work = work
        work.mkdir(parents=True, exist_ok=False)
        self.deadline = time.monotonic() + timeout
        self.timeout = timeout
        self.children = []
        self.log = (work / 'commands.jsonl').open('w')
        self.before: dict = {}
        self.restoring = False

    def run(self, args, timeout=60):
        left = self.deadline - time.monotonic()
        check(left > 0 or self.restoring, 'run timeout')
        result = subprocess.run(args, capture_output=True, text=True,
                                timeout=timeout if self.restoring else min(timeout, max(1, left)))
        self.log.write(json.dumps(dict(at=time.time(), command=args, exit_code=result.returncode,
                                       stdout=result.stdout, stderr=result.stderr)) + '\n')
        self.log.flush()
        check(result.returncode == 0, 'command failed: ' + ' '.join(args[:7]) + ': ' + result.stderr[-1200:])
        return result.stdout

    def kc(self, *args, timeout=60):
        return self.run(['kubectl', '--context', CONTEXT, '-n', NAMESPACE, '--request-timeout=30s', *args], timeout)

    def get(self, *args):
        return json.loads(self.kc('get', *args, '-o', 'json'))

    def sql(self, sql):
        return self.kc('exec', 'postgres-0', '--', 'sh', '-c',
                       'exec psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d ticketrush -tA -c "$1"', 'sh', sql).strip()

    def pods(self, service):
        selector = self.get('deployment', service)['spec']['selector']['matchLabels']
        pods = self.get('pods', '-l', ','.join(k + '=' + v for k, v in selector.items()))['items']
        return [p for p in pods if not p['metadata'].get('deletionTimestamp')]

    def stamp(self):
        pods = self.pods('fake-payment-provider')
        check(len(pods) == 1, 'provider audit requires one pod')
        return [pods[0]['metadata']['uid'], pods[0]['status']['containerStatuses']]

    def provider_stamp(self):
        uid, containers = self.stamp()
        return [uid, [(c['name'], c['restartCount']) for c in containers]]

    def snapshot(self):
        scalers = {}
        replicas = {}
        resources = {}
        consumer_env = {}
        for service in SERVICES:
            so = self.get('scaledobject', service)
            scalers[service] = dict(spec=so['spec'], annotations=so['metadata'].get('annotations', {}))
            dep = self.get('deployment', service)
            replicas[service] = dep['spec']['replicas']
            container = dep['spec']['template']['spec']['containers'][0]
            resources[service] = container['resources']
            consumer_env[service] = next(v['value'] for v in container['env'] if v['name'] == 'JAVA_TOOL_OPTIONS')
        provider = self.get('deployment', 'fake-payment-provider')['spec']['template']['spec']['containers'][0]['env']
        rate = [v for v in provider if v['name'] == 'FAKE_PROVIDER_FAILURE_RATE']
        check(len(rate) == 1 and 'value' in rate[0], 'provider failure rate must be explicit')
        return dict(replicas=replicas, resources=resources, consumer_env=consumer_env, scaledobjects=scalers,
                    provider_env={'FAKE_PROVIDER_FAILURE_RATE': rate[0]['value']},
                    db_settings=json.loads(self.sql("SELECT json_build_object('lock_timeout',current_setting('lock_timeout'),"
                        "'statement_timeout',current_setting('statement_timeout'),'chaos_locks',"
                        "(SELECT count(*) FROM pg_stat_activity WHERE application_name='m6-chaos'))")))

    def patch(self, kind, name, patch):
        self.kc('patch', kind, name, '--type=' + ('strategic' if kind == 'deployment' else 'merge'), '-p', json.dumps(patch))

    def rollout(self, service):
        self.kc('rollout', 'status', 'deployment/' + service, '--timeout=180s', timeout=190)
        deployment = self.get('deployment', service)
        selector = deployment['spec']['selector']['matchLabels']
        until = time.monotonic() + 90
        while time.monotonic() < until:
            pods = self.get('pods', '-l', ','.join(k + '=' + v for k, v in selector.items()))['items']
            # Rollout status can succeed while surplus JVMs still consume memory during termination.
            if len(pods) == deployment['spec']['replicas'] and all(
                    not p['metadata'].get('deletionTimestamp') for p in pods):
                return
            time.sleep(1)
        raise Failure('old pods did not terminate: ' + service)

    def pause(self, replicas):
        for service in SERVICES:
            # paused-replicas removes the HPA before manual scaling. Keep max >= requested replicas.
            self.patch('scaledobject', service, dict(metadata=dict(annotations={
                'autoscaling.keda.sh/paused-replicas': str(replicas)}),
                spec=dict(maxReplicaCount=max(2, replicas))))
            self.kc('scale', 'deployment/' + service, '--replicas=' + str(replicas))
        for service in SERVICES:
            self.rollout(service)

    def restore(self):
        if not self.before:
            return
        self.restoring = True
        errors = []
        # Do not let a second signal interrupt restoration.
        old_handlers = {sig: signal.signal(sig, signal.SIG_IGN) for sig in (signal.SIGINT, signal.SIGTERM)}
        for proc, log in self.children:
            try:
                if proc.poll() is None:
                    proc.terminate()
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                        proc.wait(timeout=5)
            except (OSError, subprocess.SubprocessError) as exc:
                errors.append('child shutdown: ' + str(exc))
            finally:
                log.close()
        actions: list = [lambda: self.sql("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name='m6-chaos'")]
        rate = self.before['provider_env']['FAKE_PROVIDER_FAILURE_RATE']
        actions.append(lambda: self.kc('set', 'env', 'deployment/fake-payment-provider', 'FAKE_PROVIDER_FAILURE_RATE=' + rate))
        for service in SERVICES:
            original = self.before['scaledobjects'][service]
            annotations = dict(original['annotations'])
            annotations['autoscaling.keda.sh/paused-replicas'] = annotations.get('autoscaling.keda.sh/paused-replicas')
            actions.extend([
                lambda service=service: self.patch('scaledobject', service, dict(metadata=dict(annotations={
                    'autoscaling.keda.sh/paused-replicas': str(self.before['replicas'][service])}))),
                lambda service=service: self.kc('scale', 'deployment/' + service,
                    '--replicas=' + str(self.before['replicas'][service])),
                lambda service=service: self.rollout(service),
                lambda service=service: self.patch('deployment', service, dict(spec=dict(template=dict(spec=dict(containers=[
                    dict(name=service, resources=self.before['resources'][service], env=[
                        dict(name='JAVA_TOOL_OPTIONS', value=self.before['consumer_env'][service])])]))))),
                lambda service=service: self.rollout(service),
                lambda service=service, original=original, annotations=annotations:
                    self.patch('scaledobject', service, dict(spec=original['spec'], metadata=dict(annotations=annotations))),
            ])
        for action in actions:
            try:
                action()
            except (Failure, OSError, subprocess.SubprocessError) as exc:
                errors.append(str(exc))
        for service in (*SERVICES, 'fake-payment-provider'):
            try:
                self.rollout(service)
            except (Failure, OSError, subprocess.SubprocessError) as exc:
                errors.append(str(exc))
        self.restoring = False
        for sig, handler in old_handlers.items():
            signal.signal(sig, handler)
        check(not errors, 'cleanup failed: ' + '; '.join(errors))

    def forward(self, service, port):
        log = (self.work / (service + '-forward.log')).open('w+')
        proc = subprocess.Popen(['kubectl', '--context', CONTEXT, '-n', NAMESPACE,
            'port-forward', 'svc/' + service, ':' + str(port), '--address=127.0.0.1'], stdout=log, stderr=log)
        self.children.append((proc, log))
        until = time.monotonic() + 20
        while time.monotonic() < until:
            log.seek(0)
            match = re.search(r'Forwarding from 127\.0\.0\.1:(\d+)', log.read())
            if match:
                return 'http://127.0.0.1:' + match[1]
            check(proc.poll() is None, 'port-forward failed: ' + service)
            time.sleep(0.2)
        raise Failure('port-forward timeout: ' + service)

    def http(self, url):
        with urllib.request.urlopen(url, timeout=15) as response:
            return json.load(response)

    def lag(self, service, topic):
        query = 'kafka_consumergroup_lag_sum{consumergroup="' + service + '",topic="' + topic + '"}'
        response = self.http(self.prom + '/api/v1/query?' + urllib.parse.urlencode({'query': query}))
        self.log.write(json.dumps(dict(at=time.time(), prometheus=response, query=query)) + '\n')
        values = response['data']['result']
        check(response['status'] == 'success' and values, 'missing lag data: ' + service)
        timestamps = self.http(self.prom + '/api/v1/query?' + urllib.parse.urlencode({'query': 'timestamp(' + query + ')'}))
        self.log.write(json.dumps(dict(at=time.time(), scrape_timestamps=timestamps, query=query)) + '\n')
        stamps = timestamps['data']['result']
        check(timestamps['status'] == 'success' and len(stamps) == len(values) and
              all(0 <= time.time() - float(v['value'][1]) < 30 for v in stamps), 'stale lag scrape')
        lag = sum(float(v['value'][1]) for v in values)
        check(lag >= 0, 'negative lag')
        return lag

    def stocks(self, events):
        result = self.kc('exec', 'redis-0', '--', 'redis-cli', '--raw', 'EVAL',
            "local r={} for i,k in ipairs(KEYS) do r[i]=tonumber(redis.call('HGET',k,'stock')) end return cjson.encode(r)",
            str(len(events)), *['inventory:event:' + event for event in events])
        stocks = json.loads(result)
        check(len(stocks) == len(events) and all(type(n) is int and n >= 0 for n in stocks), 'invalid stock')
        return stocks

    def orders(self, events):
        check(all(re.fullmatch(r'[a-z0-9-]+', v) for v in events), 'unsafe audit event ID')
        literals = ','.join("'" + v + "'" for v in events)
        return json.loads(self.sql("SELECT coalesce(json_agg(t),'[]'::json) FROM (SELECT order_id,event_id,customer_id,"
            "idempotency_key,quantity,status,failure_reason,extract(epoch from updated_at) AS terminal_at "
            "FROM orders.orders WHERE event_id IN (" + literals + ')) t'))

    def sample(self, e, events):
        stocks = self.stocks(events)
        sample = dict(at=time.time(), stock=sum(stocks), event_stocks=stocks,
                      inventory_lag=self.lag(SERVICES[0], 'orders.requested'),
                      payment_lag=self.lag(SERVICES[1], 'tickets.reserved'))
        e['samples'].append(sample)
        with (self.work / 'samples.jsonl').open('a') as log:
            log.write(json.dumps(sample) + '\n')
        print('SAMPLE', json.dumps(sample), flush=True)
        return sample

    def start_load(self, events, run_id, experiment):
        log = (self.work / 'k6.log').open('w')
        proc = subprocess.Popen(['k6', 'run', '--summary-export', str(self.work / 'k6-summary.json'),
            '-e', 'M6_AUDIT_ACKS=1', '-e', 'M6_EXPERIMENT=' + experiment,
            '-e', 'BASE_URL=' + self.gateway, '-e', 'EVENT_IDS=' + json.dumps(events),
            '-e', 'RUN_ID=' + run_id, 'scripts/chaos-load.js'], stdout=log, stderr=log)
        self.children.append((proc, log))
        return proc

    def accepted(self):
        # These are real response bodies, not IDs reconstructed from database totals.
        return sorted(set(re.findall(r'M6_ACK ([a-f0-9-]{36})', (self.work / 'k6.log').read_text())))

    def ramp(self, service):
        # Keep the chart's memory/JVM settings; bound concurrent startup instead of shrinking heaps.
        self.rollout(service)
        for replicas in range(2, 9):
            self.patch('scaledobject', service, dict(metadata=dict(annotations={
                'autoscaling.keda.sh/paused-replicas': str(replicas)}), spec=dict(maxReplicaCount=8)))
            self.kc('scale', 'deployment/' + service, '--replicas=' + str(replicas))
            self.rollout(service)
        check(self.get('deployment', service)['status'].get('readyReplicas') == 8, 'not eight Ready')

    def fault(self, experiment, e, events):
        f = e['fault']
        if experiment in ('inventory-kill', 'broker-restart'):
            if experiment == 'inventory-kill':
                pod = self.pods('inventory-service')[0]
            else:
                pods = self.get('pods', '-l', 'strimzi.io/name=ticketrush-kafka')['items']
                check(pods, 'no broker pods')
                pod = sorted(pods, key=lambda p: p['metadata']['name'])[0]
            name, uid = pod['metadata']['name'], pod['metadata']['uid']
            f.update(target=name, uid_before=uid)
            self.kc('delete', 'pod', name, '--wait=false')
            until = time.monotonic() + 180
            while time.monotonic() < until:
                candidates = (self.pods('inventory-service') if experiment == 'inventory-kill'
                              else self.get('pods', '-l', 'strimzi.io/name=ticketrush-kafka')['items'])
                replacements = [p for p in candidates if p['metadata']['uid'] != uid and
                    (experiment == 'inventory-kill' or p['metadata']['name'] == name) and
                    any(c['type'] == 'Ready' and c['status'] == 'True' for c in p.get('status', {}).get('conditions', []))]
                if replacements:
                    f['uid_after'] = replacements[0]['metadata']['uid']
                    break
                time.sleep(1)
            else:
                raise Failure('replacement pod not Ready')
        elif experiment == 'postgres-latency':
            begin = time.monotonic()
            self.sql('SELECT count(*) FROM orders.orders')
            f['baseline_query_seconds'] = time.monotonic() - begin
            log = (self.work / 'postgres-lock.log').open('w')
            proc = subprocess.Popen(['kubectl', '--context', CONTEXT, '-n', NAMESPACE, 'exec', 'postgres-0', '--',
                'sh', '-c', 'PGAPPNAME=m6-chaos psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d ticketrush -c '
                "'BEGIN; LOCK TABLE orders.orders IN ACCESS EXCLUSIVE MODE; SELECT pg_sleep(4); COMMIT;'"], stdout=log, stderr=log)
            self.children.append((proc, log))
            until = time.monotonic() + 10
            while time.monotonic() < until:
                locked = self.sql("SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a USING(pid) "
                    "WHERE a.application_name='m6-chaos' AND l.mode='AccessExclusiveLock' AND l.granted")
                if int(locked):
                    break
                time.sleep(0.1)
            else:
                raise Failure('Postgres lock not observed')
            begin = time.monotonic()
            self.sql('SELECT count(*) FROM orders.orders')
            f.update(injected_query_seconds=time.monotonic() - begin, method='postgres-lock: ACCESS EXCLUSIVE for pg_sleep(4)')
            check(proc.wait(timeout=10) == 0, 'Postgres lock command failed')
        elif experiment == 'consumer-ramp':
            f['method'] = 'staged inventory 1->8->1, then payment 1->8; unchanged chart resources/JVM'
            f['measurements'] = []
            for service in SERVICES:
                self.ramp(service)
                a = time.time()
                while time.time() - a < 15:
                    self.sample(e, events)
                    time.sleep(3)
                b = time.time()
                rows = self.orders(events)
                count = sum(o['status'] in TERMINAL and a <= o['terminal_at'] < b for o in rows)
                measurement = dict(service=service, started_at=a, finished_at=b, completions=count,
                                   orders_per_second=count / (b-a), ready=8)
                f['measurements'].append(measurement)
                f['replicas'].append(dict(service=service, at=b, ready=8,
                                          orders_per_second=measurement['orders_per_second']))
                if service == 'inventory-service':
                    self.patch('scaledobject', service, dict(metadata=dict(annotations={
                        'autoscaling.keda.sh/paused-replicas': '1'})))
                    self.kc('scale', 'deployment/' + service, '--replicas=1')
                    self.rollout(service)
        elif experiment == 'payment-declines':
            # No provider rollout during its in-memory charge audit. Cleanup follows the audit.
            f['method'] = '0.3 throughout load; configured before provider audit baseline'
            time.sleep(4)
        f['restored_at'] = time.time()

    def execute(self, experiment, run_id, event_count=24):
        start = time.time()
        e: dict = dict(version=1, source='live', context=CONTEXT, experiment=experiment, run_id=run_id,
                 date=datetime.datetime.now(datetime.timezone.utc).date().isoformat(),
                 started_at=start, timeout_seconds=self.timeout, timed_out=False, samples=[], fault={})
        self.before = self.snapshot()
        (self.work / 'before.json').write_text(json.dumps(self.before, indent=2))
        self.pause(1)
        if experiment == 'payment-declines':
            self.kc('set', 'env', 'deployment/fake-payment-provider', 'FAKE_PROVIDER_FAILURE_RATE=0.3')
            self.rollout('fake-payment-provider')
            e['fault'].update(failure_rate=0.3, rate_applied_at=time.time())
        provider_before = self.provider_stamp()
        e['fault']['provider_baseline_at'] = time.time()
        self.gateway = self.forward('api-gateway', 8080)
        provider = self.forward('fake-payment-provider', 8083)
        self.prom = self.forward('prometheus', 9090)
        events = [run_id + '-' + str(i) for i in range(event_count)]
        stock = 5000 if event_count == 1 else 500
        for event in events:
            self.kc('exec', 'redis-0', '--', 'redis-cli', 'HSET', 'inventory:event:' + event,
                    'stock', str(stock), 'priceCents', '100', 'currency', 'EUR')
        proc = self.start_load(events, run_id, experiment)
        load_start = time.time()
        fault_done = False
        e['fault']['replicas'] = []
        while True:
            check(time.monotonic() < self.deadline, 'run timeout')
            self.sample(e, events)
            if not fault_done and time.time() - load_start >= 20:
                e['fault']['at'] = time.time()
                baseline = self.orders(events)
                rate_one = sum(o['status'] in TERMINAL for o in baseline) / (time.time() - load_start)
                for service in SERVICES:
                    e['fault']['replicas'].append(dict(service=service, at=e['fault']['at'],
                        ready=self.get('deployment', service)['status'].get('readyReplicas', 0), orders_per_second=rate_one))
                self.fault(experiment, e, events)
                fault_done = True
                print('FAULT', json.dumps(e['fault']), flush=True)
            if proc.poll() is not None:
                check(proc.returncode == 0, 'k6 failed; see ' + str(self.work / 'k6.log'))
                break
            time.sleep(3)
        load_end = time.time()
        accepted = self.accepted()
        summary = json.loads((self.work / 'k6-summary.json').read_text())['metrics']['accepted_orders']
        check(len(accepted) == summary.get('count', summary.get('values', {}).get('count')), 'acknowledged-ID audit differs from k6 counter')
        e['k6'] = dict(exit_code=proc.returncode, started_at=load_start, finished_at=load_end,
                       accepted_order_ids=accepted)
        while True:
            check(time.monotonic() < self.deadline, 'run timeout while settling')
            s = self.sample(e, events)
            orders = self.orders(events)
            if {o['order_id'] for o in orders} == set(accepted) and all(o['status'] in TERMINAL for o in orders) and s['inventory_lag'] == s['payment_lag'] == 0:
                break
            time.sleep(3)
        drained = s['at']
        terminal_times = [o['terminal_at'] for o in orders if o['status'] in TERMINAL]
        resumed = [t for t in terminal_times if t > e['fault']['at']]
        check(resumed, 'no completed order after fault')
        e['recovery'] = dict(resumed_at=min(resumed), drained_at=drained, seconds=drained - e['fault']['at'])
        e['throughput'] = {}
        for name, a, b in [('before', load_start, e['fault']['at']),
                            ('during', e['fault']['at'], e['fault']['restored_at']),
                            ('after', e['fault']['restored_at'], time.time())]:
            count = sum(a <= t < b for t in terminal_times)
            e['throughput'][name] = dict(started_at=a, finished_at=b, completions=count, orders_per_second=count / (b-a))
        if experiment == 'consumer-ramp':
            e['fault']['scaledobjects_before'] = self.before['scaledobjects']
        remaining = self.stocks(events)
        ids = [o['order_id'] for o in orders]
        redis = json.loads(self.kc('exec', 'redis-0', '--', 'redis-cli', '--raw', 'EVAL',
            "local r={} for i,k in ipairs(KEYS) do r[i]=redis.call('HGETALL',k) end return cjson.encode(r)",
            str(len(ids)), *['inventory:order:' + oid for oid in ids]))
        charges = []
        for order, values in zip(orders, redis):
            state = dict(zip(values[::2], values[1::2]))
            held = int(state.get('quantity', '0')) if state.get('reserved') == '1' else 0
            released = state.get('released') == '1'
            order.update(reserved_quantity=held, released_quantity=held if released else 0,
                         release_count=1 if released else 0)
            response = self.http(provider + '/charges?' + urllib.parse.urlencode({'idempotencyKey': order['order_id']}))
            check(isinstance(response, list) and all(c['orderId'] == order['order_id'] for c in response), 'provider returned wrong charge')
            charges.append(dict(order_id=order['order_id'], count=len(response)))
            with (self.work / 'charges.jsonl').open('a') as log:
                log.write(json.dumps(dict(order_id=order['order_id'], response=response)) + '\n')
        e['invariants'] = dict(provider_stable=provider_before == self.provider_stamp(),
            events=[dict(event_id=event, initial_stock=stock, remaining_stock=n) for event, n in zip(events, remaining)],
            orders=orders, charges=charges, dlt=[])
        self.restore()
        after = self.snapshot()
        (self.work / 'after.json').write_text(json.dumps(after, indent=2))
        check(self.before == after, 'experiment settings did not restore exactly')
        if experiment == 'consumer-ramp':
            e['fault']['scaledobjects_after'] = after['scaledobjects']
        if experiment == 'payment-declines':
            # Restore only AFTER auditing the old provider's in-memory ledger.
            restored = time.time()
            e['fault']['restored_at'] = restored
            e['fault']['method'] += '; rate restored after charge audit; after window is idle'
            during = e['throughput']['during']
            during.update(finished_at=restored,
                          completions=sum(e['fault']['at'] <= t < restored for t in terminal_times))
            during['orders_per_second'] = during['completions'] / (restored - during['started_at'])
            self.prom = self.forward('prometheus', 9090)
            time.sleep(3)
            s = self.sample(e, events)
            check(s['inventory_lag'] == s['payment_lag'] == 0, 'post-restore lag not drained')
            e['recovery'].update(drained_at=s['at'], seconds=s['at'] - e['fault']['at'])
            end = time.time()
            e['throughput']['after'] = dict(started_at=restored, finished_at=end, completions=0, orders_per_second=0.0)
        e['finished_at'] = time.time()
        path = EVIDENCE / ('run-' + run_id + '.json')
        try:
            evaluate(e)
        except (Failure, KeyError, TypeError, ValueError):
            (self.work / 'failed-evidence.json').write_text(json.dumps(e, indent=2))
            raise
        path.write_text(json.dumps(e, indent=2))
        print('PASS', experiment, run_id, 'recovery', e['recovery']['seconds'], 'evidence', path, flush=True)
        return path, e


def run_once(experiment, timeout=600, event_count=24):
    run_id = 'm6-' + experiment + '-' + str(time.time_ns())
    live = Live(EVIDENCE / run_id, timeout)
    def interrupted(signum, frame):
        raise Failure('interrupted by signal ' + str(signum))
    signal.signal(signal.SIGINT, interrupted)
    signal.signal(signal.SIGTERM, interrupted)
    try:
        return live.execute(experiment, run_id, event_count)
    finally:
        live.restore()
        live.log.close()


def cleanup_probe(cause):
    supervisor = Live(EVIDENCE / ('probe-' + cause + '-' + str(time.time_ns())))
    before = supervisor.snapshot()
    existing = set(EVIDENCE.glob('m6-consumer-ramp-*'))
    output = (supervisor.work / 'child.log').open('w')
    args = ['bash', 'scripts/chaos.sh', '--experiment', 'consumer-ramp', '--runs', '1']
    if cause == 'timeout':
        args.extend(['--timeout-seconds', '18'])
    proc = subprocess.Popen(args, stdout=output, stderr=output, start_new_session=True)
    try:
        until = time.monotonic() + 120
        while time.monotonic() < until:
            candidates = set(EVIDENCE.glob('m6-consumer-ramp-*')) - existing
            if candidates:
                work = next(iter(candidates))
                log = work / 'k6.log'
                if log.exists() and 'M6_ACK' in log.read_text():
                    during = supervisor.snapshot()
                    check(during != before, 'probe did not observe a mutation')
                    if cause == 'SIGINT':
                        proc.send_signal(signal.SIGINT)
                    break
            check(proc.poll() is None, 'probe child ended before acknowledged load')
            time.sleep(0.5)
        else:
            raise Failure('probe child did not produce acknowledged load')
        check(proc.wait(timeout=240) == 1, 'aborted run must fail')
        after = supervisor.snapshot()
        check(before == after, 'abort did not restore effective settings')
        first_ids = sorted(set(re.findall(r'M6_ACK ([a-f0-9-]{36})', log.read_text())))
        path, rerun = run_once('inventory-kill')
        evidence = dict(source='live', context=CONTEXT, cause=cause, before=before, during=during, after=after,
            first_run_order_ids=first_ids, rerun_order_ids=rerun['k6']['accepted_order_ids'], rerun_evidence=str(path))
        (EVIDENCE / ('cleanup-' + cause + '.json')).write_text(json.dumps(evidence, indent=2))
        print('PASS cleanup', cause, 'and independently audited rerun', path, flush=True)
    finally:
        try:
            if proc.poll() is None:
                os.killpg(proc.pid, signal.SIGTERM)
                try:
                    proc.wait(timeout=20)
                except subprocess.TimeoutExpired:
                    os.killpg(proc.pid, signal.SIGKILL)
                    proc.wait(timeout=5)
                    supervisor.before = before
                    supervisor.restore()
                    raise Failure('probe child exceeded cleanup deadline; process group killed and supervisor restored settings')
        finally:
            output.close()
            supervisor.log.close()


def bottleneck():
    path, evidence = run_once('consumer-ramp', event_count=1)
    baseline = dict(evidence['throughput']['before'], ready_inventory_replicas=1)
    at_eight = next(m for m in evidence['fault']['measurements'] if m['service'] == 'inventory-service')
    scaled = {k: at_eight[k] for k in ('started_at', 'finished_at', 'completions', 'orders_per_second')}
    scaled['ready_inventory_replicas'] = 8
    measurements = [baseline, scaled]
    (EVIDENCE / 'bottleneck.json').write_text(json.dumps(dict(source='live', context=CONTEXT,
        event_count=1, run_evidence=str(path), measurements=measurements), indent=2))


def run_live():
    parser = argparse.ArgumentParser(description='M6 project-local chaos runner; reuse scripts/observability-up.sh stack')
    parser.add_argument('--experiment', choices=EXPERIMENTS)
    parser.add_argument('--runs', type=int, default=3)
    parser.add_argument('--resume', action='store_true', help='re-evaluate persisted live runs and collect only missing repetitions')
    parser.add_argument('--timeout-seconds', type=int, default=600)
    parser.add_argument('--cleanup-probes', action='store_true')
    parser.add_argument('--bottleneck', action='store_true')
    args = parser.parse_args()
    check(args.runs > 0 and args.timeout_seconds > 0, 'bounds must be positive')
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    if args.cleanup_probes:
        for cause in ('SIGINT', 'timeout'):
            cleanup_probe(cause)
    elif args.bottleneck:
        bottleneck()
    else:
        for experiment in ([args.experiment] if args.experiment else EXPERIMENTS):
            completed = 0
            if args.resume:
                for path in sorted(EVIDENCE.glob('run-*.json')):
                    evidence = json.loads(path.read_text())
                    if evidence['experiment'] == experiment:
                        check(evidence['source'] == 'live' and evidence['context'] == CONTEXT,
                              'resume requires project-local live evidence')
                        evaluate(evidence)
                        completed += 1
                        print('PASS re-evaluated prior live run', path, flush=True)
            for _ in range(max(0, args.runs - completed)):
                run_once(experiment, args.timeout_seconds)
        if args.experiment is None:
            for cause in ('SIGINT', 'timeout'):
                cleanup_probe(cause)
            bottleneck()
    print('RESULT: m6-chaos passed', flush=True)
