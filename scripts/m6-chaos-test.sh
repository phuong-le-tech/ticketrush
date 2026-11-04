#!/usr/bin/env bash
# Test only. No bootstrap, fault injection, or production evaluator lives here.
# Default: offline public evaluator tests, safe context refusal, and missing-evidence REDs.
# --results DIR: also audit real run-*.json and cleanup-*.json produced by the runner.
# --offline: evaluator/context tests only. --regressions: also run unchanged M1-M5 checks.
set -euo pipefail
cd "$(dirname "$0")/.."
exec python3 - "$@" <<'PY'
import copy
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path.cwd()
RUNNER = ROOT / 'scripts/m6-chaos.sh'
RESULTS = ROOT / 'docs/evidence/m6'
OFFLINE = '--offline' in sys.argv
REGRESSIONS = '--regressions' in sys.argv
if '--results' in sys.argv:
    RESULTS = Path(sys.argv[sys.argv.index('--results') + 1]).resolve()
EXPERIMENTS = ('inventory-kill', 'broker-restart', 'payment-declines',
               'postgres-latency', 'consumer-ramp')


def fixture(experiment='inventory-kill'):
    # Synthetic data ONLY. Never write these into docs/evidence or README.
    return dict(version=1, experiment=experiment, run_id='synthetic-only',
        started_at=100, finished_at=150, timeout_seconds=60, timed_out=False,
        k6=dict(exit_code=0, started_at=105, finished_at=140,
                accepted_order_ids=['order-1']),
        fault=dict(at=115, restored_at=125, target='inventory-service',
                   uid_before='old-pod', uid_after='new-pod'),
        recovery=dict(resumed_at=126, drained_at=145, seconds=30),
        throughput=dict(before=dict(started_at=105, finished_at=115, completions=0, orders_per_second=0),
                        during=dict(started_at=115, finished_at=125, completions=0, orders_per_second=0),
                        after=dict(started_at=125, finished_at=140, completions=1, orders_per_second=1 / 15)),
        samples=[dict(at=110, stock=1, inventory_lag=0, payment_lag=0),
                 dict(at=120, stock=0, inventory_lag=1, payment_lag=1),
                 dict(at=145, stock=0, inventory_lag=0, payment_lag=0)],
        invariants=dict(provider_stable=True,
            events=[dict(event_id='show-1', initial_stock=1, remaining_stock=0)],
            orders=[dict(order_id='order-1', event_id='show-1', customer_id='buyer-1',
                         idempotency_key='key-1', quantity=1, status='CONFIRMED')],
            charges=[dict(order_id='order-1', count=1)], dlt=[]))


def changed(path, value, base=None):
    result = copy.deepcopy(fixture() if base is None else base)
    node = result
    for key in path[:-1]:
        node = node[key]
    node[path[-1]] = value
    return result


def valid_cases():
    quantity_four = fixture()
    quantity_four['invariants']['events'][0]['initial_stock'] = 4
    quantity_four['invariants']['orders'][0]['quantity'] = 4
    quantity_four['samples'][0]['stock'] = 4
    compensated = fixture('payment-declines')
    compensated['fault'] = dict(at=115, restored_at=125, failure_rate=0.3,
                                rate_applied_at=105, provider_baseline_at=110)
    compensated['invariants']['events'][0].update(initial_stock=5, remaining_stock=4)
    compensated['samples'][0]['stock'] = 5
    compensated['samples'][2]['stock'] = 4
    compensated['k6']['accepted_order_ids'].append('declined-1')
    compensated['invariants']['orders'].append(dict(order_id='declined-1', event_id='show-1',
        customer_id='buyer-2', idempotency_key='key-2', quantity=4, status='FAILED',
        failure_reason='PAYMENT_FAILED', reserved_quantity=4, released_quantity=4, release_count=1))
    compensated['invariants']['charges'].append(dict(order_id='declined-1', count=0))
    dead_letter = fixture('postgres-latency')
    dead_letter['fault'] = dict(at=115, restored_at=125, baseline_query_seconds=0.01,
                               injected_query_seconds=0.5, method='postgres-lock')
    dead_letter['k6']['accepted_order_ids'].append('poison-1')
    dead_letter['invariants']['dlt'] = [dict(order_id='poison-1', topic='orders.requested.DLT',
                                          partition=0, offset=1)]
    return [('one-sale-zero-stock', fixture()), ('quantity-four', quantity_four),
            ('compensated-decline', compensated), ('correlated-DLT', dead_letter)]


def corrupt_cases():
    order = fixture()['invariants']['orders'][0]
    return [
        ('oversell', changed(('invariants', 'events', 0, 'initial_stock'), 0)),
        ('negative-stock', changed(('invariants', 'events', 0, 'remaining_stock'), -1)),
        ('negative-stock-sample', changed(('samples', 1, 'stock'), -1)),
        ('stock-leak', changed(('invariants', 'events', 0, 'remaining_stock'), 1)),
        ('lost-order', changed(('k6', 'accepted_order_ids'), ['order-1', 'lost-1'])),
        ('substituted-ID-same-count', changed(('k6', 'accepted_order_ids'), ['other-1'])),
        ('duplicate-order', changed(('invariants', 'orders'), [order, copy.deepcopy(order)])),
        ('duplicate-charge', changed(('invariants', 'charges', 0, 'count'), 2)),
        ('duplicate-charge-row', changed(('invariants', 'charges'),
                                       [dict(order_id='order-1', count=1)] * 2)),
        ('missing-charge', changed(('invariants', 'charges'), [])),
        ('unresolved-order', changed(('invariants', 'orders', 0, 'status'), 'PENDING')),
        ('empty-orders', changed(('invariants', 'orders'), [])),
        ('empty-samples', changed(('samples',), [])),
        ('null-invariants', changed(('invariants',), None)),
        ('timestamp-string', changed(('started_at',), '100')),
        ('duration-boolean', changed(('timeout_seconds',), True)),
        ('provider-restart', changed(('invariants', 'provider_stable'), False)),
        ('boolean-stock', changed(('invariants', 'events', 0, 'initial_stock'), True)),
        ('string-quantity', changed(('invariants', 'orders', 0, 'quantity'), '1')),
        ('quantity-zero', changed(('invariants', 'orders', 0, 'quantity'), 0)),
        ('quantity-five', changed(('invariants', 'orders', 0, 'quantity'), 5)),
        ('NaN', changed(('throughput', 'before', 'orders_per_second'), float('nan'))),
        ('zero-duration', changed(('throughput', 'before', 'finished_at'), 105)),
        ('incorrect-throughput', changed(('throughput', 'before', 'orders_per_second'), 99)),
        ('missing-recovery', changed(('recovery',), {})),
        ('recovery-before-fault', changed(('recovery', 'resumed_at'), 114)),
        ('lag-not-drained', changed(('samples', 2, 'payment_lag'), 1)),
        ('wrong-recovery-duration', changed(('recovery', 'seconds'), 1)),
        ('timeout', changed(('timed_out',), True)),
        ('elapsed-timeout', changed(('timeout_seconds',), 1)),
        ('k6-failed', changed(('k6', 'exit_code'), 1)),
        ('kill-outside-load', changed(('fault', 'at'), 141)),
        ('same-pod-UID', changed(('fault', 'uid_after'), 'old-pod')),
    ]


class CliAssertions:
    def runner(self, *args, env=None):
        self.assertTrue(RUNNER.is_file(),
                        'M6 chaos behavior is missing: scripts/m6-chaos.sh has not been implemented')
        result = subprocess.run(['bash', str(RUNNER), *args], cwd=ROOT, env=env,
                                capture_output=True, text=True, timeout=15)
        self.assertNotIn('Traceback (most recent call last)', result.stderr,
                         'Public CLI must report invalid evidence without a Python traceback')
        return result

    def evaluate(self, evidence, expected):
        with tempfile.TemporaryDirectory(prefix='m6-test-', dir=os.environ.get('TMPDIR')) as work:
            path = Path(work) / 'synthetic.json'
            path.write_text(json.dumps(evidence))
            result = self.runner('--evaluate', str(path))
        self.assertEqual(expected, result.returncode, result.stdout + result.stderr)
        self.assertIn(('PASS' if expected == 0 else 'FAIL'), result.stdout + result.stderr)


class ContractTests(CliAssertions, unittest.TestCase):
    def test_AC1_validBoundaries(self):
        for name, evidence in valid_cases():
            with self.subTest(case=name):
                self.evaluate(evidence, 0)

    def test_AC1_rejectCorruption(self):
        for name, evidence in corrupt_cases():
            with self.subTest(case=name):
                self.evaluate(evidence, 1)

    def test_AC1_rejectMalformedEvidence(self):
        for name, text in [('empty', ''), ('null', 'null'), ('malformed', '{'),
                           ('array', '[]'), ('string', '"evidence"'), ('empty-object', '{}')]:
            with self.subTest(case=name):
                with tempfile.TemporaryDirectory(prefix='m6-json-', dir=os.environ.get('TMPDIR')) as work:
                    path = Path(work) / 'bad.json'
                    path.write_text(text)
                    result = self.runner('--evaluate', str(path))
                self.assertEqual(1, result.returncode, result.stdout + result.stderr)
                self.assertIn('FAIL', result.stdout + result.stderr)

    def test_AC1_rejectDuplicateIdempotencyTuple(self):
        evidence = fixture()
        second = dict(evidence['invariants']['orders'][0], order_id='order-2', status='REJECTED')
        evidence['invariants']['orders'].append(second)
        evidence['k6']['accepted_order_ids'].append('order-2')
        evidence['invariants']['charges'].append(dict(order_id='order-2', count=0))
        self.evaluate(evidence, 1)

    def test_AC1_selftestIsOffline(self):
        with tempfile.TemporaryDirectory(prefix='m6-offline-', dir=os.environ.get('TMPDIR')) as work:
            # A genuinely empty kubeconfig, not a replacement kubectl or mocked cluster.
            kubeconfig = Path(work) / 'config'
            kubeconfig.write_text('apiVersion: v1\nkind: Config\nclusters: []\ncontexts: []\nusers: []\n')
            result = self.runner('--selftest', env=dict(os.environ, KUBECONFIG=str(kubeconfig)))
        self.assertEqual(0, result.returncode, result.stdout + result.stderr)
        self.assertIn('PASS', result.stdout + result.stderr)

    def test_AC2_contextRefusal(self):
        for context in ('', 'kind-other-project', 'production'):
            with self.subTest(context=context):
                with tempfile.TemporaryDirectory(prefix='m6-context-', dir=os.environ.get('TMPDIR')) as work:
                    config = dict(apiVersion='v1', kind='Config',
                        clusters=[{'name': 'unreachable', 'cluster': {'server': 'http://127.0.0.1:1'}}],
                        users=[{'name': 'local', 'user': {}}],
                        contexts=[{'name': context, 'context': {'cluster': 'unreachable', 'user': 'local'}}])
                    config['current-context'] = context
                    path = Path(work) / 'kubeconfig.json'
                    path.write_text(json.dumps(config))
                    result = self.runner(env=dict(os.environ, KUBECONFIG=str(path)))
                    after = path.read_text()
                self.assertEqual(1, result.returncode, result.stdout + result.stderr)
                self.assertIn('kind-ticketrush', result.stdout + result.stderr)
                self.assertRegex(result.stdout + result.stderr, r'(?i)(refus.*context|context.*not.*allowed)')
                self.assertEqual(json.dumps(config), after, 'Must not switch the user kube context')

    def test_AC2_inventoryKillRequiresTerminalConservation(self):
        evidence = valid_cases()[3][1]
        evidence['experiment'] = 'inventory-kill'
        evidence['fault'] = fixture()['fault']
        self.evaluate(evidence, 1)

    def test_AC3_brokerRestartRequiresTerminalConservation(self):
        evidence = valid_cases()[3][1]
        evidence['experiment'] = 'broker-restart'
        evidence['fault'] = dict(fixture()['fault'], target='kafka-broker')
        self.evaluate(evidence, 1)

    def test_AC3_brokerRestartRequiresChangedUID(self):
        evidence = fixture('broker-restart')
        evidence['fault']['target'] = 'kafka-broker'
        self.evaluate(evidence, 0)
        evidence['fault']['uid_after'] = 'old-pod'
        self.evaluate(evidence, 1)

    def test_AC4_declinesRelease(self):
        evidence = valid_cases()[2][1]
        self.evaluate(evidence, 0)
        for field, value in [('released_quantity', 0), ('release_count', 2)]:
            with self.subTest(field=field):
                self.evaluate(changed(('invariants', 'orders', 1, field), value, evidence), 1)

    def test_AC4_declinesCannotBeCharged(self):
        evidence = valid_cases()[2][1]
        evidence['invariants']['charges'][1]['count'] = 1
        self.evaluate(evidence, 1)

    def test_AC4_requiresObservedSuccessAndDecline(self):
        all_success = fixture('payment-declines')
        all_success['fault'] = valid_cases()[2][1]['fault']
        all_decline = copy.deepcopy(all_success)
        all_decline['invariants']['orders'][0].update(status='FAILED', failure_reason='PAYMENT_FAILED',
            reserved_quantity=1, released_quantity=1, release_count=1)
        all_decline['invariants']['charges'][0]['count'] = 0
        all_decline['invariants']['events'][0]['remaining_stock'] = 1
        all_decline['samples'][1]['stock'] = 1
        all_decline['samples'][2]['stock'] = 1
        for name, evidence in [('no-decline', all_success), ('no-success', all_decline)]:
            with self.subTest(case=name):
                self.evaluate(evidence, 1)

    def test_AC4_failureRateAndBaseline(self):
        evidence = valid_cases()[2][1]
        for field, value in [('failure_rate', 0), ('provider_baseline_at', 104)]:
            with self.subTest(field=field):
                self.evaluate(changed(('fault', field), value, evidence), 1)

    def test_AC5_postgresLatency(self):
        evidence = valid_cases()[3][1]
        self.evaluate(evidence, 0)
        evidence['fault']['injected_query_seconds'] = 0.01
        self.evaluate(evidence, 1)

    def test_AC5_DLTRequiresAcknowledgedOrderCorrelation(self):
        evidence = valid_cases()[3][1]
        evidence['invariants']['dlt'][0]['order_id'] = 'unrelated-poison'
        self.evaluate(evidence, 1)

    def test_AC6_rebalanceRamp(self):
        evidence = fixture('consumer-ramp')
        evidence['fault'] = dict(at=115, restored_at=125, replicas=[
            dict(service='inventory-service', at=115, ready=1, orders_per_second=0.1),
            dict(service='payment-service', at=115, ready=1, orders_per_second=0.1),
            dict(service='inventory-service', at=125, ready=8, orders_per_second=0.2),
            dict(service='payment-service', at=125, ready=8, orders_per_second=0.2)],
            scaledobjects_before={'inventory': {'maxReplicaCount': 2}, 'payment': {'maxReplicaCount': 2}},
            scaledobjects_after={'inventory': {'maxReplicaCount': 2}, 'payment': {'maxReplicaCount': 2}})
        self.evaluate(evidence, 0)
        for name, broken in [
            ('inventory-no-baseline', changed(('fault', 'replicas', 0, 'ready'), 8, evidence)),
            ('payment-no-baseline', changed(('fault', 'replicas', 1, 'ready'), 8, evidence)),
            ('inventory-not-eight', changed(('fault', 'replicas', 2, 'ready'), 2, evidence)),
            ('payment-not-eight', changed(('fault', 'replicas', 3, 'ready'), 2, evidence)),
            ('inventory-baseline-no-rate', changed(('fault', 'replicas', 0, 'orders_per_second'), None, evidence)),
            ('payment-baseline-no-rate', changed(('fault', 'replicas', 1, 'orders_per_second'), None, evidence)),
            ('inventory-eight-NaN-rate', changed(('fault', 'replicas', 2, 'orders_per_second'), float('nan'), evidence)),
            ('payment-eight-negative-rate', changed(('fault', 'replicas', 3, 'orders_per_second'), -1, evidence)),
            ('scaler-not-restored', changed(('fault', 'scaledobjects_after', 'payment', 'maxReplicaCount'), 8, evidence)),
        ]:
            with self.subTest(case=name):
                self.evaluate(broken, 1)


@unittest.skipIf(OFFLINE, 'Offline selection excludes real-evidence acceptance checks')
class RealEvidenceTests(CliAssertions, unittest.TestCase):
    # Audit persisted live evidence, never synthetic selftest output.
    def runs(self, experiment):
        files = sorted(RESULTS.glob('run-*.json'))
        self.assertTrue(files, 'No measured M6 run evidence: ' + str(RESULTS))
        rows = [(p, json.loads(p.read_text())) for p in files]
        selected = [(p, e) for p, e in rows if e['experiment'] == experiment]
        self.assertGreaterEqual(len(selected), 3, experiment + ': at least three real runs required')
        self.assertEqual(len(selected), len({e['run_id'] for _, e in selected}), 'Duplicate run IDs')
        return selected

    def audit(self, experiment):
        for path, evidence in self.runs(experiment):
            with self.subTest(path=str(path)):
                self.assertEqual('kind-ticketrush', evidence['context'])
                self.assertEqual('live', evidence['source'], 'Synthetic fixtures are not chaos evidence')
                result = self.runner('--evaluate', str(path))
                self.assertEqual(0, result.returncode, result.stdout + result.stderr)

    def test_AC2_inventoryKill(self):
        self.audit('inventory-kill')

    def test_AC3_brokerRestart(self):
        self.audit('broker-restart')

    def test_AC4_realDeclinesRelease(self):
        self.audit('payment-declines')

    def test_AC5_realPostgresLatency(self):
        self.audit('postgres-latency')

    def test_AC6_realRebalanceRamp(self):
        self.audit('consumer-ramp')

    def test_AC6_abortRestores(self):
        for cause in ('SIGINT', 'timeout'):
            with self.subTest(cause=cause):
                path = RESULTS / ('cleanup-' + cause + '.json')
                self.assertTrue(path.is_file(), 'Real abort/timeout cleanup evidence missing: ' + str(path))
                evidence = json.loads(path.read_text())
                self.assertEqual('live', evidence['source'])
                self.assertEqual(cause, evidence['cause'])
                self.assertEqual('kind-ticketrush', evidence['context'])
                self.assertNotEqual(evidence['before'], evidence['during'], 'Abort must occur after mutation')
                self.assertEqual({'inventory-service', 'payment-service'}, set(evidence['before']['replicas']))
                self.assertEqual(2, len(evidence['before']['scaledobjects']))
                self.assertIn('FAKE_PROVIDER_FAILURE_RATE', evidence['before']['provider_env'])
                self.assertTrue(evidence['before']['db_settings'], 'Capture relevant DB settings/lock state')
                self.assertEqual(evidence['before']['replicas'], evidence['after']['replicas'])
                self.assertEqual(evidence['before']['scaledobjects'], evidence['after']['scaledobjects'])
                self.assertEqual(evidence['before']['provider_env'], evidence['after']['provider_env'])
                self.assertEqual(evidence['before']['db_settings'], evidence['after']['db_settings'])
                self.assertTrue(evidence['first_run_order_ids'])
                self.assertTrue(evidence['rerun_order_ids'])
                self.assertTrue(set(evidence['first_run_order_ids']).isdisjoint(evidence['rerun_order_ids']))
                rerun_path = ROOT / evidence['rerun_evidence']
                self.assertTrue(rerun_path.is_file(), 'Cleanup must link a completed rerun audit')
                rerun = json.loads(rerun_path.read_text())
                self.assertEqual('live', rerun['source'])
                self.assertEqual(set(evidence['rerun_order_ids']), set(rerun['k6']['accepted_order_ids']))
                result = self.runner('--evaluate', str(rerun_path))
                self.assertEqual(0, result.returncode, result.stdout + result.stderr)

    def test_AC7_resultsTraceable(self):
        readme = (ROOT / 'README.md').read_text()
        for experiment in EXPERIMENTS:
            for path, evidence in self.runs(experiment):
                with self.subTest(run=path.name):
                    link = str(path.relative_to(ROOT))
                    rows = [line for line in readme.splitlines() if line.startswith('|') and link in line]
                    self.assertEqual(1, len(rows), 'README must have exactly one table row linking ' + link)
                    row = rows[0]
                    self.assertIn(experiment, row)
                    self.assertIn(evidence['date'], row)
                    self.assertIn(str(evidence['recovery']['seconds']), row)
                    self.assertIn('PASS', row)
                    for window in ('before', 'during', 'after'):
                        self.assertIn(str(evidence['throughput'][window]['orders_per_second']), row)
        self.assertRegex(readme.lower(), r'(?s)bottlenecks.*single.show.*partition')
        bottleneck_path = RESULTS / 'bottleneck.json'
        self.assertTrue(bottleneck_path.is_file(), 'Missing measured single-show throughput evidence')
        bottleneck = json.loads(bottleneck_path.read_text())
        self.assertEqual('live', bottleneck['source'])
        self.assertEqual(1, bottleneck['event_count'])
        self.assertEqual([1, 8], [s['ready_inventory_replicas'] for s in bottleneck['measurements']])
        for measurement in bottleneck['measurements']:
            self.assertGreater(measurement['finished_at'], measurement['started_at'])
            self.assertGreater(measurement['completions'], 0)
            self.assertAlmostEqual(measurement['completions'] /
                (measurement['finished_at'] - measurement['started_at']), measurement['orders_per_second'])
            self.assertIn(str(measurement['orders_per_second']), readme)
        self.assertIn(str(bottleneck_path.relative_to(ROOT)), readme)

    def test_AC9_honestReport(self):
        progress = (ROOT / 'docs/PROGRESS.md').read_text()
        design = (ROOT / 'docs/DESIGN.md').read_text()
        self.assertIn('### Milestone 6', progress)
        self.assertIn('scripts/m6-chaos.sh', progress)
        self.assertIn('NOT', progress)
        self.assertIn('M6', design)
        manifest = RESULTS / 'verification.json'
        self.assertTrue(manifest.is_file(), 'M6 command/log traceability manifest missing')
        commands = json.loads(manifest.read_text())['commands']
        for command in ('scripts/m6-chaos.sh --selftest', 'scripts/m6-chaos.sh',
                        'mvn verify', 'scripts/m3-chart-check.sh', 'scripts/m5-chart-check.sh'):
            with self.subTest(command=command):
                record = commands[command]
                self.assertEqual(0, record['exit_code'])
                log = ROOT / record['log']
                self.assertTrue(log.is_file(), 'Missing raw verification log: ' + str(log))
                self.assertTrue(record['summary'].strip(), 'Empty command result summary')
                self.assertIn(record['summary'], log.read_text())
                self.assertIn(command, progress)
                self.assertIn(record['summary'], progress)
        for experiment in EXPERIMENTS:
            self.runs(experiment)


class RegressionTests(unittest.TestCase):
    @unittest.skipUnless(REGRESSIONS, 'Select --regressions to run the unchanged Maven/chart suite')
    def test_AC8_regressions(self):
        env = dict(os.environ, JAVA_HOME='/Library/Java/JavaVirtualMachines/jdk-21.jdk/Contents/Home')
        for command in (['mvn', 'verify'], ['bash', 'scripts/m3-chart-check.sh'],
                        ['bash', 'scripts/m5-chart-check.sh']):
            with self.subTest(command=command):
                result = subprocess.run(command, cwd=ROOT, env=env, timeout=1500)
                self.assertEqual(0, result.returncode)


suite = unittest.TestSuite([
    unittest.defaultTestLoader.loadTestsFromTestCase(ContractTests),
    unittest.defaultTestLoader.loadTestsFromTestCase(RealEvidenceTests),
    unittest.defaultTestLoader.loadTestsFromTestCase(RegressionTests),
])
result = unittest.TextTestRunner(verbosity=2).run(suite)
sys.exit(0 if result.wasSuccessful() else 1)
PY
