"""Offline regressions for cleanup control flow, not live chaos evidence."""
import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import Mock

from chaos_runner import Failure, SERVICES, evaluate, synthetic_evidence
from chaos_live import Live


class CleanupTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(dir=os.environ.get('TMPDIR'))
        self.addCleanup(self.temp.cleanup)
        self.live = Live(Path(self.temp.name) / 'unit-only')
        self.addCleanup(self.live.log.close)
        self.live.before = dict(
            resources={s: {'requests': {'memory': '384Mi'}} for s in SERVICES},
            consumer_env={s: 'original-options' for s in SERVICES},
            replicas={s: 1 for s in SERVICES}, provider_env={'FAKE_PROVIDER_FAILURE_RATE': '0'},
            scaledobjects={s: dict(spec={'maxReplicaCount': 2}, annotations={}) for s in SERVICES})
        self.calls = []
        self.live.sql = Mock(side_effect=lambda sql: self.calls.append(('sql', sql)))
        self.live.kc = Mock(side_effect=lambda *args: self.calls.append(('kubectl', *args)))
        self.live.patch = Mock(side_effect=lambda *args: self.calls.append(('patch', *args)))
        self.live.rollout = Mock(side_effect=lambda service: self.calls.append(('ready', service)))

    def test_downscale_and_wait_before_restoring_large_pod_resources(self):
        self.live.restore()
        for service in SERVICES:
            resource_index = next(i for i, c in enumerate(self.calls)
                                  if c[:3] == ('patch', 'deployment', service))
            earlier = self.calls[:resource_index]
            self.assertIn(('kubectl', 'scale', 'deployment/' + service, '--replicas=1'), earlier)
            self.assertIn(('ready', service), earlier)
            self.assertTrue(any(c[:3] == ('patch', 'scaledobject', service) and
                c[3]['metadata']['annotations'].get('autoscaling.keda.sh/paused-replicas') == '1'
                for c in earlier), 'KEDA must remain paused at the original count while shrinking')

    def test_ramp_readies_small_pod_before_incremental_scaling(self):
        self.live.get = Mock(return_value={'status': {'readyReplicas': 8}})
        self.live.ramp(SERVICES[0])
        scales = [c for c in self.calls if c[:2] == ('kubectl', 'scale')]
        self.assertEqual(['--replicas=' + str(n) for n in range(2, 9)], [c[-1] for c in scales])
        first_scale = self.calls.index(scales[0])
        self.assertIn(('ready', SERVICES[0]), self.calls[:first_scale])
        for call in scales:
            index = self.calls.index(call)
            self.assertEqual(('ready', SERVICES[0]), self.calls[index + 1])

    def test_ramp_preserves_chart_memory_and_jvm_defaults(self):
        self.live.get = Mock(return_value={'status': {'readyReplicas': 8}})
        self.live.ramp(SERVICES[0])
        self.assertFalse(any(c[:3] == ('patch', 'deployment', SERVICES[0]) for c in self.calls),
                         'The larger VM permits ramping without an artificial small-heap template')

    def test_rollout_waits_for_terminating_pods_before_next_phase(self):
        self.live.rollout = Live.rollout.__get__(self.live)
        self.live.kc = Mock()
        self.live.get = Mock(side_effect=[
            {'spec': {'replicas': 1, 'selector': {'matchLabels': {'app': 'inventory'}}}},
            {'items': [{'metadata': {'deletionTimestamp': 'synthetic'}}, {'metadata': {}}]},
            {'items': [{'metadata': {}}]},
        ])
        self.live.rollout(SERVICES[0])
        self.assertEqual(3, self.live.get.call_count,
                         'A deployment rollout alone does not wait for old pods to exit')

    def test_decline_audit_accepts_actual_provider_reason(self):
        evidence = synthetic_evidence()
        evidence['experiment'] = 'payment-declines'
        evidence['fault'].update(failure_rate=0.3, rate_applied_at=0.1, provider_baseline_at=0.2)
        evidence['k6']['accepted_order_ids'].append('declined')
        inv = evidence['invariants']
        inv['events'][0].update(initial_stock=2, remaining_stock=1)
        inv['orders'].append(dict(order_id='declined', event_id='show', customer_id='buyer-2',
            idempotency_key='key-2', quantity=1, status='FAILED', failure_reason='PROVIDER_DECLINED',
            reserved_quantity=1, released_quantity=1, release_count=1))
        inv['charges'].append(dict(order_id='declined', count=0))
        rejection = None
        try:
            evaluate(evidence)
        except Failure as exc:
            rejection = str(exc)
        self.assertIsNone(rejection, 'Provider reason must not be confused with release-event reason')

    def test_pod_query_excludes_terminating_rollout_pod(self):
        self.live.get = Mock(side_effect=[
            {'spec': {'selector': {'matchLabels': {'app': 'provider'}}}},
            {'items': [{'metadata': {'name': 'old', 'deletionTimestamp': 'synthetic'}},
                       {'metadata': {'name': 'new'}}]}])
        self.assertEqual(['new'], [p['metadata']['name'] for p in self.live.pods('fake-payment-provider')])

    def test_lag_rejects_stale_scrape_even_with_fresh_query_timestamp(self):
        self.live.prom = 'http://unit-only'
        now = time.time()
        def vector(value):
            return dict(status='success', data=dict(result=[{'value': [now, str(value)]}]))
        self.live.http = Mock(side_effect=[vector(0), vector(now - 60)])
        with self.assertRaises(Failure):
            self.live.lag('inventory-service', 'orders.requested')

    def test_lag_accepts_fresh_scrape(self):
        self.live.prom = 'http://unit-only'
        now = time.time()
        def vector(value):
            return dict(status='success', data=dict(result=[{'value': [now, str(value)]}]))
        self.live.http = Mock(side_effect=[vector(2), vector(now - 5)])
        self.assertEqual(2, self.live.lag('inventory-service', 'orders.requested'))

    def test_child_stop_error_does_not_skip_cluster_restoration(self):
        proc, log = Mock(), Mock()
        proc.poll.return_value = None
        proc.terminate.side_effect = OSError('synthetic child stop failure')
        self.live.children = [(proc, log)]
        try:
            self.live.restore()
        except (Failure, OSError):
            pass
        self.assertTrue(any(c[0] == 'sql' for c in self.calls), 'DB restoration must still execute')
        self.assertTrue(any(c[:3] == ('patch', 'deployment', SERVICES[0]) for c in self.calls))
        log.close.assert_called_once()


if __name__ == '__main__':
    unittest.main(verbosity=2)
