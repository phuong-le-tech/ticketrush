"""M6 evidence validation and bounded, project-local chaos experiments."""
import copy
import json
import math
from pathlib import Path
import subprocess
import sys

CONTEXT = 'kind-ticketrush'
NAMESPACE = 'ticketrush'
SERVICES = ('inventory-service', 'payment-service')
EXPERIMENTS = ('inventory-kill', 'broker-restart', 'payment-declines',
               'postgres-latency', 'consumer-ramp')
TERMINAL = ('CONFIRMED', 'REJECTED', 'FAILED')
EVIDENCE = Path('docs/evidence/chaos')


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


def rate(window):
    a, b, count = (window[k] for k in ('started_at', 'finished_at', 'completions'))
    check(number(a) and number(b) and b > a and integer(count) and count >= 0,
          'invalid completion window')
    value = window['orders_per_second']
    check(number(value) and value >= 0 and math.isclose(value, count / (b - a)),
          'throughput does not match measured completions/duration')


def evaluate(e):
    check(isinstance(e, dict) and integer(e['version']) and e['version'] == 1 and e['timed_out'] is False,
          'invalid version or timeout')
    start, end, limit = (e[k] for k in ('started_at', 'finished_at', 'timeout_seconds'))
    check(all(number(v) for v in (start, end, limit)) and 0 < end - start <= limit,
          'invalid timestamps or elapsed timeout')
    check(e['experiment'] in EXPERIMENTS, 'unknown experiment')
    k6, fault, recovery = (e[k] for k in ('k6', 'fault', 'recovery'))
    check(integer(k6['exit_code']) and k6['exit_code'] == 0, 'k6 failed')
    a, b = k6['started_at'], k6['finished_at']
    check(number(a) and number(b) and start <= a < b <= end, 'invalid load interval')
    at, restored = fault['at'], fault['restored_at']
    check(number(at) and number(restored) and a <= at < b and at <= restored <= end,
          'fault outside load or invalid restoration timestamp')
    resumed, drained, seconds = (recovery[k] for k in ('resumed_at', 'drained_at', 'seconds'))
    check(all(number(v) for v in (resumed, drained, seconds)) and
          at < resumed <= end and restored <= drained <= end and
          math.isclose(seconds, drained - at), 'invalid recovery')
    samples = e['samples']
    check(isinstance(samples, list) and samples, 'missing samples')
    for s in samples:
        check(number(s['at']) and start <= s['at'] <= end, 'invalid sample time')
        check(all(number(s[k]) and s[k] >= 0 for k in ('stock', 'inventory_lag', 'payment_lag')),
              'negative or missing stock/lag sample')
    check(any(s['at'] >= drained and s['inventory_lag'] == s['payment_lag'] == 0
              for s in samples), 'no drained recovery sample')
    for name in ('before', 'during', 'after'):
        window = e['throughput'][name]
        rate(window)
        check(a <= window['started_at'] < window['finished_at'] <= end,
              'completion window outside run')
    evaluate_invariants(e)
    experiment = e['experiment']
    if experiment in ('inventory-kill', 'broker-restart'):
        check(nonempty(fault['uid_before']) and nonempty(fault['uid_after']) and
              fault['uid_before'] != fault['uid_after'], 'pod UID did not change')
        check(not e['invariants']['dlt'], 'pod faults require full terminal conservation')
    elif experiment == 'payment-declines':
        check(number(fault['failure_rate']) and fault['failure_rate'] == 0.3,
              'failure rate must be 0.3')
        applied, baseline = fault['rate_applied_at'], fault['provider_baseline_at']
        check(number(applied) and number(baseline) and start <= applied <= baseline <= at,
              'provider baseline predates failure-rate rollout')
        orders = e['invariants']['orders']
        check(any(o['status'] == 'CONFIRMED' for o in orders) and
              any(o['status'] == 'FAILED' and o['failure_reason'] in ('PAYMENT_FAILED', 'PROVIDER_DECLINED') for o in orders),
              'must observe both successes and declines')
    elif experiment == 'postgres-latency':
        baseline, injected = fault['baseline_query_seconds'], fault['injected_query_seconds']
        check(number(baseline) and number(injected) and 0 <= baseline < injected and
              nonempty(fault['method']), 'no measured Postgres latency increase')
    elif experiment == 'consumer-ramp':
        check(fault['scaledobjects_before'] and
              fault['scaledobjects_before'] == fault['scaledobjects_after'], 'scalers not restored')
        for service in SERVICES:
            ss = [s for s in fault['replicas'] if s['service'] == service]
            for s in ss:
                check(number(s['at']) and start <= s['at'] <= end and
                      integer(s['ready']) and number(s['orders_per_second']) and
                      s['orders_per_second'] >= 0, 'invalid replica measurement')
            check(any(s['ready'] == 1 and any(t['ready'] == 8 and t['at'] > s['at'] for t in ss)
                      for s in ss), service + ': missing Ready 1 -> 8 measurements')


def evaluate_invariants(e):
    inv = e['invariants']
    check(inv['provider_stable'] is True, 'provider restarted; charge audit lost')
    events, orders, charges, dlt = (inv[k] for k in ('events', 'orders', 'charges', 'dlt'))
    check(all(isinstance(v, list) for v in (events, orders, charges, dlt)) and events and orders,
          'missing invariant evidence')
    accepted = e['k6']['accepted_order_ids']
    check(isinstance(accepted, list) and accepted and all(nonempty(i) for i in accepted) and
          len(accepted) == len(set(accepted)), 'missing or duplicate acknowledged IDs')
    ids = [o['order_id'] for o in orders]
    keys = [(o['event_id'], o['customer_id'], o['idempotency_key']) for o in orders]
    check(all(nonempty(i) for i in ids) and len(ids) == len(set(ids)) and
          len(keys) == len(set(keys)), 'duplicate order or idempotency tuple')
    dead = set()
    for entry in dlt:
        check(entry['order_id'] in accepted and nonempty(entry['topic']) and entry['topic'].endswith('.DLT') and
              integer(entry['partition']) and entry['partition'] >= 0 and
              integer(entry['offset']) and entry['offset'] >= 0, 'uncorrelated or malformed DLT')
        dead.add(entry['order_id'])
    check(set(ids) | dead == set(accepted), 'lost or substituted acknowledged order')
    event_ids = [v['event_id'] for v in events]
    check(all(nonempty(i) for i in event_ids) and len(event_ids) == len(set(event_ids)), 'duplicate event')
    check({o['event_id'] for o in orders} <= set(event_ids), 'unknown event')
    charge_ids = [c['order_id'] for c in charges]
    check(len(charge_ids) == len(set(charge_ids)) and set(charge_ids) == set(ids), 'incomplete/duplicate charge audit')
    by_id = {c['order_id']: c['count'] for c in charges}
    for o in orders:
        check(o['status'] in TERMINAL and integer(o['quantity']) and 1 <= o['quantity'] <= 4 and
              nonempty(o['customer_id']) and nonempty(o['idempotency_key']), 'unsettled or malformed order')
        count = by_id[o['order_id']]
        check(integer(count) and count == (1 if o['status'] == 'CONFIRMED' else 0),
              'duplicate/missing charge or charged failure')
        if o['status'] == 'FAILED':
            reserved, released, releases = (o[k] for k in ('reserved_quantity', 'released_quantity', 'release_count'))
            check(all(integer(v) for v in (reserved, released, releases)) and
                  reserved == released and releases == (1 if reserved else 0), 'reservation not released once')
    for event in events:
        initial, remaining = event['initial_stock'], event['remaining_stock']
        sold = sum(o['quantity'] for o in orders if o['event_id'] == event['event_id'] and o['status'] == 'CONFIRMED')
        check(integer(initial) and integer(remaining) and 0 <= remaining <= initial and
              sold + remaining == initial, 'oversell or stock not conserved')


def synthetic_evidence() -> dict:
    # Synthetic data ONLY. Never persisted as live results.
    e = dict(version=1, experiment='inventory-kill', timed_out=False,
             started_at=0, finished_at=10, timeout_seconds=20,
             k6=dict(exit_code=0, started_at=1, finished_at=8, accepted_order_ids=['order']),
             fault=dict(at=3, restored_at=4, uid_before='old', uid_after='new'),
             recovery=dict(resumed_at=5, drained_at=9, seconds=6),
             throughput={name: dict(started_at=a, finished_at=b, completions=n, orders_per_second=n / (b-a))
                         for name, a, b, n in [('before', 1, 3, 0), ('during', 3, 4, 0), ('after', 4, 9, 1)]},
             samples=[dict(at=9, stock=0, inventory_lag=0, payment_lag=0)],
             invariants=dict(provider_stable=True, events=[dict(event_id='show', initial_stock=1, remaining_stock=0)],
                             orders=[dict(order_id='order', event_id='show', customer_id='buyer', idempotency_key='key',
                                          quantity=1, status='CONFIRMED')], charges=[dict(order_id='order', count=1)], dlt=[]))
    return e


def selftest():
    e = synthetic_evidence()
    evaluate(e)
    for name in ('oversell', 'lost order', 'duplicate charge'):
        bad = copy.deepcopy(e)
        if name == 'oversell':
            bad['invariants']['events'][0]['remaining_stock'] = -1
        elif name == 'lost order':
            bad['k6']['accepted_order_ids'].append('lost')
        else:
            bad['invariants']['charges'][0]['count'] = 2
        try:
            evaluate(bad)
        except Failure:
            print('PASS rejects ' + name)
        else:
            raise Failure('selftest accepted ' + name)
    print('RESULT: m6-chaos selftest passed (offline synthetic evidence only)')


def main():
    if sys.argv[1:] == ['--selftest']:
        selftest()
    elif len(sys.argv) == 3 and sys.argv[1] == '--evaluate':
        evaluate(json.loads(Path(sys.argv[2]).read_text()))
        print('PASS M6 evidence evaluation')
    else:
        context = subprocess.run(['kubectl', 'config', 'current-context'], capture_output=True, text=True, timeout=10)
        check(context.returncode == 0 and context.stdout.strip() == CONTEXT,
              'Refusing context: only kind-ticketrush is allowed; no context was switched')
        # Live helpers must share this Failure type when the CLI runs as __main__.
        sys.modules['chaos_runner'] = sys.modules[__name__]
        from chaos_live import run_live
        run_live()


if __name__ == '__main__':
    try:
        main()
    except (Failure, KeyError, TypeError, ValueError, OSError, subprocess.SubprocessError) as exc:
        print('FAIL m6-chaos: ' + str(exc), file=sys.stderr)
        sys.exit(1)
