import { DestroyRef } from '@angular/core';
import { provideHttpClient } from '@angular/common/http';
import {
  HttpTestingController,
  TestRequest,
  provideHttpClientTesting,
} from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { MetricsStore, QUERIES, QueryId, current, parseMatrix, parseVector } from './metrics';

const IDS = Object.keys(QUERIES) as QueryId[];
const T0 = 1794400000;

function vector(value: string, metric: Record<string, string> = {}, t = T0) {
  return {
    status: 'success',
    data: { resultType: 'vector', result: [{ metric, value: [t, value] }] },
  };
}
function matrix(values: [number, string][], metric: Record<string, string> = {}) {
  return { status: 'success', data: { resultType: 'matrix', result: [{ metric, values }] } };
}

describe('parsing', () => {
  it('parseVector maps NaN and missing values to null', () => {
    const body = {
      status: 'success',
      data: {
        resultType: 'vector',
        result: [
          { metric: { job: 'payment-service' }, value: [1794400000, 'NaN'] },
          { metric: { job: 'inventory-service' }, value: [1794400000, '2'] },
        ],
      },
    };
    expect(parseVector(body)).toEqual([
      { labels: { job: 'payment-service' }, points: [{ t: 1794400000000, v: null }] },
      { labels: { job: 'inventory-service' }, points: [{ t: 1794400000000, v: 2 }] },
    ]);
  });

  it('parseVector and parseMatrix return [] for an error body, an empty result or a non-object', () => {
    for (const body of [
      null,
      'x',
      { status: 'error' },
      { status: 'success', data: { result: [] } },
    ]) {
      expect(parseVector(body)).toEqual([]);
      expect(parseMatrix(body)).toEqual([]);
    }
  });

  it('parseMatrix converts fractional seconds to ms', () => {
    expect(parseMatrix(matrix([[T0 + 0.5, '1.5']]))).toEqual([
      { labels: {}, points: [{ t: T0 * 1000 + 500, v: 1.5 }] },
    ]);
  });
});

describe('MetricsStore', () => {
  let http: HttpTestingController;
  let store: MetricsStore;
  let destroyed: () => void;

  beforeEach(() => {
    vi.useFakeTimers();
    TestBed.configureTestingModule({
      providers: [provideHttpClient(), provideHttpClientTesting()],
    });
    http = TestBed.inject(HttpTestingController);
    store = TestBed.inject(MetricsStore);
    const ref = {
      onDestroy: (cb: () => void) => ((destroyed = cb), () => undefined),
    } as unknown as DestroyRef;
    store.start(ref);
  });

  afterEach(() => {
    destroyed();
    http.verify({ ignoreCancelled: true });
    vi.useRealTimers();
  });

  const route = (path: string) => http.match((r) => r.url === path);
  // forkJoin cancels siblings of a failed call, so only flush live requests.
  const flush = (r: TestRequest, body: unknown, opts?: object) => {
    if (!r.cancelled) r.flush(body as never, opts);
  };
  const answerBackfill = (ok = true) => {
    const reqs = route('/api/metrics/query_range');
    expect(reqs).toHaveLength(8);
    reqs.forEach((r) =>
      ok
        ? r.flush(matrix([[T0, '1']], { id: r.request.params.get('query')! }))
        : flush(r, 'boom', { status: 503, statusText: 'x' }),
    );
  };
  const answerPoll = (ok = true, value = '5') => {
    const reqs = route('/api/metrics/query');
    expect(reqs).toHaveLength(8);
    reqs.forEach((r) =>
      ok ? r.flush(vector(value)) : flush(r, 'boom', { status: 503, statusText: 'x' }),
    );
  };
  const tick = (ms = 5000) => vi.advanceTimersByTimeAsync(ms);

  it('is loading until the first round settles', () => {
    expect(store.state()).toBe('loading');
    answerBackfill();
    expect(store.state()).toBe('live');
  });

  it('is unavailable when the first round fails, and never becomes stale without a prior success', async () => {
    answerBackfill(false);
    expect(store.state()).toBe('unavailable');
    await tick();
    answerPoll(false);
    expect(store.state()).toBe('unavailable');
    expect(store.staleSince()).toBeNull();
    await tick();
    answerPoll(true);
    expect(store.state()).toBe('live');
  });

  it('treats an empty result as success, but an error status body as failure', async () => {
    route('/api/metrics/query_range').forEach((r) =>
      r.flush({ status: 'success', data: { resultType: 'matrix', result: [] } }),
    );
    expect(store.state()).toBe('live');
    await tick();
    route('/api/metrics/query').forEach((r, i) =>
      flush(r, i === 0 ? { status: 'error', error: 'bad' } : vector('1')),
    );
    expect(store.state()).toBe('stale');
  });

  it('is live after a successful round, stale with staleSince after a later failure, live again on recovery', async () => {
    answerBackfill();
    expect(store.state()).toBe('live');
    await tick();
    answerPoll(false);
    expect(store.state()).toBe('stale');
    const since = store.staleSince();
    expect(since).toBeInstanceOf(Date);
    await tick();
    answerPoll(false);
    expect(store.staleSince()).toBe(since);
    await tick();
    answerPoll(true);
    expect(store.state()).toBe('live');
    expect(store.staleSince()).toBeNull();
  });

  it('keeps the last good series while stale', async () => {
    answerBackfill();
    const before = store.series('ordersRate')();
    expect(before.length).toBeGreaterThan(0);
    await tick();
    answerPoll(false);
    expect(store.series('ordersRate')()).toEqual(before);
  });

  it('returns the same signal for the same id', () => {
    expect(store.series('readyPods')).toBe(store.series('readyPods'));
  });

  it('polls every 5000 ms and does not start a second round while one is in flight', async () => {
    answerBackfill();
    expect(route('/api/metrics/query')).toHaveLength(0);
    await tick(4999);
    expect(route('/api/metrics/query')).toHaveLength(0);
    await tick(1);
    const first = route('/api/metrics/query');
    expect(first).toHaveLength(8);
    await tick(5000);
    expect(route('/api/metrics/query')).toHaveLength(0);
    first.forEach((r) => r.flush(vector('1')));
    await tick(5000);
    expect(route('/api/metrics/query')).toHaveLength(8);
  });

  it('caps each series at 60 points, dropping the oldest', async () => {
    const values: [number, string][] = Array.from({ length: 61 }, (_, i) => [
      T0 + i * 5,
      String(i),
    ]);
    route('/api/metrics/query_range').forEach((r) => r.flush(matrix(values)));
    const backfilled = store.series('ordersRate')()[0].points;
    expect(backfilled).toHaveLength(60);
    expect(backfilled[0].v).toBe(1);
    await tick();
    route('/api/metrics/query').forEach((r) => r.flush(vector('99', {}, T0 + 400)));
    const points = store.series('ordersRate')()[0].points;
    expect(points).toHaveLength(60);
    expect(points[0].v).toBe(2);
    expect(points[59]).toEqual({ t: (T0 + 400) * 1000, v: 99 });
  });

  it('keeps history of a series absent from a poll and matches series by full label set', async () => {
    route('/api/metrics/query_range').forEach((r) =>
      r.flush({
        status: 'success',
        data: {
          resultType: 'matrix',
          result: [
            { metric: { a: '1' }, values: [[T0, '1']] },
            { metric: { a: '2' }, values: [[T0, '1']] },
          ],
        },
      }),
    );
    await tick();
    route('/api/metrics/query').forEach((r) => r.flush(vector('7', { a: '2' }, T0 + 5)));
    const byA = Object.fromEntries(
      store
        .series('ordersRate')()
        .map((s) => [s.labels['a'], s]),
    );
    expect(byA['1'].points).toHaveLength(1);
    expect(byA['2'].points.map((p) => p.v)).toEqual([1, 7]);
  });

  it('sends the exact QUERIES strings url-encoded to /api/metrics/query', async () => {
    answerBackfill();
    await tick();
    const reqs = route('/api/metrics/query');
    expect(reqs.map((r) => r.request.params.get('query')).sort()).toEqual(
      IDS.map((id) => QUERIES[id]).sort(),
    );
    for (const r of reqs) {
      const raw = r.request.urlWithParams.split('query=')[1];
      expect(raw).not.toContain('|');
      expect(decodeURIComponent(raw)).toBe(r.request.params.get('query'));
    }
    reqs.forEach((r) => r.flush(vector('1')));
  });

  it('backfills query_range over 300 s with step 5', () => {
    const reqs = route('/api/metrics/query_range');
    for (const r of reqs) {
      const end = Number(r.request.params.get('end'));
      expect(end - Number(r.request.params.get('start'))).toBe(300);
      expect(r.request.params.get('step')).toBe('5');
    }
    reqs.forEach((r) => r.flush(matrix([])));
  });
});

describe('current', () => {
  const T = 1_000_000;
  it('a pod series whose newest point is older than 15s renders as absent, not as its old count', () => {
    const pods = [
      { labels: { job: 'inventory-service' }, points: [{ t: T, v: 2 }] },
      { labels: { job: 'payment-service' }, points: [{ t: T - 15001, v: 1 }] },
    ];
    expect(current(pods)).toEqual([{ labels: { job: 'inventory-service' }, v: 2 }]);
  });

  it('keeps a series exactly 15s behind the newest and passes null through', () => {
    const all = [
      { labels: { a: '1' }, points: [{ t: T, v: null }] },
      { labels: { a: '2' }, points: [{ t: T - 15000, v: 3 }] },
    ];
    expect(current(all).map((c) => c.v)).toEqual([null, 3]);
  });

  it('is empty for no data', () => {
    expect(current([])).toEqual([]);
  });
});
