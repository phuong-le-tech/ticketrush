import { HttpClient } from '@angular/common/http';
import { DestroyRef, Injectable, Signal, inject, signal } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import {
  Observable,
  catchError,
  concat,
  exhaustMap,
  forkJoin,
  interval,
  map,
  of,
  tap,
  timeout,
} from 'rxjs';

export type SourceState = 'loading' | 'live' | 'stale' | 'unavailable';
export interface Sample {
  t: number; // ms
  v: number | null; // null when NaN or absent
}
export interface Series {
  labels: Record<string, string>;
  points: Sample[];
}
export type QueryId =
  | 'ordersRate'
  | 'sagaOutcomes'
  | 'lagByGroup'
  | 'partitionLag'
  | 'redisOps'
  | 'redisLatency'
  | 'readyPods'
  | 'consumerRate';

const GROUPS =
  'consumergroup=~"inventory-service|payment-service",topic=~"orders.requested|tickets.reserved"';

export const QUERIES: Record<QueryId, string> = {
  ordersRate: 'sum(rate(ticketrush_orders_total[30s]))',
  sagaOutcomes: 'sum by (outcome) (ticketrush_saga_outcomes_total)',
  lagByGroup: `sum by (consumergroup, topic) (kafka_consumergroup_lag_sum{${GROUPS}})`,
  partitionLag: `kafka_consumergroup_lag{${GROUPS}}`,
  redisOps:
    'sum by (db_operation) (rate(lettuce_seconds_count{db_operation=~"EVALSHA|HGET"}[30s]))',
  redisLatency:
    'sum(rate(lettuce_seconds_sum{db_operation="EVALSHA"}[30s])) / sum(rate(lettuce_seconds_count{db_operation="EVALSHA"}[30s]))',
  readyPods: 'count by (job) (up{job=~"inventory-service|payment-service"} == 1)',
  consumerRate:
    'sum by (consumer, topic) (rate(ticketrush_consumer_processing_seconds_count[30s]))',
};

const IDS = Object.keys(QUERIES) as QueryId[];
const POLL_INTERVAL_MS = 5000;
const READ_TIMEOUT_MS = 8000;
const BACKFILL_SECONDS = 300;
const STEP_SECONDS = 5;
const MAX_POINTS = 60;

type Pair = [number, string];
interface PromResult {
  metric?: Record<string, string>;
  value?: Pair;
  values?: Pair[];
}

function isSuccess(body: unknown): boolean {
  return (
    typeof body === 'object' && body !== null && (body as { status?: string }).status === 'success'
  );
}

function results(body: unknown): PromResult[] {
  const result = isSuccess(body) ? (body as { data?: { result?: unknown } }).data?.result : null;
  return Array.isArray(result) ? (result as PromResult[]) : [];
}

function sample([seconds, raw]: Pair): Sample {
  const v = Number(raw);
  return { t: Math.round(seconds * 1000), v: Number.isFinite(v) ? v : null };
}

export function parseVector(body: unknown): Series[] {
  return results(body)
    .filter((r) => Array.isArray(r.value))
    .map((r) => ({ labels: r.metric ?? {}, points: [sample(r.value!)] }));
}

export function parseMatrix(body: unknown): Series[] {
  return results(body).map((r) => ({
    labels: r.metric ?? {},
    points: (Array.isArray(r.values) ? r.values : []).map(sample),
  }));
}

function labelKey(labels: Record<string, string>): string {
  return JSON.stringify(Object.entries(labels).sort());
}

@Injectable({ providedIn: 'root' })
export class MetricsStore {
  private readonly http = inject(HttpClient);
  private readonly stateSignal = signal<SourceState>('loading');
  private readonly staleSinceSignal = signal<Date | null>(null);
  private readonly data = Object.fromEntries(IDS.map((id) => [id, signal<Series[]>([])])) as Record<
    QueryId,
    ReturnType<typeof signal<Series[]>>
  >;
  private everSucceeded = false;

  readonly state: Signal<SourceState> = this.stateSignal.asReadonly();
  readonly staleSince: Signal<Date | null> = this.staleSinceSignal.asReadonly();

  series(id: QueryId): Signal<Series[]> {
    return this.data[id].asReadonly();
  }

  start(destroyRef: DestroyRef): void {
    const end = Math.floor(Date.now() / 1000);
    const backfill = this.round(
      'query_range',
      parseMatrix,
      { start: end - BACKFILL_SECONDS, end, step: STEP_SECONDS },
      (id, parsed) =>
        this.data[id].set(parsed.map((s) => ({ ...s, points: s.points.slice(-MAX_POINTS) }))),
    );
    const poll = interval(POLL_INTERVAL_MS).pipe(
      exhaustMap(() =>
        this.round('query', parseVector, {}, (id, parsed) => this.append(id, parsed)),
      ),
    );
    concat(backfill, poll).pipe(takeUntilDestroyed(destroyRef)).subscribe();
  }

  /** One round = all eight queries; it succeeds only if every one returns HTTP 200 and status "success". */
  private round(
    path: 'query' | 'query_range',
    parse: (body: unknown) => Series[],
    extra: Record<string, number>,
    apply: (id: QueryId, parsed: Series[]) => void,
  ): Observable<void> {
    const calls = IDS.map((id) =>
      this.http
        .get<unknown>(`/api/metrics/${path}`, { params: { query: QUERIES[id], ...extra } })
        .pipe(
          timeout(READ_TIMEOUT_MS),
          tap((body) => {
            if (!isSuccess(body)) throw new Error('Prometheus status is not success');
          }),
          map((body) => parse(body)),
        ),
    );
    return forkJoin(calls).pipe(
      tap((all) => {
        all.forEach((parsed, i) => apply(IDS[i], parsed));
        this.everSucceeded = true;
        this.stateSignal.set('live');
        this.staleSinceSignal.set(null);
      }),
      catchError(() => {
        if (!this.everSucceeded) this.stateSignal.set('unavailable');
        else {
          this.stateSignal.set('stale');
          this.staleSinceSignal.update((since) => since ?? new Date());
        }
        return of(undefined);
      }),
      map(() => undefined),
    );
  }

  private append(id: QueryId, incoming: Series[]): void {
    this.data[id].update((current) => {
      const next = current.map((s) => ({ ...s }));
      for (const s of incoming) {
        const key = labelKey(s.labels);
        const found = next.find((c) => labelKey(c.labels) === key);
        if (found) found.points = [...found.points, ...s.points].slice(-MAX_POINTS);
        else next.push(s);
      }
      return next;
    });
  }
}
