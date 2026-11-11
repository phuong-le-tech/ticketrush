import { DatePipe } from '@angular/common';
import { HttpClient, HttpErrorResponse } from '@angular/common/http';
import {
  Component,
  DestroyRef,
  OnInit,
  computed,
  effect,
  inject,
  signal,
  untracked,
} from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import {
  BehaviorSubject,
  EMPTY,
  catchError,
  exhaustMap,
  interval,
  startWith,
  switchMap,
  timeout,
} from 'rxjs';
import { Chapter, current } from './chapter';
import { TimeSeriesChart } from './chart';
import { MetricsStore, Sample } from './metrics';

const POLL_INTERVAL_MS = 2000;
const READ_TIMEOUT_MS = 8000;
const PURCHASE_TIMEOUT_MS = 15000;
const CUSTOMER_STORAGE_KEY = 'ticketrush.customerId';
const MAX_EVENT_ID_LENGTH = 255;
const MIN_QUANTITY = 1;
const MAX_QUANTITY = 4;
const DASH = '-';
const STOCK_HISTORY_MAX = 150; // 5 minutes of 2s polls
const FLASH_MS = 600;
const PARTITIONS = 12;
const TOPICS = ['orders.requested', 'tickets.reserved'];
const SERVICES = ['inventory-service', 'payment-service'];
// Mirror autoscaling.maxReplicas and autoscaling.lagThreshold in deploy/helm/ticketrush/values.yaml.
const MAX_REPLICAS = 2;
const LAG_THRESHOLD = 10;
// Lines 21-23 and 29 of inventory-service/src/main/resources/reserve.lua, unchanged.
const LUA_SNIPPET = [
  "local stock = tonumber(redis.call('HGET', KEYS[1], 'stock'))",
  'local quantity = tonumber(ARGV[1])',
  'if stock == nil or quantity == nil or quantity <= 0 or stock < quantity then',
  "redis.call('HINCRBY', KEYS[1], 'stock', -quantity)",
].join('\n');

interface RecentOrder {
  orderId: string;
  eventId: string;
  quantity: number;
  status: string;
  failureReason: string | null;
  amountCents: number | null;
  currency: string | null;
  createdAt: string;
  updatedAt: string;
}

interface Activity {
  windowSeconds: number;
  ordersInWindow: number;
  orders: RecentOrder[];
}

interface PurchaseAttempt {
  key: string;
  body: { eventId: string; customerId: string; quantity: number };
}

function newUuid(): string {
  if (typeof crypto.randomUUID === 'function') return crypto.randomUUID();
  const bytes = crypto.getRandomValues(new Uint8Array(16));
  bytes[6] = (bytes[6] & 0x0f) | 0x40;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const hex = Array.from(bytes, (byte) => byte.toString(16).padStart(2, '0')).join('');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function customerIdentity(): string {
  const stored = localStorage.getItem(CUSTOMER_STORAGE_KEY);
  if (
    stored &&
    /^web-[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(stored)
  ) {
    return stored;
  }
  const customerId = `web-${newUuid()}`;
  localStorage.setItem(CUSTOMER_STORAGE_KEY, customerId);
  return customerId;
}

@Component({
  imports: [DatePipe, Chapter, TimeSeriesChart],
  selector: 'app-root',
  styleUrl: './app.css',
  templateUrl: './app.html',
})
export class App implements OnInit {
  private readonly http = inject(HttpClient);
  private readonly destroyRef = inject(DestroyRef);
  private readonly metrics = inject(MetricsStore);
  private readonly stockEvents = new BehaviorSubject('concert-1');
  protected readonly eventId = signal('concert-1');
  protected readonly quantity = signal(1);
  protected readonly stock = signal<number | null>(null);
  protected readonly activity = signal<Activity | null>(null);
  protected readonly stockStale = signal(false);
  protected readonly activityStale = signal(false);
  protected readonly stale = computed(() => this.stockStale() || this.activityStale());
  protected readonly staleSince = signal<Date | null>(null);
  protected readonly stockHistory = signal<Sample[]>([]);
  protected readonly metricsState = this.metrics.state;
  protected readonly metricsStaleSince = this.metrics.staleSince;
  protected readonly rate = computed(() => {
    const activity = this.activity();
    return activity ? (activity.ordersInWindow / activity.windowSeconds).toFixed(1) : DASH;
  });
  protected readonly stockSeries = computed(() => [
    { name: 'stock', emphasis: true, points: this.stockHistory() },
  ]);
  protected readonly stockFloor = computed(() => this.stockHistory()[0]?.v ?? 1);

  private outcomeCount(name: string): string {
    const v = current(this.metrics.series('sagaOutcomes')()).find(
      (c) => c.labels['outcome']?.toUpperCase() === name,
    )?.v;
    return v == null ? DASH : String(Math.round(v));
  }
  protected readonly outcomes = computed(() => ({
    confirmed: this.outcomeCount('CONFIRMED'),
    rejected: this.outcomeCount('REJECTED'),
    failed: this.outcomeCount('FAILED'),
  }));

  private readonly lagAll = computed(() =>
    this.metrics
      .series('lagByGroup')()
      .map((s) => ({
        name: `${s.labels['consumergroup']} / ${s.labels['topic']}`,
        emphasis: s.labels['consumergroup'] === 'inventory-service',
        points: s.points,
      })),
  );
  protected readonly lagSeries = this.lagAll;
  protected readonly inventoryLag = computed(() => this.lagAll().filter((s) => s.emphasis));
  protected readonly totalLag = computed(() => {
    const values = current(this.metrics.series('lagByGroup')())
      .map((c) => c.v)
      .filter((v): v is number => v !== null);
    return values.length ? String(Math.round(values.reduce((a, b) => a + b, 0))) : DASH;
  });
  protected readonly partitions = computed(() => {
    const fresh = current(this.metrics.series('partitionLag')());
    return TOPICS.map((topic) => ({
      topic,
      squares: Array.from({ length: PARTITIONS }, (_, partition) => {
        const lags = fresh
          .filter((c) => c.labels['topic'] === topic && Number(c.labels['partition']) === partition)
          .map((c) => c.v ?? 0);
        const lag = lags.length ? Math.max(...lags) : 0;
        return { partition, lag, opacity: 0.3 + 0.7 * Math.min(lag / LAG_THRESHOLD, 1) };
      }),
    }));
  });

  protected readonly redisSeries = computed(() =>
    this.metrics
      .series('redisOps')()
      .map((s) => ({
        name: s.labels['db_operation'],
        emphasis: s.labels['db_operation'] === 'EVALSHA',
        points: s.points,
      })),
  );
  protected readonly latency = computed(() => {
    const v = current(this.metrics.series('redisLatency')())[0]?.v ?? null;
    return v === null ? DASH : `${(v * 1000).toFixed(1)} ms`;
  });

  protected readonly pods = computed(() => {
    const fresh = current(this.metrics.series('readyPods')());
    return SERVICES.map((job) => {
      const n = Math.round(fresh.find((c) => c.labels['job'] === job)?.v ?? 0);
      return { job, n, squares: Array.from({ length: MAX_REPLICAS }, (_, i) => i < n) };
    });
  });
  protected readonly luaSnippet = LUA_SNIPPET;
  protected readonly maxReplicas = MAX_REPLICAS;
  protected readonly lagThreshold = LAG_THRESHOLD;
  protected readonly idle = computed(() => {
    const points = this.metrics
      .series('ordersRate')()
      .flatMap((s) => s.points);
    return points.length > 0 && points.every((p) => !p.v);
  });

  // A figure that changed since the previous poll is flagged for 600ms (accent colour, no counting).
  private readonly flashing = signal<ReadonlySet<string>>(new Set());
  private shown: Record<string, string> = {};
  protected flash(key: string): boolean {
    return this.flashing().has(key);
  }
  private readonly watched = computed<Record<string, string>>(() => ({
    stock: this.stock()?.toString() ?? DASH,
    rate: this.rate(),
    ...this.outcomes(),
    lag: this.totalLag(),
    latency: this.latency(),
    ...Object.fromEntries(this.pods().map((p) => [`pod:${p.job}`, String(p.n)])),
  }));
  protected readonly pending = signal(false);
  protected readonly attempt = signal<PurchaseAttempt | null>(null);
  protected readonly buyMessage = signal(
    '1 to 4 tickets per order. Acceptance is not confirmation.',
  );
  protected readonly buyError = signal(false);
  protected readonly validInput = computed(() => {
    const id = this.eventId().trim();
    return (
      id.length > 0 &&
      id.length <= MAX_EVENT_ID_LENGTH &&
      Number.isInteger(this.quantity()) &&
      this.quantity() >= MIN_QUANTITY &&
      this.quantity() <= MAX_QUANTITY
    );
  });

  constructor() {
    effect(() => {
      const now = this.watched();
      untracked(() => {
        const keys = Object.keys(now).filter(
          (k) =>
            this.shown[k] !== undefined &&
            this.shown[k] !== DASH &&
            now[k] !== DASH &&
            this.shown[k] !== now[k],
        );
        // A pod square that appeared gets the outline, a pod that left does not.
        const grown = keys.filter(
          (k) => k.startsWith('pod:') && Number(now[k]) > Number(this.shown[k]),
        );
        this.shown = now;
        if (!keys.length) return;
        const marks = [...keys, ...grown.map((k) => `new:${k}`)];
        this.flashing.update((set) => new Set([...set, ...marks]));
        const timer = setTimeout(
          () => this.flashing.update((set) => new Set([...set].filter((k) => !marks.includes(k)))),
          FLASH_MS,
        );
        this.destroyRef.onDestroy(() => clearTimeout(timer));
      });
    });
  }

  ngOnInit(): void {
    this.metrics.start(this.destroyRef);
    this.stockEvents
      .pipe(
        switchMap((eventId) =>
          eventId
            ? interval(POLL_INTERVAL_MS).pipe(
                startWith(0),
                exhaustMap(() =>
                  this.http
                    .get<{ eventId: string; stock: number }>(
                      `/api/events/${encodeURIComponent(eventId)}/stock`,
                    )
                    .pipe(
                      timeout(READ_TIMEOUT_MS),
                      catchError(() => {
                        this.stockStale.set(true);
                        this.staleSince.update((since) => since ?? new Date());
                        return EMPTY;
                      }),
                    ),
                ),
              )
            : EMPTY,
        ),
        takeUntilDestroyed(this.destroyRef),
      )
      .subscribe((response) => {
        this.stock.set(response.stock);
        this.stockStale.set(false);
        if (!this.stale()) this.staleSince.set(null);
        this.stockHistory.update((h) =>
          [...h, { t: Date.now(), v: response.stock }].slice(-STOCK_HISTORY_MAX),
        );
      });

    interval(POLL_INTERVAL_MS)
      .pipe(
        startWith(0),
        exhaustMap(() =>
          this.http.get<Activity>('/api/orders/activity').pipe(
            timeout(READ_TIMEOUT_MS),
            catchError(() => {
              this.activityStale.set(true);
              this.staleSince.update((since) => since ?? new Date());
              return EMPTY;
            }),
          ),
        ),
        takeUntilDestroyed(this.destroyRef),
      )
      .subscribe((activity) => {
        this.activity.set(activity);
        this.activityStale.set(false);
        if (!this.stale()) this.staleSince.set(null);
      });
  }

  protected changeEvent(value: string): void {
    this.eventId.set(value);
    this.stock.set(null);
    this.stockStale.set(false);
    if (!this.stale()) this.staleSince.set(null);
    this.stockHistory.set([]);
    const id = value.trim();
    this.stockEvents.next(id && id.length <= MAX_EVENT_ID_LENGTH ? id : '');
  }

  protected changeQuantity(value: string): void {
    this.quantity.set(Number(value));
  }

  protected buy(event: Event): void {
    event.preventDefault();
    if (this.pending() || (!this.attempt() && !this.validInput())) return;
    if (!this.attempt()) {
      try {
        const customerId = customerIdentity();
        this.attempt.set({
          key: newUuid(),
          body: {
            eventId: this.eventId().trim(),
            customerId,
            quantity: this.quantity(),
          },
        });
      } catch {
        this.buyError.set(true);
        this.buyMessage.set(
          'Cannot safely create a purchase. Enable local storage and browser cryptography, then try again.',
        );
        return;
      }
    }
    const attempt = this.attempt()!;
    this.pending.set(true);
    this.buyError.set(false);
    this.buyMessage.set('Submitting purchase. Waiting for acceptance.');
    this.http
      .post<{ orderId: string }>('/api/orders', attempt.body, {
        headers: { 'Idempotency-Key': attempt.key },
        observe: 'response',
      })
      .pipe(timeout(PURCHASE_TIMEOUT_MS), takeUntilDestroyed(this.destroyRef))
      .subscribe({
        next: (response) => {
          this.pending.set(false);
          if (response.status === 202) {
            this.attempt.set(null);
            this.buyMessage.set(
              `Order ${response.body?.orderId ?? ''} accepted. Follow its saga state below; it is not yet confirmed.`,
            );
          } else {
            this.buyError.set(true);
            this.buyMessage.set('Unexpected response. Retry keeps the original purchase and key.');
          }
        },
        error: (error: unknown) => {
          this.pending.set(false);
          this.buyError.set(true);
          if (
            error instanceof HttpErrorResponse &&
            error.status >= 400 &&
            error.status < 500 &&
            error.status !== 429
          ) {
            this.attempt.set(null);
            this.buyMessage.set(
              `Purchase rejected (HTTP ${error.status}). Check the event and quantity before buying again.`,
            );
          } else {
            // A lost reply may hide an accepted purchase, so never replace this attempt on retry.
            this.buyMessage.set(
              'Purchase not acknowledged. Retry reuses the original purchase and key.',
            );
          }
        },
      });
  }

  protected amount(order: RecentOrder): string {
    if (order.amountCents === null || !order.currency) return 'Not available';
    // Keep cents exact rather than round Java long values through floating-point currency formatting.
    if (!Number.isSafeInteger(order.amountCents)) return 'Amount outside browser precision';
    return `${order.currency} ${BigInt(order.amountCents) / 100n}.${(BigInt(order.amountCents) % 100n).toString().padStart(2, '0')}`;
  }
}
