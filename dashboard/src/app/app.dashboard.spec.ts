import { provideHttpClient } from '@angular/common/http';
import {
  HttpTestingController,
  provideHttpClientTesting,
  TestRequest,
} from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { App } from './app';
import { QUERIES, QueryId } from './metrics';

const STOCK_URL = '/api/events/concert-1/stock';
const ACTIVITY_URL = '/api/orders/activity';
const UUID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const FIRST_KEY = '11111111-1111-4111-8111-111111111111';
const SECOND_KEY = '22222222-2222-4222-8222-222222222222';
const THIRD_KEY = '33333333-3333-4333-8333-333333333333';
const FOURTH_KEY = '44444444-4444-4444-8444-444444444444';
const EMPTY_ACTIVITY = { windowSeconds: 10, ordersInWindow: 0, orders: [] };
const ACCEPTED = { orderId: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa' };

// DOM ids come from the approved Open Design reference. No component internals are accessed.
describe('M7 dashboard HTTP and user interactions', () => {
  let fixture: ComponentFixture<App>;
  let http: HttpTestingController;
  let root: HTMLElement;

  beforeEach(async () => {
    localStorage.clear();
    await TestBed.configureTestingModule({
      imports: [App],
      providers: [provideHttpClient(), provideHttpClientTesting()],
    }).compileComponents();
    vi.useFakeTimers();
    vi.setSystemTime(new Date('2026-11-09T12:00:00Z'));
    vi.spyOn(crypto, 'randomUUID')
      .mockReturnValueOnce(FIRST_KEY)
      .mockReturnValueOnce(SECOND_KEY)
      .mockReturnValueOnce(THIRD_KEY)
      .mockReturnValue(FOURTH_KEY);
    http = TestBed.inject(HttpTestingController);
    fixture = TestBed.createComponent(App);
    root = fixture.nativeElement;
    fixture.detectChanges();
  });

  afterEach(() => {
    fixture.destroy();
    discardMetrics();
    http.verify({ ignoreCancelled: true });
    vi.useRealTimers();
    vi.restoreAllMocks();
    localStorage.clear();
  });

  // The metrics store polls Prometheus from the start; these tests do not care unless they answer it.
  function discardMetrics(): TestRequest[] {
    return http.match((req) => req.url.startsWith('/api/metrics'));
  }

  function answerBackfill(
    data: Partial<Record<QueryId, [Record<string, string>, string, number?][]>>,
  ) {
    const nowS = Math.floor(Date.now() / 1000);
    for (const req of http.match((r) => r.url === '/api/metrics/query_range')) {
      const id = (Object.keys(QUERIES) as QueryId[]).find(
        (k) => QUERIES[k] === req.request.params.get('query'),
      )!;
      const result = (data[id] ?? []).map(([metric, value, age = 0]) => ({
        metric,
        values: [[nowS - age, value]],
      }));
      req.flush({ status: 'success', data: { resultType: 'matrix', result } });
    }
    fixture.detectChanges();
  }

  function buyButton(): HTMLButtonElement {
    const button = root.querySelector<HTMLButtonElement>('#buy-button');
    expect(button, 'The approved Buy form must expose its Buy/Retry button').toBeInstanceOf(
      HTMLButtonElement,
    );
    return button!;
  }

  function clickBuy(): TestRequest {
    buyButton().click();
    fixture.detectChanges();
    return http.expectOne({ method: 'POST', url: '/api/orders' });
  }

  function stockRead(): TestRequest {
    return http.expectOne({ method: 'GET', url: STOCK_URL });
  }

  function activityRead(): TestRequest {
    // The API defaults to 20. Both omission and explicit limit=20 are equivalent.
    const request = http.expectOne(
      (req) => req.method === 'GET' && req.url.split('?')[0] === ACTIVITY_URL,
    );
    const url = new URL(request.request.urlWithParams, 'http://dashboard.test');
    expect(url.searchParams.get('limit') ?? '20').toBe('20');
    expect(url.origin).toBe('http://dashboard.test');
    return request;
  }

  function initialSnapshot(): void {
    stockRead().flush({ eventId: 'concert-1', stock: 37 });
    activityRead().flush(EMPTY_ACTIVITY);
    fixture.detectChanges();
  }

  it('AC5 sends distinct UUID Idempotency-Keys for two purchases separated by 202', () => {
    const first = clickBuy();
    const firstKey = first.request.headers.get('Idempotency-Key');
    const customerId = first.request.body.customerId;
    expect(firstKey).toMatch(UUID_PATTERN);
    expect(customerId).toMatch(/^web-[0-9a-f-]{36}$/i);
    expect(first.request.body).toEqual({ eventId: 'concert-1', customerId, quantity: 1 });
    first.flush(ACCEPTED, { status: 202, statusText: 'Accepted' });
    fixture.detectChanges();

    const second = clickBuy();

    expect(second.request.headers.get('Idempotency-Key')).toMatch(UUID_PATTERN);
    expect(second.request.headers.get('Idempotency-Key')).not.toBe(firstKey);
    expect(second.request.body).toEqual({ eventId: 'concert-1', customerId, quantity: 1 });
    second.flush(ACCEPTED, { status: 202, statusText: 'Accepted' });
  });

  it.each([
    ['network', (request: TestRequest) => request.error(new ProgressEvent('error'))],
    [
      '500',
      (request: TestRequest) =>
        request.flush({}, { status: 500, statusText: 'Internal Server Error' }),
    ],
    [
      '503',
      (request: TestRequest) =>
        request.flush({}, { status: 503, statusText: 'Service Unavailable' }),
    ],
    [
      '429',
      (request: TestRequest) => request.flush({}, { status: 429, statusText: 'Too Many Requests' }),
    ],
  ] as const)('AC5 Retry preserves the failed purchase key after %s', (_label, fail) => {
    const first = clickBuy();
    const key = first.request.headers.get('Idempotency-Key');
    const body = first.request.body;
    expect(key).toMatch(UUID_PATTERN);
    fail(first);
    fixture.detectChanges();
    expect(buyButton().textContent?.trim()).toBe('Retry');
    expect(buyButton().disabled).toBe(false);

    const retry = clickBuy();

    expect(retry.request.headers.get('Idempotency-Key')).toBe(key);
    expect(retry.request.body).toEqual(body);
    retry.flush(ACCEPTED, { status: 202, statusText: 'Accepted' });
    fixture.detectChanges();
    expect(buyButton().textContent?.trim()).toBe('Buy');
    expect(buyButton().disabled).toBe(false);
  });

  it('AC5 disables Buy while its POST is pending and prevents a second submission', () => {
    const pending = clickBuy();

    expect(buyButton().disabled).toBe(true);
    buyButton().click();
    http.expectNone({ method: 'POST', url: '/api/orders' });
    pending.flush(ACCEPTED, { status: 202, statusText: 'Accepted' });
    fixture.detectChanges();
    expect(buyButton().disabled).toBe(false);
  });

  it('AC6 renders the exact stock returned by the same-origin stock API', () => {
    stockRead().flush({ eventId: 'concert-1', stock: 37 });
    activityRead().flush(EMPTY_ACTIVITY);

    fixture.detectChanges();

    expect(root.querySelector('#stock-value')?.textContent?.trim()).toBe('37');
  });

  it('labels the saga outcome counters as all events since order-service start', () => {
    stockRead().flush({ eventId: 'concert-1', stock: 37 });
    activityRead().flush(EMPTY_ACTIVITY);

    fixture.detectChanges();

    expect(root.querySelector('#outcomes-label')?.textContent?.trim()).toBe(
      'Saga outcomes, all events, since order-service start',
    );
  });

  it.each([
    { ordersInWindow: 32, windowSeconds: 10, expected: '3.2' },
    { ordersInWindow: 0, windowSeconds: 10, expected: '0.0' },
    { ordersInWindow: 7, windowSeconds: 4, expected: '1.8' },
  ])(
    'AC6 renders $ordersInWindow / $windowSeconds orders per second as $expected',
    ({ ordersInWindow, windowSeconds, expected }) => {
      stockRead().flush({ eventId: 'concert-1', stock: 37 });
      activityRead().flush({ windowSeconds, ordersInWindow, orders: [] });

      fixture.detectChanges();

      expect(root.querySelector('#rate-value')?.textContent?.trim()).toBe(expected);
    },
  );

  it('AC6 renders API saga rows in response order with real statuses and failure reason', () => {
    stockRead().flush({ eventId: 'concert-1', stock: 37 });
    activityRead().flush({
      windowSeconds: 10,
      ordersInWindow: 2,
      orders: [
        {
          orderId: 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb',
          eventId: 'concert-1',
          quantity: 2,
          status: 'FAILED',
          failureReason: 'Payment declined',
          amountCents: 10000,
          currency: 'EUR',
          createdAt: '2026-11-09T11:59:59Z',
          updatedAt: '2026-11-09T12:00:00Z',
        },
        {
          orderId: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
          eventId: 'concert-1',
          quantity: 1,
          status: 'RESERVED',
          failureReason: null,
          amountCents: 5000,
          currency: 'EUR',
          createdAt: '2026-11-09T11:59:58Z',
          updatedAt: '2026-11-09T11:59:59Z',
        },
      ],
    });

    fixture.detectChanges();

    const rows = root.querySelectorAll('tbody tr');
    expect(rows).toHaveLength(2);
    expect(rows[0].textContent).toContain('bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb');
    expect(rows[0].textContent).toContain('concert-1');
    expect(rows[0].textContent).toContain('FAILED');
    expect(rows[0].textContent).toContain('Payment declined');
    expect(rows[1].textContent).toContain('aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa');
    expect(rows[1].textContent).toContain('RESERVED');
    expect(root.textContent).not.toContain('PAYMENT_FAILED');
  });

  it('AC6 polls both read APIs at 2000 ms intervals, not earlier', () => {
    initialSnapshot();

    vi.advanceTimersByTime(1999);
    http.expectNone(STOCK_URL);
    http.expectNone((req) => req.url.startsWith(ACTIVITY_URL));
    vi.advanceTimersByTime(1);
    stockRead().flush({ eventId: 'concert-1', stock: 36 });
    activityRead().flush({ ...EMPTY_ACTIVITY, ordersInWindow: 11 });
    fixture.detectChanges();

    expect(root.querySelector('#stock-value')?.textContent?.trim()).toBe('36');
    expect(root.querySelector('#rate-value')?.textContent?.trim()).toBe('1.1');
    vi.advanceTimersByTime(2000);
    stockRead().flush({ eventId: 'concert-1', stock: 35 });
    activityRead().flush({ ...EMPTY_ACTIVITY, ordersInWindow: 12 });
    fixture.detectChanges();
    expect(root.querySelector('#stock-value')?.textContent?.trim()).toBe('35');
    expect(root.querySelector('#rate-value')?.textContent?.trim()).toBe('1.2');
  });

  it.each(['stock', 'activity'] as const)(
    'AC6 marks a failed %s poll stale and continues polling to recovery',
    (failedSource) => {
      initialSnapshot();
      expect(root.textContent).not.toMatch(/\bstale\b/i);
      vi.advanceTimersByTime(2000);
      const stock = stockRead();
      const activity = activityRead();
      const responses = {
        stock: () => {
          stock.flush({}, { status: 503, statusText: 'Service Unavailable' });
          activity.flush(EMPTY_ACTIVITY);
        },
        activity: () => {
          stock.flush({ eventId: 'concert-1', stock: 37 });
          activity.flush({}, { status: 503, statusText: 'Service Unavailable' });
        },
      };

      responses[failedSource]();
      fixture.detectChanges();

      expect(root.textContent).toMatch(/\bstale\b/i);
      expect(root.querySelector('#stock-value')?.textContent?.trim()).toBe('37');
      vi.advanceTimersByTime(2000);
      stockRead().flush({ eventId: 'concert-1', stock: 34 });
      activityRead().flush({ ...EMPTY_ACTIVITY, ordersInWindow: 15 });
      fixture.detectChanges();
      expect(root.querySelector('#stock-value')?.textContent?.trim()).toBe('34');
      expect(root.querySelector('#rate-value')?.textContent?.trim()).toBe('1.5');
      expect(root.textContent).not.toMatch(/\bstale\b/i);
    },
  );

  it('metrics failing from the start leaves stock, Buy and the orders table working', () => {
    for (const req of discardMetrics()) {
      if (!req.cancelled) req.flush('boom', { status: 503, statusText: 'Unavailable' });
    }
    stockRead().flush({ eventId: 'concert-1', stock: 37 });
    activityRead().flush({
      ...EMPTY_ACTIVITY,
      orders: [
        {
          orderId: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa',
          eventId: 'concert-1',
          quantity: 1,
          status: 'RESERVED',
          failureReason: null,
          amountCents: 5000,
          currency: 'EUR',
          createdAt: '2026-11-09T11:59:58Z',
          updatedAt: '2026-11-09T11:59:59Z',
        },
      ],
    });
    fixture.detectChanges();

    expect(root.querySelector('#stock-value')?.textContent?.trim()).toBe('37');
    expect(root.querySelectorAll('tbody tr')).toHaveLength(1);
    expect(buyButton().disabled).toBe(false);
    const chapters = Array.from(root.querySelectorAll('tr-chapter'));
    expect(chapters).toHaveLength(4);
    expect(chapters[0].textContent).not.toContain('Live metrics need the kind stack.');
    for (const chapter of chapters.slice(1)) {
      expect(chapter.textContent).toContain('Live metrics need the kind stack.');
      expect(chapter.querySelector('tr-chart')).toBeNull();
    }
  });

  it('NaN redis latency renders a dash, never "NaN"', () => {
    answerBackfill({ redisLatency: [[{}, 'NaN']] });
    expect(root.querySelector('#latency-value')?.textContent?.trim()).toBe('-');
    expect(root.textContent).not.toContain('NaN');
  });

  it('renders redis latency in milliseconds with one decimal', () => {
    answerBackfill({ redisLatency: [[{}, '0.0008']] });
    expect(root.querySelector('#latency-value')?.textContent?.trim()).toBe('0.8 ms');
  });

  it('stock history resets when the event id changes', () => {
    const chart = () => root.querySelector('tr-chapter tr-chart')!.textContent;
    stockRead().flush({ eventId: 'concert-1', stock: 37 });
    activityRead().flush(EMPTY_ACTIVITY);
    vi.advanceTimersByTime(2000);
    stockRead().flush({ eventId: 'concert-1', stock: 36 });
    activityRead().flush(EMPTY_ACTIVITY);
    fixture.detectChanges();
    expect(chart()).not.toContain('No data yet');

    const input = root.querySelector<HTMLInputElement>('#event-id')!;
    input.value = 'other-event';
    input.dispatchEvent(new Event('input', { bubbles: true }));
    http.expectOne('/api/events/other-event/stock').flush({ eventId: 'other-event', stock: 8 });
    fixture.detectChanges();

    expect(chart()).toContain('No data yet');
  });

  it('renders ready pods as "1 of 2" for payment-service and "2 of 2" for inventory-service', () => {
    answerBackfill({
      readyPods: [
        [{ job: 'inventory-service' }, '2'],
        [{ job: 'payment-service' }, '1'],
      ],
    });
    const text = root.querySelectorAll('tr-chapter')[3].textContent!.replace(/\s+/g, ' ');
    expect(text).toContain('inventory-service 2 of 2');
    expect(text).toContain('payment-service 1 of 2');
  });

  it('renders a pod series that stopped reporting as 0 of 2, not its old count', () => {
    answerBackfill({
      readyPods: [
        [{ job: 'inventory-service' }, '2'],
        [{ job: 'payment-service' }, '2', 60],
      ],
    });
    const text = root.querySelectorAll('tr-chapter')[3].textContent!.replace(/\s+/g, ' ');
    expect(text).toContain('inventory-service 2 of 2');
    expect(text).toContain('payment-service 0 of 2');
  });

  it('total lag sums both consumer groups', () => {
    answerBackfill({
      lagByGroup: [
        [{ consumergroup: 'inventory-service', topic: 'orders.requested' }, '7'],
        [{ consumergroup: 'payment-service', topic: 'tickets.reserved' }, '5'],
      ],
    });
    expect(root.querySelector('#lag-value')?.textContent?.trim()).toBe('12');
  });

  it('chapters 02-04 say there is no load when the order rate was 0 for the whole window', () => {
    answerBackfill({ ordersRate: [[{}, '0']] });
    for (const chapter of Array.from(root.querySelectorAll('tr-chapter')).slice(1)) {
      expect(chapter.textContent).toContain(
        'No orders in the last 5 minutes. Start load: scripts/autoscale-acceptance.sh',
      );
    }
  });

  it('an unreadable order rate does not claim there was no load', () => {
    answerBackfill({ ordersRate: [[{}, 'NaN']] });
    expect(root.textContent).not.toContain('No orders in the last 5 minutes');
  });

  it('failed stock and activity reads keep the figures to a dash and name Unavailable beside them', () => {
    stockRead().flush({}, { status: 503, statusText: 'Service Unavailable' });
    activityRead().flush({}, { status: 503, statusText: 'Service Unavailable' });
    fixture.detectChanges();
    expect(root.querySelector('#stock-value')?.textContent?.trim()).toBe('-');
    expect(root.querySelector('#rate-value')?.textContent?.trim()).toBe('-');
    expect(root.querySelector('tr-chapter')?.textContent).toContain('Unavailable');
  });

  it('the Lua quote ends the rejection branch before the decrement', () => {
    answerBackfill({});
    const lines = root.querySelector('pre.lua')!.textContent!.split('\n');
    expect(lines.indexOf('end')).toBeGreaterThan(-1);
    expect(lines.indexOf('end')).toBeLessThan(lines.findIndex((l) => l.includes('HINCRBY')));
    expect(lines).toContain('  return {0}');
  });
});
