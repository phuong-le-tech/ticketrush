import { provideHttpClient } from '@angular/common/http';
import { HttpTestingController, provideHttpClientTesting } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { App } from './app';

describe('M7 purchase and polling boundaries', () => {
  let fixture: ComponentFixture<App>;
  let http: HttpTestingController;
  let root: HTMLElement;

  beforeEach(() => {
    localStorage.clear();
    TestBed.configureTestingModule({
      imports: [App],
      providers: [provideHttpClient(), provideHttpClientTesting()],
    });
    vi.useFakeTimers();
    http = TestBed.inject(HttpTestingController);
    fixture = TestBed.createComponent(App);
    root = fixture.nativeElement;
    fixture.detectChanges();
    flushReads();
  });

  afterEach(() => {
    fixture.destroy();
    http.match((req) => req.url.startsWith('/api/metrics'));
    http.verify({ ignoreCancelled: true });
    vi.useRealTimers();
    vi.restoreAllMocks();
    localStorage.clear();
  });

  function flushReads() {
    http.expectOne('/api/events/concert-1/stock').flush({ eventId: 'concert-1', stock: 37 });
    http
      .expectOne('/api/orders/activity')
      .flush({ windowSeconds: 10, ordersInWindow: 0, orders: [] });
    fixture.detectChanges();
  }

  function buy() {
    root.querySelector<HTMLButtonElement>('#buy-button')!.click();
    fixture.detectChanges();
    return http.expectOne('/api/orders');
  }

  function input(id: string, value: string) {
    const element = root.querySelector<HTMLInputElement>(id)!;
    element.value = value;
    element.dispatchEvent(new Event('input', { bubbles: true }));
    fixture.detectChanges();
  }

  it('AC5 keeps the customer identity across component reloads', () => {
    const first = buy();
    const customerId = first.request.body.customerId;
    first.flush({ orderId: 'first' }, { status: 202, statusText: 'Accepted' });
    fixture.destroy();
    fixture = TestBed.createComponent(App);
    root = fixture.nativeElement;
    fixture.detectChanges();
    flushReads();
    const next = buy();
    expect(next.request.body.customerId).toBe(customerId);
    next.flush({ orderId: 'next' }, { status: 202, statusText: 'Accepted' });
  });

  it('AC5 generates v4 customer and purchase UUIDs without randomUUID', () => {
    vi.spyOn(crypto, 'randomUUID');
    Object.defineProperty(crypto, 'randomUUID', { configurable: true, value: undefined });
    const request = buy();
    expect(request.request.body.customerId).toMatch(
      /^web-[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
    );
    expect(request.request.headers.get('Idempotency-Key')).toMatch(
      /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
    );
    request.flush({ orderId: 'fallback' }, { status: 202, statusText: 'Accepted' });
  });

  it.each(['0', '5', '1.5', ''])('AC5 rejects invalid quantity %s without a purchase', (value) => {
    input('#quantity', value);
    root
      .querySelector('form')!
      .dispatchEvent(new Event('submit', { bubbles: true, cancelable: true }));
    http.expectNone('/api/orders');
  });

  it('AC5 keeps retry inputs locked and preserves the complete failed payload', () => {
    input('#quantity', '4');
    const first = buy();
    const payload = first.request.body;
    const key = first.request.headers.get('Idempotency-Key');
    first.flush({}, { status: 503, statusText: 'Unavailable' });
    fixture.detectChanges();
    expect(root.querySelector<HTMLInputElement>('#event-id')!.disabled).toBe(true);
    expect(root.querySelector<HTMLInputElement>('#quantity')!.disabled).toBe(true);
    const retry = buy();
    expect(retry.request.body).toEqual(payload);
    expect(retry.request.headers.get('Idempotency-Key')).toBe(key);
    retry.flush({ orderId: 'retried' }, { status: 202, statusText: 'Accepted' });
  });

  it('AC6 does not clear one source failure when only the other recovers', () => {
    vi.advanceTimersByTime(2000);
    http
      .expectOne('/api/events/concert-1/stock')
      .flush({}, { status: 503, statusText: 'Unavailable' });
    http
      .expectOne('/api/orders/activity')
      .flush({ windowSeconds: 10, ordersInWindow: 0, orders: [] });
    fixture.detectChanges();
    expect(root.textContent).toMatch(/\bstale\b/i);
  });

  it('AC6 changing event cancels an old stock read and never shows the previous event stock', () => {
    vi.advanceTimersByTime(2000);
    const old = http.expectOne('/api/events/concert-1/stock');
    http
      .expectOne('/api/orders/activity')
      .flush({ windowSeconds: 10, ordersInWindow: 0, orders: [] });
    input('#event-id', 'other-event');
    expect(old.cancelled).toBe(true);
    expect(root.querySelector('#stock-value')!.textContent).not.toContain('37');
    http.expectOne('/api/events/other-event/stock').flush({ eventId: 'other-event', stock: 8 });
    fixture.detectChanges();
    expect(root.querySelector('#stock-value')!.textContent?.trim()).toBe('8');
  });
});
