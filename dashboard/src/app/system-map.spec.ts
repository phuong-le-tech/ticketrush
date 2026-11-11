import { TestBed } from '@angular/core/testing';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { RecentOrder } from './flow';
import { SystemMap } from './system-map';

const order = (status: string): RecentOrder => ({
  orderId: 'a',
  eventId: 'concert-1',
  quantity: 1,
  status,
  failureReason: null,
  amountCents: null,
  currency: null,
  createdAt: '',
  updatedAt: '',
});

function render(orders: RecentOrder[] | null) {
  const fixture = TestBed.createComponent(SystemMap);
  fixture.componentRef.setInput('orders', orders);
  fixture.componentRef.setInput('mine', null);
  fixture.detectChanges();
  return fixture;
}

describe('SystemMap', () => {
  afterEach(() => vi.unstubAllGlobals());

  it('shows a dash, not 0, for a consumer with no rate series', () => {
    const el: HTMLElement = render(null).nativeElement;
    const rates = Array.from(el.querySelectorAll('.rate')).map((r) => r.textContent?.trim());
    expect(rates.length).toBe(5);
    expect(rates.every((r) => r === '-')).toBe(true);
  });

  it('renders the five real topic names', () => {
    const text = (render(null).nativeElement as HTMLElement).textContent ?? '';
    for (const t of [
      'orders.requested',
      'tickets.reserved',
      'payments.result',
      'orders.confirmed',
      'orders.rejected',
    ])
      expect(text).toContain(t);
  });

  it('flashes the destination node instead of drawing a dot under reduced motion', () => {
    vi.stubGlobal('matchMedia', () => ({ matches: true }));
    const fixture = render([]);
    fixture.componentRef.setInput('orders', [order('PENDING')]);
    fixture.detectChanges();
    const el: HTMLElement = fixture.nativeElement;
    expect(el.querySelectorAll('.dot').length).toBe(0);
    expect(el.querySelector('[data-stage="inventory"]')?.classList.contains('flash')).toBe(true);
  });

  it('draws and removes a dot where the Web Animations API is missing', () => {
    vi.stubGlobal('matchMedia', () => ({ matches: false }));
    const fixture = render([]);
    fixture.componentRef.setInput('orders', [order('PENDING')]);
    fixture.detectChanges();
    expect((fixture.nativeElement as HTMLElement).querySelectorAll('.dot').length).toBe(0);
  });
});
