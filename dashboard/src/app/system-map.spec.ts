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

  it('discloses that dots are inferred from polled status changes', () => {
    const text = (render(null).nativeElement as HTMLElement).querySelector('.legend')?.textContent;
    expect(text).toContain('One dot = one order whose status changed, polled every 2 s');
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

  it('draws no dot where the Web Animations API is missing', () => {
    vi.stubGlobal('matchMedia', () => ({ matches: false }));
    const fixture = render([]);
    fixture.componentRef.setInput('orders', [order('PENDING')]);
    fixture.detectChanges();
    expect((fixture.nativeElement as HTMLElement).querySelectorAll('.dot').length).toBe(0);
  });

  it('adds a dot, accent for mine, and removes it when the animation finishes', () => {
    vi.stubGlobal('matchMedia', () => ({ matches: false }));
    const animations: { onfinish?: () => void }[] = [];
    Element.prototype.animate = (() => {
      const a = {};
      animations.push(a);
      return a;
    }) as unknown as typeof Element.prototype.animate;
    try {
      const fixture = render([]);
      fixture.componentRef.setInput('mine', 'a');
      fixture.componentRef.setInput('orders', [order('PENDING')]);
      fixture.detectChanges();
      const el: HTMLElement = fixture.nativeElement;
      const dots = el.querySelectorAll('.dot.mine');
      expect(dots.length).toBeGreaterThan(0);
      expect((dots[0] as SVGElement).style.fill).toBe('var(--accent)');
      animations.forEach((a) => a.onfinish?.());
      expect(el.querySelectorAll('.dot').length).toBe(0);
    } finally {
      delete (Element.prototype as { animate?: unknown }).animate;
    }
  });

  it('fits a 1344px host: svg as wide as the host, every node inside the margins', () => {
    vi.stubGlobal(
      'ResizeObserver',
      class {
        constructor(private cb: (e: unknown[]) => void) {}
        observe() {
          this.cb([{ contentRect: { width: 1344 } }]);
        }
        disconnect() {}
      },
    );
    const el: HTMLElement = render(null).nativeElement;
    expect(el.querySelector('svg')?.getAttribute('width')).toBe('1344');
    const rects = Array.from(el.querySelectorAll('[data-stage] rect'));
    expect(rects.length).toBeGreaterThan(6);
    for (const r of rects) {
      expect(+r.getAttribute('x')! + +r.getAttribute('width')!).toBeLessThanOrEqual(1344 - 16);
      expect(+r.getAttribute('x')!).toBeGreaterThanOrEqual(16);
    }
  });
});
