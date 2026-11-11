import { describe, expect, it } from 'vitest';
import { Dot, RecentOrder, capDots, transitions } from './flow';

function order(orderId: string, status: string): RecentOrder {
  return {
    orderId,
    eventId: 'concert-1',
    quantity: 1,
    status,
    failureReason: null,
    amountCents: null,
    currency: null,
    createdAt: '2026-11-11T12:00:00Z',
    updatedAt: '2026-11-11T12:00:00Z',
  };
}
const seen = (id: string, status: string) => [order(id, status)];

describe('transitions', () => {
  it('first poll emits no dots', () =>
    expect(transitions(null, [order('a', 'CONFIRMED')], null)).toEqual([]));
  it('a new PENDING order travels gateway to inventory', () =>
    expect(transitions([], seen('a', 'PENDING'), null)[0].path).toEqual(['gateway', 'inventory']));
  it('PENDING to RESERVED travels inventory to payment', () =>
    expect(transitions(seen('a', 'PENDING'), seen('a', 'RESERVED'), null)[0].path).toEqual([
      'inventory',
      'payment',
    ]));
  it('RESERVED to CONFIRMED travels payment, order, notification', () =>
    expect(transitions(seen('a', 'RESERVED'), seen('a', 'CONFIRMED'), null)[0].path).toEqual([
      'payment',
      'order',
      'notification',
    ]));
  it('a new order already CONFIRMED travels the whole path', () =>
    expect(transitions([], seen('a', 'CONFIRMED'), null)[0].path).toEqual([
      'gateway',
      'inventory',
      'payment',
      'order',
      'notification',
    ]));
  it('REJECTED exits at inventory through the rejected stage', () => {
    expect(transitions(seen('a', 'PENDING'), seen('a', 'REJECTED'), null)[0].path).toEqual([
      'inventory',
      'rejected',
    ]);
    expect(transitions([], seen('a', 'REJECTED'), null)[0].path).toEqual([
      'gateway',
      'inventory',
      'rejected',
    ]);
  });
  it('a failed payment stops at the order node', () =>
    expect(transitions(seen('a', 'RESERVED'), seen('a', 'FAILED'), null)[0].path).toEqual([
      'payment',
      'order',
    ]));
  it('an unchanged order emits nothing', () =>
    expect(transitions(seen('a', 'PENDING'), seen('a', 'PENDING'), null)).toEqual([]));
  it('a move backwards on the main path emits nothing', () =>
    expect(transitions(seen('a', 'CONFIRMED'), seen('a', 'PENDING'), null)).toEqual([]));
  it('an unknown status emits nothing and does not throw', () =>
    expect(transitions([], seen('a', 'WHATEVER'), null)).toEqual([]));
  it('marks only the order matching mine, with a unique id per emission', () => {
    const dots = transitions([], [order('a', 'PENDING'), order('b', 'PENDING')], 'b');
    expect(dots.map((d) => d.mine)).toEqual([false, true]);
    const later = transitions(seen('a', 'PENDING'), seen('a', 'RESERVED'), null);
    expect(new Set([dots[0].id, later[0].id]).size).toBe(2);
  });
});

describe('capDots', () => {
  const dot = (id: string, mine = false): Dot => ({ id, orderId: id, path: [], mine });
  it('keeps at most max, never drops a mine dot, drops the oldest others first', () => {
    const active = [dot('1'), dot('2', true), dot('3'), dot('4')];
    const kept = capDots(active, [dot('5'), dot('6')], 4);
    expect(kept.map((d) => d.id)).toEqual(['2', '4', '5', '6']);
  });
  it('returns everything under the cap', () =>
    expect(capDots([dot('1')], [dot('2')], 40).map((d) => d.id)).toEqual(['1', '2']));
});
