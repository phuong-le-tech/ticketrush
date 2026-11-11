export interface RecentOrder {
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

export type Stage = 'gateway' | 'inventory' | 'payment' | 'order' | 'notification' | 'rejected';
export interface Dot {
  id: string;
  orderId: string;
  path: Stage[];
  mine: boolean;
}

// The stage where an order in each status rests.
export const PATH_FOR_STATUS: Record<string, Stage[]> = {
  PENDING: ['inventory'],
  RESERVED: ['payment'],
  CONFIRMED: ['notification'],
  REJECTED: ['rejected'],
  FAILED: ['order'],
};

const MAIN: Stage[] = ['gateway', 'inventory', 'payment', 'order', 'notification'];

function segment(from: Stage, to: Stage): Stage[] {
  const i = MAIN.indexOf(from);
  if (to === 'rejected') return i >= 0 && i <= 1 ? [...MAIN.slice(i, 2), 'rejected'] : [];
  const j = MAIN.indexOf(to);
  return i >= 0 && j > i ? MAIN.slice(i, j + 1) : [];
}

/** One dot per order that is new or changed status since `prev`. The first poll (prev null) is history, not events. */
export function transitions(
  prev: RecentOrder[] | null,
  next: RecentOrder[],
  mine: string | null,
): Dot[] {
  if (!prev) return [];
  const before = new Map(prev.map((o) => [o.orderId, o.status]));
  return next.flatMap((o) => {
    const to = PATH_FOR_STATUS[o.status]?.[0];
    const was = before.get(o.orderId);
    if (!to || was === o.status) return [];
    const path = segment(
      was === undefined ? 'gateway' : (PATH_FOR_STATUS[was]?.[0] ?? 'gateway'),
      to,
    );
    return path.length
      ? [{ id: `${o.orderId}:${o.status}`, orderId: o.orderId, path, mine: o.orderId === mine }]
      : [];
  });
}

/** At most `max` dots; mine dots are never dropped, the oldest others go first. */
export function capDots(active: Dot[], incoming: Dot[], max: number): Dot[] {
  const all = [...active, ...incoming];
  let excess = all.length - max;
  return all.filter((d) => d.mine || excess <= 0 || (excess--, false));
}
