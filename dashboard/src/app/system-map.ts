import {
  ChangeDetectionStrategy,
  Component,
  DestroyRef,
  ElementRef,
  computed,
  effect,
  inject,
  input,
  signal,
  untracked,
  viewChild,
} from '@angular/core';
import { Dot, RecentOrder, Stage, capDots, transitions } from './flow';
import { FLASH_MS, MetricsStore, current } from './metrics';

const H = 420;
const NODE_W = 152;
const NODE_H = 64;
const SAT_W = 120;
const SAT_H = 44;
const PILL_BOTTOM = 44; // above the edge line; node tops are 32px above it
const MARGIN = 16;
const MAX_DOTS = 40;
const EDGE_MS = 600;
const EASE = 'cubic-bezier(0.32, 0.72, 0, 1)';
const LINE_Y = Math.round(H * 0.45);
const SAT_Y = LINE_Y + 130;
const REJECTED_Y = 40;
const REJECTED_PILL_Y = 90; // clear of the pills above the main edges
const TRAIL = [
  { delay: 0, opacity: 1 },
  { delay: 70, opacity: 0.5 },
  { delay: 140, opacity: 0.3 },
  { delay: 210, opacity: 0.15 },
];

interface Spec {
  stage: Stage | 'buyer';
  label: string;
  consumer?: string;
}
const SPECS: Spec[] = [
  { stage: 'buyer', label: 'buyer' },
  { stage: 'gateway', label: 'api-gateway' },
  { stage: 'inventory', label: 'inventory-service', consumer: 'inventory-service' },
  { stage: 'payment', label: 'payment-service', consumer: 'payment-service' },
  { stage: 'order', label: 'order-service', consumer: 'order-service' },
  { stage: 'notification', label: 'notification-service', consumer: 'notification-service' },
];
const MAP_TOPICS = ['orders.requested', 'tickets.reserved', 'payments.result', 'orders.confirmed'];
// Redis, the fake provider and Postgres hang under inventory, payment and order.
const SATELLITES = [
  { label: 'Redis', at: 2 },
  { label: 'fake provider', at: 3 },
  { label: 'Postgres', at: 4 },
];
const ARIA =
  'System map. An order goes from the buyer to api-gateway, then inventory-service through orders.requested, ' +
  'payment-service through tickets.reserved, order-service through payments.result and notification-service ' +
  'through orders.confirmed. A refused order leaves inventory-service through orders.rejected.';

function rate(v: number | null | undefined): string {
  return v == null ? '-' : `${v.toFixed(1)} /s`;
}

@Component({
  selector: 'tr-system-map',
  changeDetection: ChangeDetectionStrategy.OnPush,
  styleUrl: './system-map.css',
  template: `
    <svg
      [attr.viewBox]="'0 0 ' + w() + ' ' + H"
      [attr.width]="w()"
      [attr.height]="H"
      role="img"
      [attr.aria-label]="aria"
    >
      <rect class="ground" x="0" y="0" [attr.width]="w()" [attr.height]="H" />
      @for (e of edges(); track $index) {
        <line
          class="edge"
          [attr.x1]="e.x1"
          [attr.y1]="e.y1"
          [attr.x2]="e.x2"
          [attr.y2]="e.y2"
          [class.dashed]="e.dashed"
        />
      }
      @for (p of pills(); track p.text) {
        <rect
          class="pill"
          [attr.x]="p.x - p.w / 2"
          [attr.y]="p.y - 10"
          [attr.width]="p.w"
          height="20"
          rx="6"
        />
        <text class="topic" [attr.x]="p.x" [attr.y]="p.y + 4" text-anchor="middle">
          {{ p.text }}
        </text>
      }
      @for (n of nodes(); track n.label) {
        <g
          class="node"
          [class.flash]="flashes()[n.stage]"
          [class.bad]="n.stage === 'rejected' || n.stage === 'order'"
          [attr.data-stage]="n.stage"
        >
          <rect
            [attr.x]="n.x - n.w / 2"
            [attr.y]="n.y - n.h / 2"
            [attr.width]="n.w"
            [attr.height]="n.h"
            rx="6"
          />
          <text
            class="name"
            [attr.x]="n.x"
            [attr.y]="n.rate === null ? n.y + 4 : n.y - 4"
            [attr.textLength]="n.label.length > 17 ? 136 : null"
            lengthAdjust="spacingAndGlyphs"
            text-anchor="middle"
          >
            {{ n.label }}
          </text>
          @if (n.rate !== null) {
            <text class="rate" [attr.x]="n.x" [attr.y]="n.y + 16" text-anchor="middle">
              {{ n.rate }}
            </text>
          }
        </g>
      }
      <g #layer aria-hidden="true"></g>
    </svg>
    <p class="legend">
      <span class="key"><i class="swatch"></i>Background orders</span>
      <span class="key"><i class="swatch mine"></i>Your order</span>
      <span class="key note">One dot = one order whose status changed, polled every 2 s</span>
    </p>
  `,
})
export class SystemMap {
  readonly orders = input.required<RecentOrder[] | null>();
  readonly mine = input.required<string | null>();

  private readonly store = inject(MetricsStore);
  private readonly layer = viewChild<ElementRef<SVGGElement>>('layer');
  private readonly width = signal(600);
  private prev: RecentOrder[] | null = null;
  private readonly active = new Map<string, { dot: Dot; els: SVGElement[] }>();
  private readonly timers = new Set<ReturnType<typeof setTimeout>>();
  protected readonly flashes = signal<Record<string, boolean>>({});
  protected readonly H = H;
  protected readonly aria = ARIA;
  protected readonly w = computed(() => this.width());

  private readonly xs = computed(() => {
    const step = (this.w() - 2 * MARGIN - NODE_W) / 5;
    return SPECS.map((_, i) => MARGIN + NODE_W / 2 + i * step);
  });

  private readonly rates = computed(() => {
    const consumers = current(this.store.series('consumerRate')());
    const orders = current(this.store.series('ordersRate')())[0]?.v;
    return (s: Spec) => {
      if (s.stage === 'gateway') return rate(orders);
      if (!s.consumer) return null;
      const mine = consumers.filter((c) => c.labels['consumer'] === s.consumer && c.v !== null);
      return rate(mine.length ? mine.reduce((sum, c) => sum + c.v!, 0) : null);
    };
  });

  protected readonly nodes = computed(() => {
    const rateOf = this.rates();
    const xs = this.xs();
    return [
      ...SPECS.map((s, i) => ({
        stage: s.stage,
        label: s.label,
        x: xs[i],
        y: LINE_Y,
        w: NODE_W,
        h: NODE_H,
        rate: s.stage === 'buyer' ? null : rateOf(s),
      })),
      {
        stage: 'rejected' as const,
        label: 'rejected',
        x: xs[2],
        y: REJECTED_Y,
        w: SAT_W,
        h: 32,
        rate: null,
      },
      ...SATELLITES.map((s) => ({
        stage: s.label,
        label: s.label,
        x: xs[s.at],
        y: SAT_Y,
        w: SAT_W,
        h: SAT_H,
        rate: null,
      })),
    ];
  });

  protected readonly edges = computed(() => {
    const xs = this.xs();
    const main = xs.slice(0, -1).map((x, i) => ({
      x1: x + NODE_W / 2,
      y1: LINE_Y,
      x2: xs[i + 1] - NODE_W / 2,
      y2: LINE_Y,
      dashed: false,
    }));
    const up = {
      x1: xs[2],
      y1: LINE_Y - NODE_H / 2,
      x2: xs[2],
      y2: REJECTED_Y + 16,
      dashed: false,
    };
    const sats = SATELLITES.map((s) => ({
      x1: xs[s.at],
      y1: LINE_Y + NODE_H / 2,
      x2: xs[s.at],
      y2: SAT_Y - SAT_H / 2,
      dashed: true,
    }));
    const ticks = MAP_TOPICS.map((_, i) => ({
      x1: (xs[i + 1] + xs[i + 2]) / 2,
      y1: LINE_Y - PILL_BOTTOM,
      x2: (xs[i + 1] + xs[i + 2]) / 2,
      y2: LINE_Y,
      dashed: false,
    }));
    return [...main, up, ...ticks, ...sats];
  });

  protected readonly pills = computed(() => {
    const xs = this.xs();
    const on = MAP_TOPICS.map((text, i) => ({
      text,
      x: (xs[i + 1] + xs[i + 2]) / 2,
      y: LINE_Y - PILL_BOTTOM - 10,
    }));
    return [...on, { text: 'orders.rejected', x: xs[2], y: REJECTED_PILL_Y }].map((p) => ({
      ...p,
      w: p.text.length * 7.2 + 8,
    }));
  });

  constructor() {
    const host = inject<ElementRef<HTMLElement>>(ElementRef).nativeElement;
    const destroyRef = inject(DestroyRef);
    destroyRef.onDestroy(() => this.timers.forEach(clearTimeout));
    if (typeof ResizeObserver !== 'undefined') {
      const ro = new ResizeObserver(([e]) => {
        const w = Math.round(e.contentRect.width);
        if (w > 0) this.width.set(w);
      });
      ro.observe(host);
      destroyRef.onDestroy(() => ro.disconnect());
    }
    effect(() => {
      const next = this.orders();
      const mine = this.mine();
      if (next) untracked(() => this.ingest(next, mine));
    });
  }

  private point(stage: Stage): [number, number] {
    const xs = this.xs();
    const i = SPECS.findIndex((s) => s.stage === stage);
    return stage === 'rejected' ? [xs[2], REJECTED_Y] : [xs[i], LINE_Y];
  }

  private ingest(next: RecentOrder[], mine: string | null): void {
    const incoming = transitions(this.prev, next, mine);
    this.prev = next;
    if (!incoming.length) return;
    // Read once per emission so a changed OS setting applies to the next order.
    const reduced =
      typeof matchMedia === 'function' && matchMedia('(prefers-reduced-motion: reduce)').matches;
    if (reduced) {
      incoming.forEach((d) => this.flash(d.path[d.path.length - 1]));
      return;
    }
    const kept = capDots(
      [...this.active.values()].map((a) => a.dot),
      incoming,
      MAX_DOTS,
    );
    for (const [id, a] of this.active) if (!kept.includes(a.dot)) this.drop(id);
    incoming.filter((d) => kept.includes(d)).forEach((d) => this.spawn(d));
  }

  private drop(id: string): void {
    this.active.get(id)?.els.forEach((el) => el.remove());
    this.active.delete(id);
  }

  private spawn(dot: Dot): void {
    const layer = this.layer()?.nativeElement;
    if (!layer || typeof layer.animate !== 'function') return; // no Web Animations API: draw nothing
    const pts = dot.path.map((s) => this.point(s));
    const last = dot.path[dot.path.length - 1];
    const fill = dot.mine
      ? 'var(--accent)'
      : last === 'rejected' || last === 'order'
        ? 'var(--bad)'
        : 'var(--fg-3)';
    const els = (dot.mine ? TRAIL : TRAIL.slice(0, 1)).map((t) => {
      const el = document.createElementNS('http://www.w3.org/2000/svg', 'circle');
      el.classList.add('dot');
      if (dot.mine) el.classList.add('mine');
      el.setAttribute('r', '3');
      el.style.fill = fill;
      el.style.opacity = String(dot.mine ? t.opacity : 1);
      el.style.transform = `translate(${pts[0][0]}px, ${pts[0][1]}px)`;
      layer.appendChild(el);
      const frames = pts.map(([x, y], i) => ({
        transform: `translate(${x}px, ${y}px)`,
        offset: i / (pts.length - 1),
        easing: EASE,
      }));
      el.animate(frames, {
        duration: EDGE_MS * (pts.length - 1),
        delay: t.delay,
        fill: 'both',
      }).onfinish = () => {
        el.remove();
        if (els.every((e) => !e.isConnected)) this.active.delete(dot.id);
      };
      return el;
    });
    this.active.set(dot.id, { dot, els });
  }

  private flash(stage: Stage): void {
    this.flashes.update((f) => ({ ...f, [stage]: true }));
    const t = setTimeout(() => {
      this.timers.delete(t);
      this.flashes.update((f) => ({ ...f, [stage]: false }));
    }, FLASH_MS);
    this.timers.add(t);
  }
}
