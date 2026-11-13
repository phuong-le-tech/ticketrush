import {
  ChangeDetectionStrategy,
  Component,
  DestroyRef,
  ElementRef,
  computed,
  inject,
  input,
  signal,
} from '@angular/core';
import { Sample } from './metrics';

type XY = [number, number];

// With tNewest and windowMs, x is a fixed window ending at tNewest, shared by every series of a chart.
// Without them, x is linear between the first and last non-null point; a single point sits at x 0.
export function scalePoints(
  points: Sample[],
  width: number,
  height: number,
  yMax: number,
  tNewest?: number,
  windowMs?: number,
): XY[] {
  const live = points.filter((p): p is { t: number; v: number } => p.v !== null);
  if (!live.length) return [];
  if (tNewest !== undefined && windowMs !== undefined) {
    return live.map((p) => [
      width - ((tNewest - p.t) / windowMs) * width,
      height - (p.v / yMax) * height,
    ]);
  }
  const t0 = live[0].t;
  const span = live[live.length - 1].t - t0;
  return live.map((p) => [
    span > 0 ? ((p.t - t0) / span) * width : 0,
    height - (p.v / yMax) * height,
  ]);
}

export function linePath(xy: XY[]): string {
  return xy.length < 2 ? '' : xy.map(([x, y], i) => `${i ? 'L' : 'M'} ${x} ${y}`).join(' ');
}

// Next value of 1, 2, 5, 10, 20, 50... at or above the max; never below floor, never 0.
export function niceMax(values: (number | null)[], floor: number): number {
  const m = Math.max(floor, ...values.filter((v): v is number => v !== null));
  if (!(m > 0)) return 1;
  const base = 10 ** Math.floor(Math.log10(m));
  return [1, 2, 5, 10].find((c) => m / base <= c + 1e-9)! * base;
}

const PAD_R = 8;
const H = 172;
const PAD_L = 40;
const PAD_T = 8;

@Component({
  selector: 'tr-chart',
  changeDetection: ChangeDetectionStrategy.OnPush,
  template: `
    <svg [attr.viewBox]="'0 0 ' + width() + ' 200'" role="img" [attr.aria-label]="label()">
      <rect class="ground" x="0" y="0" [attr.width]="width()" height="200" />
      <text class="axis y-max" x="36" [attr.y]="PAD_T + 10" text-anchor="end">{{ yMax() }}</text>
      <text class="axis" x="36" [attr.y]="PAD_T + H" text-anchor="end">0</text>
      <text class="axis" [attr.x]="PAD_L" y="198">{{ windowLabel() }}</text>
      <text class="axis" [attr.x]="PAD_L + plotW()" y="198" text-anchor="end">now</text>
      <g [attr.transform]="'translate(' + PAD_L + ' ' + PAD_T + ')'">
        <line class="base" x1="0" [attr.y1]="H" [attr.x2]="plotW()" [attr.y2]="H" />
        @if (threshold() !== null) {
          <line
            class="threshold"
            x1="0"
            [attr.y1]="thresholdY()"
            [attr.x2]="plotW()"
            [attr.y2]="thresholdY()"
          />
          <text
            class="axis threshold-label"
            [attr.x]="plotW()"
            [attr.y]="thresholdY() < 14 ? thresholdY() + 14 : thresholdY() - 4"
            text-anchor="end"
          >
            {{ threshold() }} {{ unit() }}
          </text>
        }
        @if (!hasData()) {
          <text class="axis" [attr.x]="plotW() / 2" [attr.y]="H / 2" text-anchor="middle">
            No data yet
          </text>
        } @else {
          @for (k of [slideKey()]; track k) {
            <g class="slide" [style.--step]="step() + 'px'">
              @for (s of drawn(); track s.name) {
                @if (s.emphasis && area()) {
                  <path class="area" [attr.d]="s.area" />
                }
                <path class="line" [class.emphasis]="s.emphasis" [attr.d]="s.d" />
              }
              @if (head(); as h) {
                <circle class="head" [attr.cx]="h[0]" [attr.cy]="h[1]" r="4" />
              }
            </g>
          }
        }
      </g>
    </svg>
  `,
  styles: `
    :host {
      display: block;
    }
    svg {
      display: block;
      width: 100%;
      height: 200px;
    }
    .ground {
      fill: var(--surface);
    }
    .base {
      stroke: var(--hairline);
    }
    .threshold {
      stroke: var(--fg-3);
      stroke-dasharray: 4 4;
    }
    .axis {
      fill: var(--fg-2);
      font: 400 12px var(--font-mono, monospace);
      letter-spacing: 0.2px;
    }
    .line {
      fill: none;
      stroke: var(--fg-3);
      stroke-width: 1;
    }
    .line.emphasis {
      stroke: var(--accent);
      stroke-width: 1.5;
    }
    .area {
      fill: var(--accent);
      fill-opacity: 0.12;
    }
    .head {
      fill: var(--accent);
    }
    .slide {
      animation: slide var(--t-move, 600ms) var(--ease-out, cubic-bezier(0.32, 0.72, 0, 1));
    }
    @keyframes slide {
      from {
        transform: translateX(var(--step));
      }
    }
    @media (prefers-reduced-motion: reduce) {
      .slide {
        animation: none;
      }
    }
  `,
})
export class TimeSeriesChart {
  readonly series = input.required<{ name: string; emphasis: boolean; points: Sample[] }[]>();
  readonly threshold = input<number | null>(null);
  readonly yFloor = input(1);
  readonly area = input(false);
  readonly unit = input('');
  readonly windowMs = input(300000);

  // One viewBox unit is one CSS pixel: the width follows the host (600 without ResizeObserver).
  protected readonly width = signal(600);
  protected readonly plotW = computed(() => this.width() - PAD_L - PAD_R);
  protected readonly H = H;
  protected readonly PAD_L = PAD_L;
  protected readonly PAD_T = PAD_T;

  constructor() {
    const host = inject<ElementRef<HTMLElement>>(ElementRef).nativeElement;
    if (typeof ResizeObserver === 'undefined') return;
    const ro = new ResizeObserver(([e]) => {
      const w = Math.round(e.contentRect.width);
      if (w > PAD_L + PAD_R + 1) this.width.set(w);
    });
    ro.observe(host);
    inject(DestroyRef).onDestroy(() => ro.disconnect());
  }

  protected readonly yMax = computed(() => {
    const values = this.series().flatMap((s) => s.points.map((p) => p.v));
    const th = this.threshold();
    return niceMax(th === null ? values : [...values, th], this.yFloor());
  });
  protected readonly thresholdY = computed(() => H - ((this.threshold() ?? 0) / this.yMax()) * H);

  protected readonly windowLabel = computed(() => {
    const ms = this.windowMs();
    return ms % 60000 === 0 ? `-${ms / 60000}m` : `-${Math.round(ms / 1000)}s`;
  });
  private readonly tNewest = computed(() =>
    Math.max(0, ...this.series().flatMap((s) => s.points.map((p) => p.t))),
  );

  protected readonly drawn = computed(() =>
    this.series().map((full) => {
      // Points older than the window are not drawn.
      const s = {
        ...full,
        points: full.points.filter((p) => p.t >= this.tNewest() - this.windowMs()),
      };
      // Split at nulls so a gap breaks the line instead of bridging it.
      const xy = scalePoints(
        s.points,
        this.plotW(),
        H,
        this.yMax(),
        this.tNewest(),
        this.windowMs(),
      );
      const segs: XY[][] = [[]];
      let j = 0;
      for (const p of s.points) {
        if (p.v === null) segs.push([]);
        else segs[segs.length - 1].push(xy[j++]);
      }
      const lines = segs.filter((g) => g.length > 1);
      return {
        name: s.name,
        emphasis: s.emphasis,
        xy,
        d: lines.map(linePath).join(' '),
        area: lines
          .map((g) => `${linePath(g)} L ${g[g.length - 1][0]} ${H} L ${g[0][0]} ${H} Z`)
          .join(' '),
      };
    }),
  );
  protected readonly hasData = computed(() => this.drawn().some((s) => s.d !== ''));
  protected readonly head = computed(
    () =>
      this.drawn()
        .find((s) => s.emphasis)
        ?.xy.at(-1) ?? null,
  );

  // A new newest timestamp re-creates the group, which restarts the slide animation.
  protected readonly slideKey = this.tNewest;
  // One sample interval (last two timestamps of the longest series) in window scale.
  protected readonly step = computed(() => {
    const longest = this.series().reduce<Sample[]>(
      (a, s) => (s.points.length > a.length ? s.points : a),
      [],
    );
    const interval =
      longest.length > 1 ? longest[longest.length - 1].t - longest[longest.length - 2].t : 0;
    return (interval / this.windowMs()) * this.plotW();
  });

  protected readonly label = computed(() => {
    const names = this.series()
      .map((s) => s.name)
      .join(', ');
    const latest = this.series()
      .find((s) => s.emphasis)
      ?.points.filter((p) => p.v !== null)
      .at(-1)?.v;
    return latest === undefined
      ? `${names}: no data`
      : `${names}: latest ${latest}${this.unit() ? ' ' + this.unit() : ''}`;
  });
}
