import { TestBed } from '@angular/core/testing';
import { describe, expect, it } from 'vitest';
import { TimeSeriesChart, linePath, niceMax, scalePoints } from './chart';
import { Sample } from './metrics';

type S = { name: string; emphasis: boolean; points: Sample[] };
const pts = (...vs: (number | null)[]): Sample[] => vs.map((v, i) => ({ t: i * 1000, v }));

function render(series: S[], inputs: Record<string, unknown> = {}) {
  const fixture = TestBed.createComponent(TimeSeriesChart);
  fixture.componentRef.setInput('series', series);
  for (const [k, v] of Object.entries(inputs)) fixture.componentRef.setInput(k, v);
  fixture.detectChanges();
  return fixture.nativeElement as HTMLElement;
}

describe('scalePoints', () => {
  it('maps the y range to the box with 0 at the bottom', () => {
    expect(
      scalePoints(
        [
          { t: 0, v: 0 },
          { t: 10, v: 5 },
          { t: 20, v: 10 },
        ],
        200,
        100,
        10,
      ),
    ).toEqual([
      [0, 100],
      [100, 50],
      [200, 0],
    ]);
  });
  it('skips null values', () => {
    expect(scalePoints(pts(0, null, 10), 100, 100, 10)).toEqual([
      [0, 100],
      [100, 0],
    ]);
  });
  it('puts a single point at x 0', () => {
    expect(scalePoints(pts(5), 100, 100, 10)).toEqual([[0, 50]]);
  });
});

describe('niceMax', () => {
  it('never returns 0: all-zero and empty input give the floor', () => {
    expect(niceMax([0, 0, 0], 1)).toBe(1);
    expect(niceMax([], 10)).toBe(10);
    expect(niceMax([null], 1)).toBe(1);
  });
  it('rounds up along 1, 2, 5, 10', () => {
    expect(niceMax([3], 1)).toBe(5);
    expect(niceMax([5], 1)).toBe(5);
    expect(niceMax([5.1], 1)).toBe(10);
    expect(niceMax([130], 1)).toBe(200);
    expect(niceMax([0.3], 0.1)).toBe(0.5);
  });
});

describe('linePath', () => {
  it('is empty for 0 or 1 points and M/L for more', () => {
    expect(linePath([])).toBe('');
    expect(linePath([[1, 2]])).toBe('');
    expect(
      linePath([
        [0, 1],
        [2, 3],
        [4, 5],
      ]),
    ).toBe('M 0 1 L 2 3 L 4 5');
  });
});

describe('TimeSeriesChart', () => {
  it('renders "No data yet" and no path when all points are null', () => {
    const el = render([{ name: 'a', emphasis: true, points: pts(null, null, null) }]);
    expect(el.textContent).toContain('No data yet');
    expect(el.querySelector('path')).toBeNull();
  });

  it('draws a flat line on the floor when stock is 0 throughout', () => {
    const el = render([{ name: 'stock', emphasis: true, points: pts(0, 0, 0, 0) }]);
    const d = el.querySelector('path.line')!.getAttribute('d')!;
    const ys = [...d.matchAll(/ (-?[\d.]+)(?= L|$)/g)].map((m) => m[1]);
    expect(ys).toHaveLength(4);
    expect(new Set(ys)).toEqual(new Set(['172']));
  });

  it('breaks the line at null points', () => {
    const el = render([{ name: 'a', emphasis: true, points: pts(1, 2, null, 3, 4) }]);
    expect(el.querySelector('path.line')!.getAttribute('d')!.match(/M/g)).toHaveLength(2);
  });

  it('renders the threshold line and its label when threshold is set', () => {
    const el = render([{ name: 'a', emphasis: true, points: pts(1, 2, 3) }], {
      threshold: 50,
      unit: 'ms',
    });
    expect(el.querySelector('line.threshold')).not.toBeNull();
    expect(el.querySelector('.threshold-label')!.textContent).toContain('50');
    expect(el.querySelector('.y-max')!.textContent).toBe('50');
  });

  it('marks exactly one series as emphasised with the accent stroke', () => {
    const el = render([
      { name: 'a', emphasis: true, points: pts(1, 2, 3) },
      { name: 'b', emphasis: false, points: pts(3, 2, 1) },
    ]);
    expect(el.querySelectorAll('path.line.emphasis')).toHaveLength(1);
    expect(el.querySelectorAll('path.line:not(.emphasis)')).toHaveLength(1);
    expect(el.querySelectorAll('circle')).toHaveLength(1);
  });

  it('labels the axes and describes the chart for assistive tech', () => {
    const el = render([{ name: 'orders', emphasis: true, points: pts(1, 2, 3) }], { unit: '/s' });
    expect(el.textContent).toContain('-5m');
    expect(el.textContent).toContain('now');
    expect(el.querySelector('svg')!.getAttribute('role')).toBe('img');
    expect(el.querySelector('svg')!.getAttribute('aria-label')).toBe('orders: latest 3 /s');
  });

  it('fills under the emphasised series only when area is set', () => {
    const s = [{ name: 'a', emphasis: true, points: pts(1, 2, 3) }];
    expect(render(s).querySelector('path.area')).toBeNull();
    expect(render(s, { area: true }).querySelector('path.area')).not.toBeNull();
  });
});
