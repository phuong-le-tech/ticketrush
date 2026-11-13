import { Component, signal } from '@angular/core';
import { TestBed } from '@angular/core/testing';
import { describe, expect, it } from 'vitest';
import { Chapter } from './chapter';
import { SourceState } from './metrics';

@Component({
  imports: [Chapter],
  template: `
    <tr-chapter
      number="02"
      [title]="'KAFKA'"
      claim="A claim."
      mechanism="A mechanism."
      [state]="state()"
      [staleSince]="since()"
    >
      <i chart id="projected-chart">chart</i>
      <b figure id="projected-figure">42</b>
    </tr-chapter>
  `,
})
class Host {
  state = signal<SourceState>('live');
  since = signal<Date | null>(null);
}

function render(state: SourceState, since: Date | null = null) {
  const fixture = TestBed.createComponent(Host);
  fixture.componentInstance.state.set(state);
  fixture.componentInstance.since.set(since);
  fixture.detectChanges();
  return fixture.nativeElement as HTMLElement;
}

describe('Chapter', () => {
  it('unavailable renders only "Live metrics need the kind stack." and projects no chart or figure', () => {
    const root = render('unavailable');
    expect(root.textContent).toContain('Live metrics need the kind stack.');
    expect(root.querySelector('#projected-chart')).toBeNull();
    expect(root.querySelector('#projected-figure')).toBeNull();
  });

  it('stale keeps projected content and shows "stale since 14:03:07"', () => {
    const root = render('stale', new Date(2026, 10, 11, 14, 3, 7));
    expect(root.querySelector('#projected-chart')).not.toBeNull();
    expect(root.querySelector('#projected-figure')).not.toBeNull();
    expect(root.textContent).toContain('stale since 14:03:07');
  });

  it('live shows the label, the claim and the mechanism and no stale line', () => {
    const root = render('live');
    expect(root.textContent).toContain('02 / KAFKA');
    expect(root.textContent).toContain('A claim.');
    expect(root.textContent).toContain('A mechanism.');
    expect(root.textContent).not.toMatch(/stale/i);
  });

  it('loading shows a dash in the figure slot fallback', () => {
    const root = render('loading');
    expect(root.querySelector('#projected-figure')).toBeNull();
    expect(root.querySelector('.dash')?.textContent?.trim()).toBe('-');
  });
});
