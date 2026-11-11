import { DatePipe } from '@angular/common';
import { ChangeDetectionStrategy, Component, input } from '@angular/core';
import { Series, SourceState } from './metrics';

const FRESH_MS = 15000;

export interface Current {
  labels: Record<string, string>;
  v: number | null;
}

// The store keeps the old points of a series that vanished from a poll. A figure may only use a
// series whose newest point is within 15s of the newest point of the query; the rest is absent.
export function current(all: Series[]): Current[] {
  const newest = Math.max(0, ...all.flatMap((s) => s.points.map((p) => p.t)));
  return all.flatMap((s) => {
    const last = s.points.at(-1);
    return last && last.t >= newest - FRESH_MS ? [{ labels: s.labels, v: last.v }] : [];
  });
}

@Component({
  selector: 'tr-chapter',
  imports: [DatePipe],
  changeDetection: ChangeDetectionStrategy.OnPush,
  styleUrl: './chapter.css',
  template: `
    <p class="label">{{ number() }} / {{ title() }}</p>
    <h2 class="claim">{{ claim() }}</h2>
    @if (state() === 'unavailable') {
      <p class="unavailable">Live metrics need the kind stack.</p>
    } @else {
      <div class="row">
        <div class="chart"><ng-content select="[chart]" /></div>
        <div class="figure">
          @if (state() === 'loading') {
            <span class="dash">-</span>
          } @else {
            <ng-content select="[figure]" />
          }
        </div>
      </div>
    }
    <p class="mechanism">{{ mechanism() }}</p>
    @if (state() === 'stale' && staleSince(); as since) {
      <p class="stale">stale since {{ since | date: 'HH:mm:ss' }}</p>
    }
    @if (state() !== 'unavailable') {
      <ng-content select="[note]" />
    }
  `,
})
export class Chapter {
  readonly number = input.required<string>();
  readonly title = input.required<string>();
  readonly claim = input.required<string>();
  readonly mechanism = input.required<string>();
  readonly state = input.required<SourceState>();
  readonly staleSince = input<Date | null>(null);
}
