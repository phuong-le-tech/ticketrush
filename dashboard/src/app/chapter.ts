import { DatePipe } from '@angular/common';
import { ChangeDetectionStrategy, Component, input } from '@angular/core';
import { SourceState } from './metrics';

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
