import { DatePipe } from '@angular/common';
import { HttpClient, HttpErrorResponse } from '@angular/common/http';
import { Component, DestroyRef, OnInit, computed, inject, signal } from '@angular/core';
import { takeUntilDestroyed } from '@angular/core/rxjs-interop';
import {
  BehaviorSubject,
  EMPTY,
  catchError,
  exhaustMap,
  interval,
  startWith,
  switchMap,
  timeout,
} from 'rxjs';

const POLL_INTERVAL_MS = 2000;
const READ_TIMEOUT_MS = 8000;
const PURCHASE_TIMEOUT_MS = 15000;
const CUSTOMER_STORAGE_KEY = 'ticketrush.customerId';
const MAX_EVENT_ID_LENGTH = 255;
const MIN_QUANTITY = 1;
const MAX_QUANTITY = 4;

interface RecentOrder {
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

interface Activity {
  windowSeconds: number;
  ordersInWindow: number;
  orders: RecentOrder[];
}

interface PurchaseAttempt {
  key: string;
  body: { eventId: string; customerId: string; quantity: number };
}

function newUuid(): string {
  if (typeof crypto.randomUUID === 'function') return crypto.randomUUID();
  const bytes = crypto.getRandomValues(new Uint8Array(16));
  bytes[6] = (bytes[6] & 0x0f) | 0x40;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const hex = Array.from(bytes, (byte) => byte.toString(16).padStart(2, '0')).join('');
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function customerIdentity(): string {
  const stored = localStorage.getItem(CUSTOMER_STORAGE_KEY);
  if (
    stored &&
    /^web-[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(stored)
  ) {
    return stored;
  }
  const customerId = `web-${newUuid()}`;
  localStorage.setItem(CUSTOMER_STORAGE_KEY, customerId);
  return customerId;
}

@Component({
  imports: [DatePipe],
  selector: 'app-root',
  styleUrl: './app.css',
  templateUrl: './app.html',
})
export class App implements OnInit {
  private readonly http = inject(HttpClient);
  private readonly destroyRef = inject(DestroyRef);
  private readonly stockEvents = new BehaviorSubject('concert-1');
  protected readonly eventId = signal('concert-1');
  protected readonly quantity = signal(1);
  protected readonly stock = signal<number | null>(null);
  protected readonly activity = signal<Activity | null>(null);
  protected readonly stockStale = signal(false);
  protected readonly activityStale = signal(false);
  protected readonly stale = computed(() => this.stockStale() || this.activityStale());
  protected readonly rate = computed(() => {
    const activity = this.activity();
    return activity ? (activity.ordersInWindow / activity.windowSeconds).toFixed(1) : 'Loading';
  });
  protected readonly pending = signal(false);
  protected readonly attempt = signal<PurchaseAttempt | null>(null);
  protected readonly buyMessage = signal(
    '1 to 4 tickets per order. Acceptance is not confirmation.',
  );
  protected readonly buyError = signal(false);
  protected readonly validInput = computed(() => {
    const id = this.eventId().trim();
    return (
      id.length > 0 &&
      id.length <= MAX_EVENT_ID_LENGTH &&
      Number.isInteger(this.quantity()) &&
      this.quantity() >= MIN_QUANTITY &&
      this.quantity() <= MAX_QUANTITY
    );
  });

  ngOnInit(): void {
    this.stockEvents
      .pipe(
        switchMap((eventId) =>
          eventId
            ? interval(POLL_INTERVAL_MS).pipe(
                startWith(0),
                exhaustMap(() =>
                  this.http
                    .get<{ eventId: string; stock: number }>(
                      `/api/events/${encodeURIComponent(eventId)}/stock`,
                    )
                    .pipe(
                      timeout(READ_TIMEOUT_MS),
                      catchError(() => {
                        this.stockStale.set(true);
                        return EMPTY;
                      }),
                    ),
                ),
              )
            : EMPTY,
        ),
        takeUntilDestroyed(this.destroyRef),
      )
      .subscribe((response) => {
        this.stock.set(response.stock);
        this.stockStale.set(false);
      });

    interval(POLL_INTERVAL_MS)
      .pipe(
        startWith(0),
        exhaustMap(() =>
          this.http.get<Activity>('/api/orders/activity').pipe(
            timeout(READ_TIMEOUT_MS),
            catchError(() => {
              this.activityStale.set(true);
              return EMPTY;
            }),
          ),
        ),
        takeUntilDestroyed(this.destroyRef),
      )
      .subscribe((activity) => {
        this.activity.set(activity);
        this.activityStale.set(false);
      });
  }

  protected changeEvent(value: string): void {
    this.eventId.set(value);
    this.stock.set(null);
    this.stockStale.set(false);
    const id = value.trim();
    this.stockEvents.next(id && id.length <= MAX_EVENT_ID_LENGTH ? id : '');
  }

  protected changeQuantity(value: string): void {
    this.quantity.set(Number(value));
  }

  protected buy(event: Event): void {
    event.preventDefault();
    if (this.pending() || (!this.attempt() && !this.validInput())) return;
    if (!this.attempt()) {
      try {
        const customerId = customerIdentity();
        this.attempt.set({
          key: newUuid(),
          body: {
            eventId: this.eventId().trim(),
            customerId,
            quantity: this.quantity(),
          },
        });
      } catch {
        this.buyError.set(true);
        this.buyMessage.set(
          'Cannot safely create a purchase. Enable local storage and browser cryptography, then try again.',
        );
        return;
      }
    }
    const attempt = this.attempt()!;
    this.pending.set(true);
    this.buyError.set(false);
    this.buyMessage.set('Submitting purchase. Waiting for acceptance.');
    this.http
      .post<{ orderId: string }>('/api/orders', attempt.body, {
        headers: { 'Idempotency-Key': attempt.key },
        observe: 'response',
      })
      .pipe(timeout(PURCHASE_TIMEOUT_MS), takeUntilDestroyed(this.destroyRef))
      .subscribe({
        next: (response) => {
          this.pending.set(false);
          if (response.status === 202) {
            this.attempt.set(null);
            this.buyMessage.set(
              `Order ${response.body?.orderId ?? ''} accepted. Follow its saga state below; it is not yet confirmed.`,
            );
          } else {
            this.buyError.set(true);
            this.buyMessage.set('Unexpected response. Retry keeps the original purchase and key.');
          }
        },
        error: (error: unknown) => {
          this.pending.set(false);
          this.buyError.set(true);
          if (
            error instanceof HttpErrorResponse &&
            error.status >= 400 &&
            error.status < 500 &&
            error.status !== 429
          ) {
            this.attempt.set(null);
            this.buyMessage.set(
              `Purchase rejected (HTTP ${error.status}). Check the event and quantity before buying again.`,
            );
          } else {
            // A lost reply may hide an accepted purchase, so never replace this attempt on retry.
            this.buyMessage.set(
              'Purchase not acknowledged. Retry reuses the original purchase and key.',
            );
          }
        },
      });
  }

  protected amount(order: RecentOrder): string {
    if (order.amountCents === null || !order.currency) return 'Not available';
    // Keep cents exact rather than round Java long values through floating-point currency formatting.
    if (!Number.isSafeInteger(order.amountCents)) return 'Amount outside browser precision';
    return `${order.currency} ${BigInt(order.amountCents) / 100n}.${(BigInt(order.amountCents) % 100n).toString().padStart(2, '0')}`;
  }
}
