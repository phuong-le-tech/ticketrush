# Live Explanatory Dashboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the M7 operations dashboard into a live page that explains concurrency, Kafka, Redis and Kubernetes scaling with data read from the running kind stack.

**Architecture:** The Angular app keeps its two existing read APIs and its buy logic, and gains a metrics layer that polls Prometheus through two new same-origin nginx routes. The page becomes a system map, four chapters each built on one shared SVG time-series chart, a buy rail and the orders table. The dashboard image is added to the Helm chart so the demo runs on kind.

**Tech Stack:** Angular 22.2.5 (standalone components, signals, RxJS 7.8), Vitest 5 + jsdom, nginx-unprivileged, Helm, Prometheus HTTP API, bash. No new npm dependency.

**Spec:** [docs/superpowers/specs/2026-11-11-live-dashboard-design.md](../specs/2026-11-11-live-dashboard-design.md). Visual rules: [docs/design/DESIGN.md](../../design/DESIGN.md). Approved mockup (simulated data, look only): OpenDesign project `ticketrush-live-control-room-1a90`, `index.html`.

## Global Constraints

- Branch `milestone-8`. Conventional commits, one logical change each. Do not push or merge.
- Never use the en dash or em dash characters anywhere. Use a plain hyphen.
- No figure on screen may be sample or hard-coded data. Missing data renders as a dash or a named state, never a number.
- No new Java endpoint, event schema, topic, partition count or DB change. No new npm dependency, no charting or animation library.
- Money stays long cents plus currency; keep the existing `amount()` BigInt formatting.
- Buy behaviour is frozen: same Idempotency-Key on retry, inputs locked while an attempt is open, the copy "Acceptance is not confirmation". Every existing `AC5`/`AC6`/`AC7` test must still pass with its assertions unchanged (selectors may be updated only where the DOM moved).
- Only the tokens in DESIGN.md: colors section 4, type scale section 5, spacing and radii section 6, motion section 8. Dark only, design width 1440px.
- Motion uses transform and opacity only, `cubic-bezier(0.32, 0.72, 0, 1)`, 150ms and 600ms, and is disabled under `prefers-reduced-motion: reduce`.
- Three corrections to the mockup, decided with the owner: the system map is the tallest region (about 420px, nodes at least 120x56px); charts fill the chapter's chart column; the series that matters in each chapter is amber, the others grey. The page title is factual: "One order, from click to confirmation."
- Do not call Redis a cache anywhere in UI copy or docs.
- `scripts/dashboard-acceptance.sh` (Compose, M7) must keep passing.
- kubectl always with `--context kind-ticketrush`; never change the current context.

## Review Focus

1. Prometheus unreachable (Compose, or port-forward dropped): chapters 02-04 must show "Live metrics need the kind stack" (never reached) or "stale since hh:mm:ss" (was live), and must not blank the stock chart, Buy or the table. Pinned in Task 2 and Task 5.
2. A query that succeeds with an empty result or `NaN` (idle system: latency is 0/0): figure shows a dash, chart shows the idle line, no `NaN` or `undefined` text. Pinned in Task 2 and Task 3.
3. Stock at 0 and all-equal series: the chart must not divide by zero and the line sits on the floor. Pinned in Task 3.
4. A burst of orders between two polls (more transitions than the dot cap): at most 40 dots, no unbounded DOM growth, rate label still true. Pinned in Task 6.
5. Anything other than GET, or any path other than the two allowed, on `/api/metrics/`: rejected by nginx and by the dev proxy. Pinned in Task 1 and Task 8.

---

## File Structure

| File | Responsibility |
|---|---|
| `dashboard/nginx.conf.template` (replaces `nginx.conf`) | Static serving and the five same-origin proxy routes, parameterised by resolver and upstream suffix |
| `dashboard/proxy.conf.json` | Dev proxy, same route boundaries |
| `dashboard/src/app/metrics.ts` | PromQL constants, response parsing, `MetricsStore` (polling, per-source state) |
| `dashboard/src/app/chart.ts` | Pure geometry (`scalePoints`, `linePath`) and the `TimeSeriesChart` component |
| `dashboard/src/app/flow.ts` | Pure order-to-dot logic (`transitions`, `capDots`) |
| `dashboard/src/app/system-map.ts` (+ `.css`) | SVG map, nodes, topic pills, travelling dots |
| `dashboard/src/app/chapter.ts` (+ `.css`) | One chapter block: number, claim, chart slot, figure slot, mechanism line, state line |
| `dashboard/src/app/app.ts`, `app.html`, `app.css` | Page composition; keeps stock, activity and buy state exactly as today |
| `dashboard/src/styles.css`, `dashboard/public/fonts/` | Tokens and self-hosted Geist |
| `deploy/helm/ticketrush/templates/dashboard.yaml`, `values.yaml` | Dashboard Deployment and Service on kind |
| `scripts/observability-up.sh` | Builds and loads the dashboard image |
| `scripts/live-dashboard-acceptance.sh` | Acceptance command |

---

### Task 1: Same-origin metrics proxy (nginx and dev proxy)

**Files:**
- Create: `dashboard/nginx.conf.template` (move content from `dashboard/nginx.conf`, then delete `nginx.conf`)
- Modify: `dashboard/Dockerfile`, `dashboard/proxy.conf.json`, `dashboard/proxy.test.cjs`, `deploy/compose/docker-compose.yml` (dashboard service env)

**Interfaces:**
- Produces: `GET /api/metrics/query?query=<promql>` and `GET /api/metrics/query_range?query=&start=&end=&step=`, returning the Prometheus HTTP API JSON unchanged. Env vars of the image: `NGINX_RESOLVER` (default `127.0.0.11`), `UPSTREAM_SUFFIX` (default empty), `PROMETHEUS_ADDR` (default `prometheus:9090`).

- [ ] **Step 1: Extend the failing dev-proxy test** in `dashboard/proxy.test.cjs`, inside the existing test:

```js
assert.deepEqual(route('/api/metrics/query?query=up'), {
  target: 'http://localhost:9090',
  path: '/api/v1/query?query=up',
});
assert.deepEqual(route('/api/metrics/query_range?query=up&step=5'), {
  target: 'http://localhost:9090',
  path: '/api/v1/query_range?query=up&step=5',
});
for (const path of ['/api/metrics', '/api/metrics/', '/api/metrics/admin/tsdb', '/api/metrics/querymore']) {
  assert.equal(route(path), null, path);
}
```

- [ ] **Step 2: Run** `cd dashboard && node --test proxy.test.cjs`. Expected: FAIL on the first new assertion.
- [ ] **Step 3: Add two exact-regex entries** to `proxy.conf.json`, keys `^/api/metrics/query(?:\\?.*)?$` and `^/api/metrics/query_range(?:\\?.*)?$`, target `http://localhost:9090`, rewriting the `/api/metrics/` prefix to `/api/v1/`. Place them before the `/api/events/**` prefix entry.
- [ ] **Step 4: Run** the same command. Expected: PASS.
- [ ] **Step 5: Convert nginx to a template.** The nginx-unprivileged image renders `/etc/nginx/templates/*.template` with envsubst at start, substituting only defined env vars, so `$uri` and friends are left alone. In `nginx.conf.template`:
  - `resolver ${NGINX_RESOLVER} valid=10s ipv6=off;`
  - each upstream server becomes `api-gateway${UPSTREAM_SUFFIX}:8080 resolve;` (same for `order-service`, `inventory-service`). Nginx's resolver ignores DNS search domains, so kind needs the full service name; Compose needs none.
  - two new exact locations, before `location /api/ { return 404; }`:

```nginx
location = /api/metrics/query       { limit_except GET { deny all; } set $prom ${PROMETHEUS_ADDR}; proxy_pass http://$prom/api/v1/query$is_args$args; }
location = /api/metrics/query_range { limit_except GET { deny all; } set $prom ${PROMETHEUS_ADDR}; proxy_pass http://$prom/api/v1/query_range$is_args$args; }
```

  The variable in `proxy_pass` defers DNS to request time, so nginx still starts on Compose where no Prometheus exists (the request then returns 502).
  In `Dockerfile`: copy the template to `/etc/nginx/templates/default.conf.template` instead of copying `nginx.conf`, and set `ENV NGINX_RESOLVER=127.0.0.11 UPSTREAM_SUFFIX= PROMETHEUS_ADDR=prometheus:9090`. Compose needs no change beyond confirming the defaults apply; add the three variables to `.env.example` with a one-line comment each.
- [ ] **Step 6: Verify the image by running it.**

```bash
docker build -q -t ticketrush/dashboard:plan dashboard
docker run -d --rm --name dash-plan -p 4299:8080 ticketrush/dashboard:plan
curl -s -o /dev/null -w '%{http_code}\n' localhost:4299/healthz                         # 200
curl -s -o /dev/null -w '%{http_code}\n' -X POST localhost:4299/api/metrics/query        # 403
curl -s -o /dev/null -w '%{http_code}\n' localhost:4299/api/metrics/admin/tsdb           # 404
curl -s -o /dev/null -w '%{http_code}\n' 'localhost:4299/api/metrics/query?query=up'     # 502 (no Prometheus here)
docker stop dash-plan
```

  Note: outside Compose the Docker resolver `127.0.0.11` exists only on user-defined networks; if nginx fails to start for that reason, run the container with `--network` set to a throwaway `docker network create` network and report it.
- [ ] **Step 7: Commit** `feat: proxy Prometheus query endpoints through the dashboard origin`.

---

### Task 2: Metrics layer

**Files:**
- Create: `dashboard/src/app/metrics.ts`
- Test: `dashboard/src/app/metrics.spec.ts`

**Interfaces:**
- Produces:

```ts
export type SourceState = 'loading' | 'live' | 'stale' | 'unavailable';
export interface Sample { t: number; v: number | null }            // t in ms, v null when NaN or absent
export interface Series { labels: Record<string, string>; points: Sample[] }
export const QUERIES: Record<QueryId, string>;                      // exact strings below
export type QueryId = 'ordersRate' | 'sagaOutcomes' | 'lagByGroup' | 'partitionLag'
  | 'redisOps' | 'redisLatency' | 'readyPods' | 'consumerRate';
export function parseVector(body: unknown): Series[];               // instant result, one point per series
export function parseMatrix(body: unknown): Series[];               // range result
@Injectable({ providedIn: 'root' }) export class MetricsStore {
  readonly state: Signal<SourceState>;
  readonly staleSince: Signal<Date | null>;
  series(id: QueryId): Signal<Series[]>;                            // rolling window, max 60 points per series
  start(destroyRef: DestroyRef): void;                              // backfill then poll
}
```

- Exact PromQL (label names verified on the cluster on 2026-11-11):

| QueryId | PromQL |
|---|---|
| `ordersRate` | `sum(rate(ticketrush_orders_total[30s]))` |
| `sagaOutcomes` | `sum by (outcome) (ticketrush_saga_outcomes_total)` (outcomes: `CONFIRMED`, `REJECTED`, `FAILED`) |
| `lagByGroup` | `sum by (consumergroup, topic) (kafka_consumergroup_lag_sum{consumergroup=~"inventory-service\|payment-service",topic=~"orders.requested\|tickets.reserved"})` |
| `partitionLag` | `kafka_consumergroup_lag{consumergroup=~"inventory-service\|payment-service",topic=~"orders.requested\|tickets.reserved"}` (12 partitions per topic) |
| `redisOps` | `sum by (db_operation) (rate(lettuce_seconds_count{db_operation=~"EVALSHA\|HGET"}[30s]))` |
| `redisLatency` | `sum(rate(lettuce_seconds_sum{db_operation="EVALSHA"}[30s])) / sum(rate(lettuce_seconds_count{db_operation="EVALSHA"}[30s]))` (seconds; NaN when idle) |
| `readyPods` | `count by (job) (up{job=~"inventory-service\|payment-service"} == 1)` |
| `consumerRate` | `sum by (consumer, topic) (rate(ticketrush_consumer_processing_seconds_count[30s]))` |

  (The backslashes before `|` above are Markdown table escapes, not part of the query.)

- Behaviour: on `start`, one `query_range` per id over the last 300s with `step=5` (backfill), then every 5000ms one `query` per id, no overlapping polls (`exhaustMap`), 8000ms timeout. State: `loading` until the first round settles; `unavailable` if no round has ever succeeded; `live` after a fully successful round; `stale` when a round fails after a success, with `staleSince` set once and cleared on recovery. Last good series are kept while stale.

- [ ] **Step 1: Write the failing tests** (Vitest, `HttpTestingController`, fake timers as in `app.dashboard.spec.ts`):

```ts
it('parseVector maps NaN and missing values to null', () => {
  const body = { status: 'success', data: { resultType: 'vector', result: [
    { metric: { job: 'payment-service' }, value: [1794400000, 'NaN'] },
    { metric: { job: 'inventory-service' }, value: [1794400000, '2'] }] } };
  expect(parseVector(body)).toEqual([
    { labels: { job: 'payment-service' }, points: [{ t: 1794400000000, v: null }] },
    { labels: { job: 'inventory-service' }, points: [{ t: 1794400000000, v: 2 }] }]);
});
it('parseVector and parseMatrix return [] for an error body, an empty result or a non-object', () => {
  for (const body of [null, 'x', { status: 'error' }, { status: 'success', data: { result: [] } }]) {
    expect(parseVector(body)).toEqual([]); expect(parseMatrix(body)).toEqual([]);
  }
});
it('is unavailable when the first round fails, and never becomes stale without a prior success', ...);
it('is live after a successful round, stale with staleSince after a later failure, live again on recovery', ...);
it('keeps the last good series while stale', ...);
it('polls every 5000 ms and does not start a second round while one is in flight', ...);
it('caps each series at 60 points, dropping the oldest', ...);
it('sends the exact QUERIES strings url-encoded to /api/metrics/query', ...);
```

- [ ] **Step 2: Run** `cd dashboard && npx ng test --watch=false`. Expected: FAIL, `metrics.ts` not found.
- [ ] **Step 3: Implement `metrics.ts`** to the interface above.
- [ ] **Step 4: Run** the tests. Expected: PASS, and the existing specs still pass.
- [ ] **Step 5: Commit** `feat: add Prometheus metrics store with per-source stale and unavailable states`.

---

### Task 3: Time-series chart

**Files:**
- Create: `dashboard/src/app/chart.ts`
- Test: `dashboard/src/app/chart.spec.ts`

**Interfaces:**
- Consumes: `Series`, `Sample` from Task 2.
- Produces:

```ts
export function scalePoints(points: Sample[], width: number, height: number, yMax: number): [number, number][];
export function linePath(xy: [number, number][]): string;           // 'M x y L x y ...', '' when fewer than 2 points
export function niceMax(values: (number | null)[], floor: number): number; // max rounded up, never below floor, never 0
@Component({ selector: 'tr-chart' }) export class TimeSeriesChart {
  series = input.required<{ name: string; emphasis: boolean; points: Sample[] }[]>();
  threshold = input<number | null>(null);    // dashed horizontal line with its value as label
  yFloor = input(1);                         // minimum y-axis max
  area = input(false);                       // fill under the emphasised series
  unit = input('');
}
```

- Rendering: one inline SVG with `viewBox`, `width: 100%`, height 200px. Emphasised series stroke `--accent` 1.5px, others `--fg-3` 1px. A 4px accent circle on the newest point of the emphasised series. Null points break the line. Y axis: 0 and max labels in 12px mono; x axis: "-5m" and "now". When every point is null or there are fewer than 2 points, render the ground and the text "No data yet" and no path. On a new sample the path group slides left by one step over 600ms using `transform`; under reduced motion it redraws in place.

- [ ] **Step 1: Write the failing tests:**

```ts
it('scalePoints maps the y range to the box with 0 at the bottom', () => {
  expect(scalePoints([{ t: 0, v: 0 }, { t: 10, v: 5 }, { t: 20, v: 10 }], 200, 100, 10))
    .toEqual([[0, 100], [100, 50], [200, 0]]);
});
it('scalePoints skips null values', ...);                 // returns only the non-null points
it('niceMax never returns 0: all-zero and empty input give the floor', () => {
  expect(niceMax([0, 0, 0], 1)).toBe(1); expect(niceMax([], 10)).toBe(10); expect(niceMax([null], 1)).toBe(1);
});
it('linePath is empty for 0 or 1 points and M/L for more', ...);
it('renders "No data yet" and no path when all points are null', ...);
it('draws a flat line on the floor when stock is 0 throughout', ...);   // y equals height for every point
it('renders the threshold line and its label when threshold is set', ...);
it('marks exactly one series as emphasised with the accent stroke', ...);
```

- [ ] **Step 2: Run** the tests. Expected: FAIL, `chart.ts` not found.
- [ ] **Step 3: Implement `chart.ts`.**
- [ ] **Step 4: Run** the tests. Expected: PASS.
- [ ] **Step 5: Commit** `feat: add SVG time-series chart component`.

---

### Task 4: Tokens, type and page skeleton

**Files:**
- Modify: `dashboard/src/styles.css`, `dashboard/src/index.html`, `dashboard/src/app/app.html`, `dashboard/src/app/app.css`, `dashboard/src/app/app.spec.ts`
- Create: `dashboard/public/fonts/Geist-Variable.woff2`, `dashboard/public/fonts/GeistMono-Variable.woff2`, `dashboard/public/fonts/OFL.txt`

**Interfaces:**
- Produces: CSS custom properties exactly as named in DESIGN.md section 4 and section 8 (`--bg`, `--surface`, `--raised`, `--hairline`, `--fg`, `--fg-2`, `--fg-3`, `--accent`, `--ok`, `--warn`, `--bad`, `--ease-out`, `--t-fast`, `--t-move`), plus `--font-sans` and `--font-mono`. Page regions with these class names for later tasks: `.topbar`, `.map-region`, `.chapters`, `.rail`, `.orders`.

- [ ] **Step 1: Get the fonts.** The owner approves this download first (it is the one new asset in the spec). Take the two variable woff2 files and the OFL license from the official `geist` npm package tarball (`npm pack geist` in a scratch directory outside the repo, then copy the three files); do not add `geist` to `package.json`. Record the package version in the commit body and in the DESIGN.md dependency table (Task 8).
- [ ] **Step 2: Update the heading test** in `app.spec.ts`: the page `h1` text is `One order, from click to confirmation.`. Run the tests. Expected: FAIL on that test only.
- [ ] **Step 3: Replace the tokens** in `styles.css` with the DESIGN.md values, add two `@font-face` rules (`font-display: swap`, `font-weight: 100 900`), body 15px/24px `--font-sans`, `font-variant-numeric: tabular-nums` on `.mono`, and one `@media (prefers-reduced-motion: reduce)` rule that sets `--t-fast` and `--t-move` to 0ms. Remove the old blue-tinted tokens and every opaque panel border in `app.css`.
- [ ] **Step 4: Restructure `app.html`** into the five regions of DESIGN.md section 2: top bar (brand, live/stale indicator reusing the existing `stale()` logic, the text `kind-ticketrush` is NOT hard-coded: show `metrics every 5s` and the existing polling text only), map region with the `h1` and an empty placeholder, a two-column grid (`.chapters` about 2/3, `.rail` 360px sticky), then `.orders`. Move the existing buy form into `.rail` and the existing table into `.orders` without changing their bindings, ids or copy. The old Stock and Orders-per-second panels are removed here; their values reappear in Task 5 (keep ids `stock-value` and `rate-value` on the new figures so existing tests keep working, and until Task 5 lands keep them as two plain figures at the top of `.chapters`).
- [ ] **Step 5: Run** `npx ng test --watch=false` and `npm run build`. Expected: all PASS, build succeeds, no budget error.
- [ ] **Step 6: Look at it.** `npm start`, open at 1440px. Check: Geist loads from `/fonts/` (network tab, no request to any other host), no horizontal scroll, hairlines only.
- [ ] **Step 7: Commit** `feat: apply control-room tokens, self-hosted Geist and page skeleton`.

---

### Task 5: The four chapters

**Files:**
- Create: `dashboard/src/app/chapter.ts`, `dashboard/src/app/chapter.css`
- Modify: `dashboard/src/app/app.ts`, `dashboard/src/app/app.html`, `dashboard/src/app/app.css`
- Test: `dashboard/src/app/chapter.spec.ts`, additions to `dashboard/src/app/app.dashboard.spec.ts`

**Interfaces:**
- Consumes: `MetricsStore`, `SourceState`, `Series` (Task 2); `TimeSeriesChart` (Task 3); `stock`, `stockStale`, `activity` signals already in `App`.
- Produces: `@Component({ selector: 'tr-chapter' }) Chapter` with inputs `number: string`, `title: string`, `claim: string`, `mechanism: string`, `state: SourceState`, `staleSince: Date | null`, and two content slots `[chart]` and `[figure]`. In `App`: `stockHistory: Signal<Sample[]>` (one sample per successful stock poll, max 150 = 5 minutes, reset on event change).

- Content, exact copy:

| # | Title | Claim | Chart | Key figure | Mechanism line |
|---|---|---|---|---|---|
| 01 | CONCURRENCY | Many buyers, few tickets. Stock never goes below zero. | `stockHistory`, area, emphasised, `yFloor` = first stock seen | stock (`#stock-value`), then mono rows: confirmed, rejected, failed from `sagaOutcomes`, and orders/s (`#rate-value`, existing `rate()`) | Atomic reservation in Redis; duplicate orders return their stored outcome. |
| 02 | KAFKA | Work waits in topics. Consumers drain it, in order per order. | `lagByGroup`: `inventory-service / orders.requested` emphasised, `payment-service / tickets.reserved` grey; under it one row of 12 squares per topic from `partitionLag` (opacity by lag, 0 lag = `--raised`) | total lag, label "messages behind" | key = orderId; offsets committed after processing. |
| 03 | REDIS | One atomic step: check stock, decrement, remember the order. | `redisOps`: `EVALSHA` emphasised, `HGET` grey, unit "cmd/s" | `redisLatency` in ms with one decimal, label "per reservation" | Lua script in inventory-service; Redis is the stock counter and the idempotency record. |
| 04 | KUBERNETES | Lag passes 10. A second consumer appears. | `lagByGroup` for inventory-service, `threshold` = 10 | ready pods as squares: `inventory-service n of 2`, `payment-service n of 2` from `readyPods`; a square that appeared since the previous poll gets an accent outline for 600ms | KEDA, Kafka lag trigger, 1 to 2 replicas. |

  The Lua snippet in chapter 03 is not invented: quote at most 5 lines from the real reserve script in `inventory-service/src/main/java/com/ticketrush/inventory/InventoryRepository.java` (or the resource it loads), trimmed, unchanged.
  The number 2 in "n of 2" and the threshold 10 mirror `autoscaling.maxReplicas` and `autoscaling.lagThreshold` in `values.yaml`; define them once as constants in `app.ts` with a comment naming that file.

- State line per chapter (DESIGN.md section 9): `loading` shows a dash in the figure; `unavailable` replaces chart and figure with the single line "Live metrics need the kind stack."; `stale` keeps the data and adds "stale since HH:mm:ss" in `--warn`. Chapter 01 uses the existing stock/activity stale flags for its chart and stock figure, and the metrics state only for its outcome rows. When `ordersRate` is 0 for the whole window, chapters 02-04 add: "No orders in the last 5 minutes. Start load: scripts/autoscale-acceptance.sh".
- A figure that changed since the previous poll gets the class `changed` for 600ms (accent color, no counting animation).

- [ ] **Step 1: Write the failing tests:**

```ts
// chapter.spec.ts
it('unavailable renders only "Live metrics need the kind stack." and projects no chart or figure', ...);
it('stale keeps projected content and shows "stale since 14:03:07"', ...);
it('loading shows a dash in the figure slot fallback', ...);
// app.dashboard.spec.ts additions
it('metrics failing from the start leaves stock, Buy and the orders table working', ...);  // flush stock+activity OK, fail every /api/metrics request; #stock-value shows the stock, table rows render, chapters 02-04 show the unavailable line
it('NaN redis latency renders a dash, never "NaN"', ...);
it('stock history resets when the event id changes', ...);
it('renders ready pods as "1 of 2" for payment-service and "2 of 2" for inventory-service', ...);
it('total lag sums both consumer groups', ...);
```

- [ ] **Step 2: Run** the tests. Expected: FAIL.
- [ ] **Step 3: Implement** `Chapter`, wire the four chapters in `app.html`, call `metrics.start(destroyRef)` in `ngOnInit`, add `stockHistory`.
- [ ] **Step 4: Run** `npx ng test --watch=false`. Expected: all PASS including every pre-existing `AC5`/`AC6` test.
- [ ] **Step 5: Commit** `feat: add concurrency, Kafka, Redis and Kubernetes chapters on live metrics`.

---

### Task 6: System map

**Files:**
- Create: `dashboard/src/app/flow.ts`, `dashboard/src/app/system-map.ts`, `dashboard/src/app/system-map.css`
- Modify: `dashboard/src/app/app.html`, `dashboard/src/app/app.ts`
- Test: `dashboard/src/app/flow.spec.ts`, `dashboard/src/app/system-map.spec.ts`

**Interfaces:**
- Consumes: `activity` signal and the `orderId` of the last accepted purchase from `App` (add `lastOrderId: Signal<string | null>`, set where the 202 response is handled); `consumerRate`, `ordersRate` from `MetricsStore`.
- Produces:

```ts
export type Stage = 'gateway' | 'inventory' | 'payment' | 'order' | 'notification' | 'rejected';
export const PATH_FOR_STATUS: Record<string, Stage[]>;
export interface Dot { id: string; orderId: string; path: Stage[]; mine: boolean }
export function transitions(prev: RecentOrder[] | null, next: RecentOrder[], mine: string | null): Dot[];
export function capDots(active: Dot[], incoming: Dot[], max: number): Dot[];
```

- Status to path (statuses in the order API: `PENDING`, `RESERVED`, `CONFIRMED`, `REJECTED`, `PAYMENT_FAILED`, `FAILED`, `TIMEOUT`): a dot is emitted when an order first appears or its status changes, and travels the segment from where its previous status left it to where the new one puts it: `PENDING` ends at inventory, `RESERVED` at payment, `CONFIRMED` at notification, `REJECTED` leaves the main path at inventory through `orders.rejected`, `PAYMENT_FAILED`/`FAILED`/`TIMEOUT` stop at the order node drawn in `--bad`. An order first seen already `CONFIRMED` travels the whole path. `transitions(null, next)` (first poll) returns `[]` so a page load does not replay history.
- `capDots`: never more than `max` (40) dots; `mine` dots are never dropped; oldest non-mine dots are dropped first.
- Map layout (SVG, about 420px tall, full width): nodes `buyer`, `api-gateway`, `inventory-service`, `payment-service`, `order-service`, `notification-service` left to right, at least 120x56px, radius 6; topic pills on the edges with the real names `orders.requested`, `tickets.reserved`, `payments.result`, `orders.confirmed`, and `orders.rejected` leaving inventory upward; `Redis` under inventory, `fake provider` under payment, `Postgres` under order, joined by dashed hairlines. Each consumer node shows its live rate from `consumerRate` in mono ("12.4 /s", dash when unknown); the gateway shows `ordersRate`. Dots are 6px circles, `--fg-3`, or `--accent` with a 3-dot fading trail when `mine`; 600ms per edge with `--ease-out`; removed from the DOM when they arrive. Under reduced motion no dot is drawn; the destination node's border flashes once instead.
- Legend under the map: "Background orders" and "Your order".

- [ ] **Step 1: Write the failing tests:**

```ts
it('first poll emits no dots', () => expect(transitions(null, [order('a', 'CONFIRMED')], null)).toEqual([]));
it('a new PENDING order travels gateway to inventory', ...);
it('PENDING to RESERVED travels inventory to payment', ...);
it('RESERVED to CONFIRMED travels payment, order, notification', ...);
it('a new order already CONFIRMED travels the whole path', ...);
it('REJECTED exits at inventory through the rejected stage', ...);
it('an unchanged order emits nothing', ...);
it('an unknown status emits nothing and does not throw', ...);
it('marks only the order matching mine', ...);
it('capDots keeps at most 40, never drops a mine dot, drops the oldest others first', ...);
it('system map shows a dash, not 0, for a consumer with no rate series', ...);
it('renders the five real topic names', ...);
```

- [ ] **Step 2: Run** the tests. Expected: FAIL.
- [ ] **Step 3: Implement** `flow.ts`, then `system-map.ts`, and place `<tr-system-map>` in `.map-region`.
- [ ] **Step 4: Run** `npx ng test --watch=false` and `npm run build`. Expected: PASS.
- [ ] **Step 5: Commit** `feat: add live system map with per-order travelling dots`.

---

### Task 7: Dashboard on kind

**Files:**
- Create: `deploy/helm/ticketrush/templates/dashboard.yaml`
- Modify: `deploy/helm/ticketrush/values.yaml`, `scripts/observability-up.sh`, `scripts/base-chart-check.sh`

**Interfaces:**
- Produces: Service `dashboard` port 8080 in namespace `ticketrush`; values key:

```yaml
dashboard:
  enabled: true
  tag: dev
  resources:
    requests: {cpu: 20m, memory: 32Mi}
    limits: {cpu: 200m, memory: 64Mi}
```

- Deployment: 1 replica, image `{{ .Values.image.repository }}/dashboard:{{ .Values.dashboard.tag }}`, `runAsNonRoot`, `runAsUser: 101`, seccomp `RuntimeDefault`, `automountServiceAccountToken: false`, readiness and liveness on `/healthz`, env `NGINX_RESOLVER=kube-dns.kube-system.svc.cluster.local`, `UPSTREAM_SUFFIX=.{{ .Release.Namespace }}.svc.cluster.local`, `PROMETHEUS_ADDR=prometheus.{{ .Release.Namespace }}.svc.cluster.local:9090`. Labels follow `apps.yaml` (`app.kubernetes.io/name: dashboard`). No ServiceMonitor (nginx has no metrics endpoint), so the dashboard must not appear in `readyPods`.
- `observability-up.sh`: the dashboard builds from its own context (`docker build ... dashboard`), unlike the Java services which build from the repo root; add a dedicated build-and-load step that reuses the image-id-as-tag scheme and passes `--set-string dashboard.tag=<tag>` to the existing `helm upgrade`.

- [ ] **Step 1: Add a failing chart check** to `scripts/base-chart-check.sh`, in its existing style: the rendered chart contains a Deployment and a Service named `dashboard`, the container runs as non-root uid 101, and the three env vars are set. Run `scripts/base-chart-check.sh`. Expected: FAIL on the new check.
- [ ] **Step 2: Write `dashboard.yaml` and the values block.** Run `scripts/base-chart-check.sh` and `scripts/observability-chart-check.sh`. Expected: both PASS.
- [ ] **Step 3: Update `observability-up.sh`** and run it against the existing cluster (it is idempotent; it does not recreate the cluster). Expected: `kubectl --context kind-ticketrush -n ticketrush rollout status deploy/dashboard` succeeds.
- [ ] **Step 4: Verify by request.**

```bash
kubectl --context kind-ticketrush -n ticketrush port-forward svc/dashboard 4200:8080 &
curl -s 'localhost:4200/api/metrics/query?query=up' | head -c 200          # {"status":"success",...
curl -s localhost:4200/api/events/concert-1/stock                           # {"eventId":"concert-1","stock":...}
curl -s localhost:4200/api/orders/activity | head -c 100                    # {"windowSeconds":10,...
```

- [ ] **Step 5: Commit** `feat: deploy the dashboard on kind with the Helm chart`.

---

### Task 8: Acceptance script and records

**Files:**
- Create: `scripts/live-dashboard-acceptance.sh`
- Modify: `docs/internal/SPEC.md` (Approved deviations), `docs/DESIGN.md` (decision log, dependency table, repo layout, nginx route list in the M7 section), `docs/internal/PROGRESS.md`, `AGENTS.md` (commands table), `docs/DEMO.md`, `dashboard/README.md`, `.env.example`

**Interfaces:**
- Consumes: everything above. Reuses `scripts/autoscale-load.js` for load.
- Produces: `scripts/live-dashboard-acceptance.sh`, exit 0 only if every check passes, one `PASS`/`FAIL` line per check, in the style of `scripts/dashboard-acceptance.sh`.

- Checks, in order:
  1. `LD1_unit`: dashboard `npm ci`, tests and production build in the pinned Node image (same invocation as step 3 of `dashboard-acceptance.sh`).
  2. `LD2_deployed`: `deploy/dashboard` rolled out on `kind-ticketrush`; port-forward to a free local port.
  3. `LD3_proxy_boundaries`: through the dashboard origin, `POST /api/metrics/query` is 403; `GET /api/metrics/admin/tsdb`, `/api/metrics/`, `/api/metrics/querymore` are 404; `/actuator/prometheus` is 404.
  4. `LD4_live_queries`: start a k6 load with `scripts/autoscale-load.js` for 60s; while it runs, each of the eight `QUERIES` strings (the script reads them from `dashboard/src/app/metrics.ts` so the two cannot drift) returns `status: success` with a non-empty result through `/api/metrics/query`; `ordersRate` is greater than 0; `readyPods` reports at least 1 for both services.
  5. `LD5_reads`: `/api/events/concert-1/stock` returns a stock of 0 or more, `/api/orders/activity` returns 200.
  6. `LD6_invariant`: after the load, stock is never negative and confirmed tickets do not exceed the seeded stock, using the same audit queries as `scripts/autoscale-acceptance.sh`.
  7. `LD7_compose_still_passes`: only with `--with-compose`; runs `scripts/dashboard-acceptance.sh`. It is separate because it resets the Compose Postgres volume.
- The script cleans up its port-forward and k6 process on exit and never changes the kubectl current context.

- [ ] **Step 1: Write the script** with all checks present and run it. Tasks 1 to 7 are done, so it should pass. To prove it can fail, run it once with `PROMETHEUS_ADDR` overridden to a wrong address on the Deployment (`kubectl set env`), confirm `LD4_live_queries` FAILS, then restore the value and confirm PASS. Paste both outputs in PROGRESS.md.
- [ ] **Step 2: Run** `scripts/live-dashboard-acceptance.sh --with-compose`. Expected: every line PASS.
- [ ] **Step 3: Visual check at 1440px** against the mockup and DESIGN.md, on kind under load: live, idle, stale (delete the Prometheus pod, wait, watch it recover), reduced motion (browser emulation), and on Compose the "needs the kind stack" state. Run the audit script from the design workflow and record fonts, sizes, radii, borders, shadows. Record what was not seen.
- [ ] **Step 4: Update the records.**
  - `SPEC.md`, Approved deviations: M8 widened on 2026-11-11 to include the live explanatory dashboard, by owner decision.
  - `docs/DESIGN.md`: decision log rows for (a) Prometheus proxied through the dashboard origin, GET only, two paths; (b) dashboard in the Helm chart; (c) pod counts from `up` because kube-state-metrics is disabled; (d) no charting library. Dependency table: Geist and Geist Mono woff2, version, OFL, self-hosted. Update the M7 route list and the repo layout.
  - `AGENTS.md` commands table: add the new acceptance command; keep the M7 line.
  - `PROGRESS.md`: M8 status, the exact command and its real output, and what is NOT done or not verified.
  - `docs/DEMO.md`: new script for a kind demo (start stack, port-forward 4200, start load, what the viewer sees per chapter). State that the existing GIF shows the old dashboard and must be re-recorded by the owner.
  - `dashboard/README.md`: how to run against kind and what Compose shows.
- [ ] **Step 5: Commit** `test: add live dashboard acceptance script` and `docs: record the live dashboard decisions and M8 scope`.
- [ ] **Step 6: Stop.** Report to the owner with the real output. Do not push, merge or start anything else.

---

## Not in this plan

- Re-recording the demo GIF (owner, after visual approval).
- Raising KEDA `maxReplicas` above 2.
- Any mobile or light-theme work.
