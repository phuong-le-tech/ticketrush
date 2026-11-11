# Dashboard

Angular 22.2.5 standalone page following the approved Open Design reference in `docs/design/dashboard/`.
Live stock, orders per second and recent saga states poll independently every 2 seconds.
The native form creates a new key per accepted Buy intent; Retry preserves the full failed purchase.
Angular packages and CLI/build tools are pinned to 22.2.5; the lockfile resolves default Vitest to 5.1.0.

## Commands

Run from the repository root. Every npm/npx command must run in the approved Node image, not host Node.

```sh
docker info
docker run --rm -e NG_CLI_ANALYTICS=false -v "$PWD/dashboard":/app -w /app node:24.22.2 sh -c 'npm ci && npx tsc -p tsconfig.spec.json --noEmit && npm test -- --reporters=verbose'
docker run --rm -e NG_CLI_ANALYTICS=false -v "$PWD/dashboard":/app -w /app node:24.22.2 npm run build
```

`npm test` runs once (`ng test --watch=false`) plus the dependency-free Node proxy-routing test.
The default Angular Vitest builder and jsdom are unchanged.
Tests use Angular HttpTestingController: no real network, fixed time, fake timers and deterministic UUIDs.
`npx prettier --check src proxy.conf.json proxy.test.cjs package.json` checks dashboard formatting.
There is no configured ESLint target; strict TypeScript and Angular production compilation are the type checks.

## Serving and local development

- `docker compose -f deploy/compose/docker-compose.yml up -d --build` from the repo root serves the
  production dashboard at http://localhost:4200. The healthcheck covers nginx, not backend readiness.
- nginx proxies only POST `/api/orders`, GET `/api/orders/activity` and GET `/api/events/` to their
  owning services. No Actuator proxy, CORS, authentication or router. Node is absent from the final image.
- `cd dashboard && npx ng serve --proxy-config proxy.conf.json` requires host Node >=24.15, or run
  the same command in the approved Node container. Do not upgrade host Node as part of this work.
  The dev proxy expects host-running gateway/inventory/order at localhost 8080/8081/8084, or explicit
  port forwards to those services. Compose does not publish inventory/order ports. In a Node container,
  localhost means that container; a reachable backend network/forward must be arranged separately.
- Quantity is an integer 1..4. Customer identity persists in localStorage as `web-<uuid>`.
  Browser cryptography uses randomUUID or a getRandomValues v4 fallback. Storage/cryptography failure
  blocks purchasing with a visible message instead of silently using a transient identity.
- Retry intent survives network/5xx/429 failures and the 15-second request timeout, in memory only.
  Reloading while acceptance is uncertain discards that key; finish Retry before reloading.
  Other 4xx responses unlock the inputs for correction. A 202 is accepted, never shown as confirmed.
- Reads time out after 8 seconds, retain last-good data with a source-specific stale marker and continue.
  No overlapping reads per source; changing event cancels the old stock read and clears its display.
- Money is displayed from integer API cents and currency, with no invented stock price. Amounts outside
  JavaScript safe-integer precision are labeled unavailable rather than displaying rounded financial data.

## Status values and acceptance

- The saga table renders the order model as returned by `GET /orders/activity`:
  `PENDING`, `RESERVED`, `CONFIRMED`, `FAILED`, `REJECTED`. `PAYMENT_FAILED` from the design
  mock is illustrative only and is never produced by the API; a failure carries its cause in
  `failureReason` (for example `TIMEOUT` or `PAYMENT_FAILED`).
- The stock endpoint returns `eventId` and `stock` only, so the page never shows a price and
  keeps amounts as integer cents plus currency in the order table.
- One command proves the whole dashboard end to end: `scripts/dashboard-acceptance.sh` runs the backend
  module tests, the dashboard container tests and build, brings Compose up and drives
  `/healthz`, the stock read, a real `POST /api/orders`, the CONFIRMED saga row and the stock
  decrease through http://localhost:4200. Real output is recorded in `docs/internal/PROGRESS.md`.

## Implementation verification

25 Vitest cases pass: the original 16 plus 9 boundary/safety cases in `app.safety.spec.ts`.
These add customer reload persistence, non-secure UUID fallback, invalid quantities, locked retry payload,
independent source failure and cancellation on event change. The Node proxy test independently verifies
Angular's loaded proxy configuration with Vite's first-match semantics, including query strings and private-path exclusion.
The generated heading fixture now provides HTTP testing and flushes initialization requests; its heading
assertion remains unchanged. The main `app.dashboard.spec.ts` assertions are unchanged.

Container production build, strict test TypeScript check, image build, nginx configuration test and isolated
non-root image health/static-route smoke checks passed in the implementation run. Full Compose purchase
acceptance, live screenshot and browser/mobile accessibility verification remain the downstream acceptance
card's job, not proven by these tests.

Fresh root `mvn verify` with Java 21.0.10 in this implementation run exited 0:
`BUILD SUCCESS`, `Total time: 14:48 min`. All 46 Surefire/Failsafe XML reports aggregate to
239 tests, zero failures, errors or skips. This includes contention and replay once, not the oversell ten-run command.
The final clean Node-container command (`npm ci`, Prettier check, test TypeScript check, `npm test`,
production build) exited 0; npm audited 296 packages with zero vulnerabilities. npm warned about four
scaffold dependency install scripts without allowScripts coverage; no script approval or package upgrade was made.

## Historical RED handoff

### Cases and observed RED output

There was no frontend test plan in this card's parent handoffs. These cases are derived directly from AC5,
AC6, decisions 1-4, and the approved reference under `docs/design/dashboard/`.

| AC | Test name | Observed failure |
|---|---|---|
| AC5 | sends distinct UUID Idempotency-Keys for two purchases separated by 202 | Buy button: expected null to be an instance of HTMLButtonElement |
| AC5 | Retry preserves the failed purchase key after network | Buy button: expected null to be an instance of HTMLButtonElement |
| AC5 | Retry preserves the failed purchase key after 500 | Buy button: expected null to be an instance of HTMLButtonElement |
| AC5 | Retry preserves the failed purchase key after 503 | Buy button: expected null to be an instance of HTMLButtonElement |
| AC5 | Retry preserves the failed purchase key after 429 | Buy button: expected null to be an instance of HTMLButtonElement |
| AC5 | disables Buy while its POST is pending and prevents a second submission | Buy button: expected null to be an instance of HTMLButtonElement |
| AC6 | renders the exact stock returned by the same-origin stock API | Expected GET /api/events/concert-1/stock, found none |
| AC6 | renders 32 / 10 orders per second as 3.2 | Expected GET /api/events/concert-1/stock, found none |
| AC6 | renders 0 / 10 orders per second as 0.0 | Expected GET /api/events/concert-1/stock, found none |
| AC6 | renders 7 / 4 orders per second as 1.8 | Expected GET /api/events/concert-1/stock, found none |
| AC6 | renders API saga rows in response order with real statuses and failure reason | Expected GET /api/events/concert-1/stock, found none |
| AC6 | polls both read APIs at 2000 ms intervals, not earlier | Expected GET /api/events/concert-1/stock, found none |
| AC6 | marks a failed stock poll stale and continues polling to recovery | Expected GET /api/events/concert-1/stock, found none |
| AC6 | marks a failed activity poll stale and continues polling to recovery | Expected GET /api/events/concert-1/stock, found none |
| AC6 | renders the approved dashboard heading | expected 'Hello, dashboard' to be 'Ticket operations' |

Final Vitest result: 16 tests, 15 failed, 1 passed. TypeScript test typecheck and the scaffold production build
both passed. The unmodified generated creation smoke test passes. The generated title assertion was replaced
with the real approved heading contract before implementation, not removed to obtain a green run.

### Implementation contract and limits (historical RED stage)

- `src/app/app.dashboard.spec.ts` tests public DOM interactions and HTTP only, not component internals.
- Keep reference ids `buy-button`, `stock-value`, `rate-value`, and a semantic `tbody` for testable zones.
- `src/app/app.spec.ts` carries the approved `Ticket operations` heading and creation smoke test.
- Stock reads use `/api/events/concert-1/stock`. Activity reads use `/api/orders/activity`, either without
  a limit (server default 20) or with explicit `limit=20`. POST is `/api/orders`.
- Initial reads happen on component initialization. Timers are checked at 1999 ms, 2000 ms and 4000 ms.
  Each source's failure must show stale, keep the last stock, and recover on the next successful poll.
- The rate table includes a synthetic four-second window to detect a hardcoded divisor, while production
  currently returns ten seconds. All other fixtures use the actual ten-second window.
- HTTP 202 is acceptance, not confirmation. Tests use actual API statuses `FAILED` and `RESERVED`, not the
  illustrative `PAYMENT_FAILED` design label. Do not invent a unit price from the stock endpoint.
- Failure assertions stop at missing UI/requests today. Deeper retry, response rendering and timer assertions
  will become reachable during implementation; no claim of GREEN behavior is made.
- Customer persistence across reloads, non-secure UUID fallback, quantity boundary validation, mobile layout,
  browser accessibility and deployment are not covered by these two AC-focused test groups.
- Backend AC1-AC4 and regression AC9 are separate Java tests. AC7 Compose acceptance and AC8 non-root image
  plus live screenshot remain downstream. No Java, deployment or design reference files changed here.
- No hand-written production skeletons or logic were added; only the CLI-generated component/bootstrap.
