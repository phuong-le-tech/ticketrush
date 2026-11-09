# M7 dashboard frontend RED handoff

Angular CLI 22.2.5 scaffold, one standalone component, no router configuration or auth.
The application still renders the generated placeholder. No purchase or polling logic is implemented.
Angular packages and CLI/build tools are pinned to 22.2.5; the lockfile resolves default Vitest to 5.1.0.

## Commands

Run from the repository root. Every npm/npx command must run in the approved Node image, not host Node.

```sh
docker info
docker run --rm -e NG_CLI_ANALYTICS=false -v "$PWD/dashboard":/app -w /app node:24.22.2 sh -c 'npm ci && npx tsc -p tsconfig.spec.json --noEmit && npm test -- --reporters=verbose'
docker run --rm -e NG_CLI_ANALYTICS=false -v "$PWD/dashboard":/app -w /app node:24.22.2 npm run build
```

`npm test` runs once (`ng test --watch=false`). The default Angular Vitest builder and jsdom are unchanged.
Tests use Angular HttpTestingController: no real network, fixed time, fake timers and deterministic UUIDs.

## Cases and observed RED output

There was no frontend test plan in this card's parent handoffs. These cases are derived directly from AC5,
AC6, decisions 1-4, and the approved reference under `docs/design/m7/`.

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

## Implementation contract and limits

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
