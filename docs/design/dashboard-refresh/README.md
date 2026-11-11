# Dashboard refresh design reference

Status: awaiting Phuong's approval on card `t_1cb9795e`. No Angular implementation is authorized by this reference yet.

## View

- Open [index.html](index.html) directly in a browser. It is self-contained, has no network access, and makes no real reservation or payment.
- [dashboard.png](dashboard.png) is the unedited Open Design renderer export: 2880 x 2504 pixels, with a 1440px CSS desktop layout.
- The existing [running dashboard screenshot](../dashboard/dashboard-running.png) is the before image. The live after screenshot belongs to the later acceptance card, not this illustrative reference.
- The four zones are stock, throughput, buy/retry, and recent orders. The initial snapshot shows stock 248, rate 3.2, and five sample statuses: PENDING, RESERVED, CONFIRMED, FAILED (PAYMENT_FAILED), and REJECTED.
- Use the demo-state selector for stale, loading, empty, error and ready states. Buy simulates a failure; Retry acknowledges the same illustrative request.
- Sparklines begin empty and collect up to 30 successful illustrative polls. Their label is "sampled by this page". They are not server history.
- The existing dark tokens are retained. Short order ids, tabular monospace data, quieter badges and dividers, and a wider table improve scanning without a new design system.

## 21st catalog provenance

The prior attempt completed these four searches, each with `--limit 8 --json`:

```sh
21st search "KPI stat card sparkline" --limit 8 --json
21st search "live status indicator pulse" --limit 8 --json
21st search "data table status badge" --limit 8 --json
21st search "dashboard dark minimal" --limit 8 --json
```

The three retrieved candidates were:

| Retrieval | Candidate | Author | Pattern passed into Open Design |
|---|---|---|---|
| `21st get 34511` | Sparkline | `appica-dev` | Compact inline SVG trend beside a tabular KPI, without a chart library |
| `21st get 25394` | Status | `diceui` | Quiet bordered semantic pill and transform/opacity polling pulse |
| `21st get 22174` | Card Table | `felipemenezes098` | Card header hierarchy, restrained row dividers, status badges and right-aligned amounts |

The current run rechecked `21st whoami` (Logged in as phuongle.tech) and `21st usage` (paid tier, unlimited search and component-code retrieval). No additional retrieval, `add`, React code installation, npm dependency or 21st AI generation was used. These are design patterns, adapted by Open Design to the repository tokens.

## Open Design provenance

- Project: `ticketrush-dashboard-refresh`, name `TicketRush dashboard refresh`.
- Skill: `frontend-design`.
- Design system: `dashboard`, confirmed in the run result.
- Conversation: `096cf6da-df47-4301-98a5-fa8ebce6b5c2`.
- Run: `f067405b-aced-4c0a-8048-a764be153bed`.
- Request: `8b7b488f-59f9-4ced-9c13-8707233b8af3`.
- Result: `succeeded`, `deliverableValid: true`, syntax validation `pass`, no diagnostics or finalizer patches, artifacts `index.html` and `dashboard.png`.
- The exact submitted arguments are in [generation-request.json](generation-request.json). Its prompt was compared with the run state's `userPrompt` using `cmp` and `jq`; they match.
- Neither the HTML nor the PNG was hand-made or hand-edited. Both were copied from Open Design's project and compared byte-for-byte with `cmp`.

### Exact MCP call sequence

`get_run` was repeated while generation was running. The two resource reads and project creation were issued together after discovery.

```text
mcp__open_design__list_projects({})
mcp__open_design__list_resources({})
mcp__open_design__read_resource({"uri":"od://skills/frontend-design/SKILL.md"})
mcp__open_design__read_resource({"uri":"od://design-systems/dashboard/DESIGN.md"})
mcp__open_design__create_project({"designSystem":"dashboard","id":"ticketrush-dashboard-refresh","name":"TicketRush dashboard refresh","skill":"frontend-design"})
mcp__open_design__start_run(<exact arguments in generation-request.json>)
mcp__open_design__get_run({"runId":"f067405b-aced-4c0a-8048-a764be153bed"})
mcp__open_design__get_artifact({"entry":"index.html","include":"all","project":"ticketrush-dashboard-refresh"})
```

Open Design's own renderer produced the PNG inside its generation run:

```sh
"$OD_NODE_BIN" "$OD_BIN" export index.html --project "$OD_PROJECT_ID" --format image --out dashboard.png
```

Reported renderer output:

```text
wrote dashboard.png (361878 bytes)
```

The exports were copied unedited from:

```text
/Users/phuong/Library/Application Support/Open Design/namespaces/release-stable/data/projects/ticketrush-dashboard-refresh/index.html
/Users/phuong/Library/Application Support/Open Design/namespaces/release-stable/data/projects/ticketrush-dashboard-refresh/dashboard.png
```

The session-specific preview at generation time was:
`http://127.0.0.1:50364/api/projects/ticketrush-dashboard-refresh/raw/index.html`.
Refresh session URLs with `get_run`; the project and entry filename are the durable identity.

## Verification in this design-card run

From the repository root:

```sh
cmp '/Users/phuong/Library/Application Support/Open Design/namespaces/release-stable/data/projects/ticketrush-dashboard-refresh/index.html' docs/design/dashboard-refresh/index.html
cmp '/Users/phuong/Library/Application Support/Open Design/namespaces/release-stable/data/projects/ticketrush-dashboard-refresh/dashboard.png' docs/design/dashboard-refresh/dashboard.png
node docs/design/dashboard/verify-reference.cjs docs/design/dashboard-refresh/index.html
shasum -a 256 docs/design/dashboard-refresh/index.html docs/design/dashboard-refresh/dashboard.png
file docs/design/dashboard-refresh/dashboard.png
cmp <(jq -r .prompt docs/design/dashboard-refresh/generation-request.json) <(jq -r .userPrompt '/Users/phuong/Library/Application Support/Open Design/namespaces/release-stable/data/runs/f067405b-aced-4c0a-8048-a764be153bed/state.json')
git diff --check -- docs/design/dashboard-refresh
```

Actual unchanged gate output:

```text
PASS: four zones, no network/private fields/forbidden dashes, rate formula, 2s cadence, pending guard, retry preservation, quantity validation, stale/loading/empty/error states.
Scope: isolated demo JavaScript checks, not real browser or backend integration.
```

SHA-256:

```text
b4a23c0bbd5a23bf124e8047f3bf50f29b8816e98bf50c3435aedfee019982f6  index.html
709c55789fa9d3e75773d5eb8c0c53834b83620651eae10bec6911a212d432e0  dashboard.png
```

`file` identified a real 2880 x 2504, 8-bit RGB PNG. Visual inspection of that export found all four zones, the notice, all five status samples and all eight columns visible without clipping. The initial export correctly shows no historical trend before successful page polls. Open Design reported inspecting 1440px desktop and 390px mobile layouts; mobile was not independently inspected by this outer worker.

## Implementation boundaries

This is a presentation reference, not production code, API data or milestone acceptance:

- Preserve real API enums and retry/idempotency behavior. The numeric demo attempt counter is not production key generation. Never display a key.
- Do not copy simulated stock changes, order insertion, amount strings or state transitions into Angular. Real values and statuses must come from the API.
- Keep the production 2-second polling cadence and existing polling test assertions unchanged. Do not copy the buy simulation timer.
- Keep histories client-side, at most 30 successful polls, reset on event change/reload. Do not add a stock capacity/progress bar or a stock price.
- Apply KPI motion only for changed values and row highlights only for newly observed ids after an established successful poll. Clear row highlighting on the following successful poll.
- In Angular, expose `matchMedia` reduced-motion state and suppress motion classes as required by the spec, in addition to the CSS media query. The reference's CSS suppression is not proof of the later Vitest no-motion-class criterion.
- The demo input's maxlength of 80 is not the production validation contract; retain the approved production limit.
- Replace illustrative notice text with one concise live-stack notice, stating that accepted is not confirmed once. Do not carry the reference's M7 version label forward as a new milestone decision.
- Amounts and full created/updated timestamps follow the real API, not the illustrative table's fixed-date/time-only presentation. Full ids must remain available in title and accessible name.

No Angular, backend, dependency, deployment, DESIGN.md, PROGRESS.md or existing M7 reference files were changed. No push or PR was made. Application tests, typecheck, build, container acceptance, the live after screenshot and a full accessibility audit are not verified here and belong to later pipeline cards.

## Approval

Pending Phuong's review of the HTML and PNG. Record his verbatim approval here before completing this card or starting Angular work.
