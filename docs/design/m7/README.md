# M7 dashboard design reference

Status: awaiting Phuong's validation. This is not the Angular application or milestone acceptance.

## View

- Open [index.html](index.html) directly in a browser. It is self-contained and makes no API calls.
- [dashboard.png](dashboard.png) is Open Design's rendered desktop capture, 2880 x 2820 pixels.
- All data, prices, timestamps and interactions are explicitly illustrative.
- The page covers stock and price, orders per second, the buy/retry form and the recent-order saga table.
- Use the demo-state selector for loading, empty, error and stale views. Buy simulates a failure; Retry simulates success without reserving or charging anything.

## Provenance

- Tool: Open Design MCP, not a hand-made replacement.
- Skill: `frontend-design`, Open Design's built-in adaptation of Anthropic's frontend-design skill. The discovered `web-artifacts-builder` entry was not selected because it advertises an upstream bundle rather than providing the full workflow.
- Design system: `dashboard` (Dashboard), attached to the project and confirmed in the run result. The generated page applies a dark operational treatment with IBM Plex Sans when locally available and safe local fallbacks.
- Project id: `ticketrush-m7-dashboard`.
- Conversation id: `58ee96b7-6f11-4280-a75e-c4e53964f4f4`.
- Run id: `7d63e2aa-7327-43f2-8277-e55c1a244621`.
- Open Design returned `status: succeeded`, `deliverableValid: true`, syntax status `pass` with no diagnostics, and artifact paths `index.html`, `dashboard.png`.
- Both exports were compared byte-for-byte with the generated Open Design project files using `cmp`; neither was edited after generation.

### Exact MCP calls

Calls are listed in execution order. `get_run` was repeated while generation was in progress.

```text
mcp__open_design__list_skills({})
mcp__open_design__list_resources({})
mcp__open_design__read_resource({"uri":"od://skills/web-artifacts-builder/SKILL.md"})
mcp__open_design__read_resource({"uri":"od://design-systems/dashboard/DESIGN.md"})
mcp__open_design__read_resource({"uri":"od://skills/frontend-design/SKILL.md"})
mcp__open_design__create_project({"designSystem":"dashboard","id":"ticketrush-m7-dashboard","name":"TicketRush M7 dashboard","skill":"frontend-design"})
mcp__open_design__start_run(<exact arguments in generation-request.json>)
mcp__open_design__get_run({"runId":"7d63e2aa-7327-43f2-8277-e55c1a244621"})
mcp__open_design__get_artifact({"include":"all","project":"ticketrush-m7-dashboard"})
mcp__open_design__get_artifact({"entry":"index.html","include":"all","project":"ticketrush-m7-dashboard"})
```

The first `get_artifact` returned `no entry file: pass entry="..." or set the project's metadata.entryFile`. Passing the explicit entry retrieved both artifacts successfully. The complete generation prompt is preserved in [generation-request.json](generation-request.json).

Open Design's renderer produced the PNG with:

```sh
"$OD_NODE_BIN" "$OD_BIN" export index.html --project "$OD_PROJECT_ID" --format image --out dashboard.png
```

The run's local preview URLs at export time were used to download the files:

```sh
curl --fail --silent --show-error http://127.0.0.1:58266/api/projects/ticketrush-m7-dashboard/raw/index.html -o docs/design/m7/index.html
curl --fail --silent --show-error http://127.0.0.1:58266/api/projects/ticketrush-m7-dashboard/raw/dashboard.png -o docs/design/m7/dashboard.png
```

Those URLs are daemon-session-specific. Refresh them through `get_run` after an Open Design restart; the project id and entry filename remain the durable identity.

## Verification in the design-card run

From the repository root:

```sh
node docs/design/m7/verify-reference.cjs docs/design/m7/index.html
git diff --check -- docs/design/m7
file docs/design/m7/dashboard.png
```

Actual isolated JavaScript-check output:

```text
PASS: four zones, no network/private fields/forbidden dashes, rate formula, 2s cadence, pending guard, retry preservation, quantity validation, stale/loading/empty/error states.
Scope: isolated demo JavaScript checks, not real browser or backend integration.
```

`file` identified a real 2880 x 2820 RGB PNG. The exported image was visually inspected: all four zones, the illustrative-data warning and all four requested sample statuses are visible, without obvious desktop clipping.

SHA-256 at export:

```text
a969a869727fe8aa41d3e258f01312ca25f21a0ea7757b0df1c9e6f2ad3d4f19  index.html
881d57ffb7d3debc484ed5931977e8876dbd07554e5aca7fbef5cfac850a7f42  dashboard.png
```

## Boundaries for implementation

This artifact is a visual reference, not a new API contract. Do not copy its demo internals into production:

- Its sequence counter and simulated failure/success are not production UUID/idempotency or saga behavior. HTTP 202 means accepted, not confirmed; final state comes from polling.
- The stock API returns only eventId and stock, not a price. The shown price is explicitly a seeded demo reference, not a fetched price for arbitrary events.
- The requested `PAYMENT_FAILED` sample label is present. M4's actual order model uses `FAILED` and can also expose `RESERVED`; the UI must preserve real API states, not change backend enums to match a mockup.
- The reference input has a demo-only maxlength of 80; production validation must follow the approved request contract rather than this prototype constraint.
- Loading/error views and fixed sample values illustrate presentation, not tested network recovery. The production stale marker and continued 2-second polling need the separate Vitest tests.
- Responsive styles and horizontal table scrolling exist, but mobile device rendering and a full accessibility audit were not independently exercised here.

No Angular, Java, deployment, Maven, DESIGN.md or PROGRESS.md files were changed by this design card. No application dependency was added. Maven tests, container acceptance and a screenshot of the real running dashboard belong to later cards and were not run here.
