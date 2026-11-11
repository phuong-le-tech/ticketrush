# Live explanatory dashboard - design

Date: 2026-11-11. Status: awaiting owner approval. Visual rules: [docs/design/DESIGN.md](../../design/DESIGN.md).

## Intent

The owner demos TicketRush live to recruiters. Today's dashboard (M7) proves the system works but
not why: it shows stock, orders per second, a buy form and a saga table. The owner finds it bland
and not explicit enough. The redesign must let a recruiter understand, in about two minutes, four
notions the project exists to demonstrate: safe concurrency, data flow through Kafka, the role of
Redis, and consumer scaling on Kubernetes. Each is stated in one sentence and then shown by live data.

## Goals

- One page: a live system map, then four chapters (Concurrency, Kafka, Redis, Kubernetes), a buy
  rail and the recent orders table.
- Every figure and chart is read from the running system. The owner chose fully live over replayed
  measurements.
- The demo runs on the kind cluster, where Prometheus, the Kafka exporter and KEDA already run.
- Sober and attractive: see DESIGN.md.

## Non-goals

- No public hosting, no mobile layout, no light theme.
- No new Java endpoint, event schema, topic, partition count or DB change.
- No control of load from the UI (k6 stays a terminal command).
- Not a Grafana replacement; Grafana stays as is.

## Anti-goals

- Any sample, simulated or hard-coded figure presented as live.
- Weakening the central invariant or the existing buy safety behaviour (idempotency key reuse on
  retry, locked inputs, "acceptance is not confirmation").
- Breaking the M7 Compose acceptance (`scripts/dashboard-acceptance.sh`).

## What each part shows, and from where

Verified on the running kind cluster on 2026-11-11 (Prometheus `ticketrush`, 5s scrape, 2h retention):
the metric names below exist. Label sets were not all inspected; the plan must check them first.

| Part | Claim shown | Live source |
|---|---|---|
| System map | An order crosses gateway, inventory, payment, order, notification through named topics | Existing `GET /api/orders/activity` (status transitions of recent orders) for individual dots; `rate(ticketrush_orders_total)` and per-consumer `rate(ticketrush_consumer_processing_seconds_count)` for edge rates |
| 01 Concurrency | Many buyers, few tickets, stock never goes below zero and no order is created twice | Existing `GET /api/events/{id}/stock`; `ticketrush_saga_outcomes_total` by outcome |
| 02 Kafka | Work queues in topics and consumers drain it; ordering per order via key = orderId | `kafka_consumergroup_lag_sum` by group and topic, `kafka_consumergroup_lag` by partition, processing time from `ticketrush_consumer_processing_seconds_*` |
| 03 Redis | Stock is one atomic Lua step in Redis (check, decrement, record the orderId), which is what makes concurrency and replay safe | `lettuce_seconds_count` / `_sum` (commands per second and latency from inventory-service) |
| 04 Kubernetes | When lag passes the KEDA threshold, a second consumer pod appears | `count by (job) (up == 1)` for ready pods per service, plotted against lag and the threshold (10) |

Corrections to the owner's framing, to keep the page honest:

- Redis is not used as a cache here. It holds the stock counter and the per-order outcome used for
  deduplication, in inventory-service only. The chapter says so.
- kube-state-metrics is disabled in this chart, so pod counts come from scrape targets (`up`), which
  counts pods Prometheus can scrape, not pods the scheduler has created. Good enough to show scaling;
  labelled "ready pods".
- KEDA `maxReplicas` is 2 today, so scaling shows 1 to 2 pods for inventory and payment. Raising it is
  a separate decision (M6 showed memory limits).

## How it gets made (owner decided)

- Everything live, on kind.
- Implementation by Sonnet agents.
- Visual direction as in DESIGN.md.

## How it gets made (my calls, not yet the owner's)

- **No charting or animation library.** Charts are small hand-written SVG in Angular (one time-series
  component); motion is CSS transitions and the Web Animations API. Reason: four simple series do not
  justify a dependency.
- **No redis-exporter.** Earlier I proposed one; the Lettuce client metrics already in Prometheus are
  enough. So no new runtime dependency.
- **One new asset: Geist and Geist Mono woff2 (OFL), self-hosted** in `dashboard/public/fonts`, so the
  demo works offline. To record in the dependency table of docs/DESIGN.md.
- **Prometheus reached through the dashboard's nginx**, same origin: `GET /api/metrics/query` and
  `/api/metrics/query_range` proxy to Prometheus `/api/v1/query` and `/api/v1/query_range`, GET only,
  nothing else. The browser sends PromQL; acceptable because the stack is local and unauthenticated
  already. The dev proxy (`proxy.conf.json`) gets the same two routes.
- **Dashboard added to the Helm chart** (Deployment + Service, same pattern as `apps.yaml`, non-root
  nginx image already built for Compose), reached with `kubectl port-forward` on 4200.
- **On Compose** there is no Prometheus: chapters 02-04 show the "needs the kind stack" state from
  DESIGN.md section 9. Nginx on Compose returns 404 for `/api/metrics/` as it does for unknown paths.
- **Polling**, as today: order and stock every 2s, metrics every 5s (the scrape interval), each source
  with its own stale marker, no overlapping reads. Charts backfill 5 minutes with `query_range` on load.
- **Code shape:** split the single `App` component into the map, the chapter, the chart, the buy rail
  and the table. Keep the existing buy logic and its tests unchanged in behaviour.

## Acceptance

One command, `scripts/live-dashboard-acceptance.sh`, that:

1. runs the dashboard unit tests and production build;
2. deploys the chart with the dashboard to kind and waits for readiness;
3. through the dashboard's own origin, proves each of the four metric queries returns a non-empty
   result while a short k6 load runs, and that stock and activity still answer;
4. proves `/api/metrics/` rejects non-GET and any path other than the two allowed;
5. re-runs `scripts/dashboard-acceptance.sh` (Compose) to show M7 still passes.

Visual acceptance is by the owner, in the browser at 1440px, against the approved mockup, including
the stale, idle and reduced-motion states.

## Open questions for the owner

1. Milestone: decided by the owner on 2026-11-11, this work is part of M8 (Portfolio polish), on
   branch `milestone-8`. It widens M8 beyond the brief in SPEC.md, so it must be recorded under
   "Approved deviations from this brief" in SPEC.md and in the DESIGN.md decision log, and the demo
   GIF and DEMO.md script from M8 must be redone once the new dashboard exists.
2. Approve the two infrastructure changes that AGENTS.md asks to confirm: the Prometheus proxy route
   in nginx, and the dashboard in the Helm chart.

## Left to the builder

Component file names, SVG geometry of the map, exact PromQL windows, test structure.
