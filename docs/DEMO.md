# TicketRush - Demo

![Buying tickets on the dashboard: stock drops and each order goes to CONFIRMED](design/demo/ticketrush-demo.gif)

**The GIF above shows the previous dashboard** (the Compose page before the live rebuild). It still
shows the buying flow correctly, but not the system map, the four chapters or the kind deployment.
The owner must re-record it from the new page (kind demo below). The GIF
(`docs/design/demo/ticketrush-demo.gif`, 14 s) was recorded against the Compose stack on
2026-11-11: four purchases through the dashboard, stock going 1000 to 996, each order reaching
`CONFIRMED` in the saga table. It shows the happy path only. Failure and chaos behaviour is
documented with measurements in [CHAOS-RESULTS.md](CHAOS-RESULTS.md), not shown in this GIF.

## Re-record the GIF (about 15 seconds)

Needs Docker only. This is the Compose page: no Prometheus there, so the four chapters show a
named "needs the kind stack" state. Use the kind demo below for the full page.

```sh
docker compose -f deploy/compose/docker-compose.yml down -v
docker compose -f deploy/compose/docker-compose.yml up -d --build
until curl -fs localhost:4200/api/events/concert-1/stock; do sleep 2; done
```

| Time | Action | What the viewer sees |
|---|---|---|
| 0:00 | Open <http://localhost:4200> | Stock 1000, 0.0 orders/s, empty order table |
| 0:02 | Click Buy once | "Order ... accepted ... not yet confirmed" (acceptance is not confirmation) |
| 0:05 | Scroll to Recent orders | Row goes `PENDING`, then `CONFIRMED`; stock 999 |
| 0:08 | Click Buy three more times, 3 s apart | Stock 998, 997, 996; orders per second rises |
| 0:14 | Stop | Four `CONFIRMED` rows, EUR 50.00 each |

## Kind demo of the live dashboard (about 90 seconds)

Needs Docker (16 GB VM), kind, helm and k6. Start the stack, open the dashboard, then add load.

```sh
scripts/observability-up.sh        # kind cluster, Kafka, Prometheus, KEDA and the dashboard image
kubectl --context kind-ticketrush -n ticketrush port-forward svc/dashboard 4200:8080
```

Open <http://localhost:4200>. In a second terminal, start the load (about 60 seconds, 8 orders per
second against isolated events, so `concert-1` stock is untouched):

```sh
GW=http://127.0.0.1:8080
kubectl --context kind-ticketrush -n ticketrush port-forward svc/api-gateway 8080:8080 &
RUN=demo-$(date +%s)
for i in 0 1 2; do kubectl --context kind-ticketrush -n ticketrush exec redis-0 -- \
  redis-cli HSET inventory:event:$RUN-$i stock 300 priceCents 100 currency EUR; done
k6 run -e BASE_URL=$GW -e RUN_ID=$RUN -e LOAD_DURATION=60s \
  -e EVENT_IDS="[\"$RUN-0\",\"$RUN-1\",\"$RUN-2\"]" scripts/autoscale-load.js
```

| Where | What the viewer sees |
|---|---|
| System map | Title "One order, from click to confirmation." A Buy click sends an accent dot gateway, `orders.requested`, inventory, `tickets.reserved`, payment, order, notification. Edge rates move with the load. |
| 01 Concurrency | Stock for `concert-1` and the saga outcomes counter. Stock only goes down and never below zero. |
| 02 Kafka | Consumer lag per group and per partition rises under load and drains afterwards; processing rate per consumer. |
| 03 Redis | Stock is one atomic Lua step in Redis (check, decrement, record the order). Commands per second and latency from inventory-service. Redis is the stock store here, not a cache. |
| 04 Kubernetes | Lag against the KEDA threshold (10); ready pods per service. With `M5_CONSUMER_PROCESSING_DELAY_MS=200` at `observability-up.sh` time a second consumer pod appears (`maxReplicas` is 2); with no artificial delay it may stay at 1. |

Stale state: delete the Prometheus pod and the chapters mark themselves stale, then recover.
Not recorded yet: this demo has not been filmed. The owner records it.

## Optional second clip: failure on kind (about 60 seconds)

Not recorded. The chaos experiments run on kind with Grafana, not through the dashboard. Setup and
requirements (16 GB Docker VM): [OPERATIONS.md](OPERATIONS.md).

| Time | Command or action | What the viewer sees |
|---|---|---|
| 0:00 | `scripts/observability-up.sh` already done; open Grafana at `localhost:3000/d/ticketrush-m5` | Four panels: lag, processing time, orders per second, saga outcomes |
| 0:10 | `scripts/chaos.sh --experiment inventory-kill --runs 1` | k6 starts at 8 orders per second |
| 0:25 | The runner deletes an inventory pod | Orders per second dips, consumer lag rises |
| 0:45 | Pod is replaced | Lag drains, completion rate catches up |
| 1:00 | Runner prints its audit | Every acknowledged order settled, no oversell, one charge per confirmed order |
