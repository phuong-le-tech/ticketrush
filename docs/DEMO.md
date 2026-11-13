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

Needs Docker (16 GB VM), kind, helm and k6. Start the stack and open the dashboard:

```sh
scripts/observability-up.sh
kubectl --context kind-ticketrush -n ticketrush port-forward svc/dashboard 4200:8080
```

Open <http://localhost:4200>. In a second terminal, seed a demo event and run the load against it. The
Redis seeding is a demo convenience: it writes the stock hash directly and bypasses the service's own
seeder. `concert-1` is not touched.

```sh
ID=demo-$(date +%s)
kubectl --context kind-ticketrush -n ticketrush exec redis-0 -- \
  redis-cli HSET inventory:event:$ID stock 500 priceCents 5000 currency EUR
kubectl --context kind-ticketrush -n ticketrush port-forward svc/api-gateway 8080:8080 &
PF=$!
sleep 3
k6 run -e BASE_URL=http://127.0.0.1:8080 -e RUN_ID=$ID -e EVENT_IDS="[\"$ID\"]" \
  -e LOAD_RATE=20 -e LOAD_DURATION=75s scripts/autoscale-load.js
kill $PF                                                  # cleanup of the background port-forward
kubectl --context kind-ticketrush -n ticketrush exec redis-0 -- redis-cli DEL inventory:event:$ID
```

Type the same `$ID` into the dashboard's Event id field before starting k6, so chapter 01 follows it.

Observed on 2026-11-11 in the built-in browser at 1440px, on the deployed dashboard, with k6 at 20 orders per
second for 75 seconds on a 500-ticket event:

| Where | Observed |
|---|---|
| System map | Node rates between 13 and 40 per second. Up to 40 dots at once (the cap), red dots for rejected orders. A manual Buy on a quiet system produced one accent dot crossing the whole map. |
| 01 Concurrency | Stock fell from 500 to 0 in well under a minute and stayed at 0; the chart line reached the floor and stayed flat. A manual Buy afterwards was accepted, then REJECTED with reason SOLD_OUT in the table. The confirmed, rejected and failed counters are labelled "Saga outcomes, all events, since order-service start": they are not per event. |
| 02 Kafka | Lag stayed between 0 and 5 messages. Consumers keep up at this rate with no artificial processing delay. |
| 03 Redis | About 20 EVALSHA commands per second, 0.6 ms per reservation. Stock is one atomic Lua step in Redis (check, decrement, record the order); Redis is the stock store here, not a cache. |
| 04 Kubernetes | Ready pods stayed 1 of 2 for both services. No second pod appeared, because lag never passed the KEDA threshold of 10. |
| Compose (localhost:4200) | Chapters 02 to 04 show "Live metrics need the kind stack."; map rates show dashes; stock and Buy work. |

Expected, not observed:

- A second consumer pod appearing in chapter 04. It is expected only when the stack is brought up with
  `M5_CONSUMER_PROCESSING_DELAY_MS=200 scripts/observability-up.sh` (as the M5 acceptance does). That was not
  watched in the dashboard.
- The stale state (delete the Prometheus pod after the page has been live: the chapters should mark themselves
  stale, then recover). Not seen.
- Reduced motion. Not seen.

The acceptance script's own load (`scripts/live-dashboard-acceptance.sh`) runs on three isolated events, so
`concert-1` stock stays flat during it.

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
