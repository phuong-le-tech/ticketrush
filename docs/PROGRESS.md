# TicketRush - Progress

Update this file at the end of every milestone: status, the exact verification command and its
result, and what is NOT done or not verified.

| # | Milestone | Branch | Status |
|---|---|---|---|
| 1 | Happy path locally | `milestone-1` | Not started (design approved) |
| 2 | Correctness under contention | `milestone-2` | Not started |
| 3 | Kubernetes | `milestone-3` | Not started |
| 4 | Failure handling | `milestone-4` | Not started |
| 5 | Observability and autoscaling | `milestone-5` | Not started |
| 6 | Chaos experiments and results | `milestone-6` | Not started |
| 7 | Minimal Angular dashboard | `milestone-7` | Not started |
| 8 | Portfolio polish | `milestone-8` | Not started |

## Next up: Milestone 1

Checklist (see [DESIGN.md](DESIGN.md) for the approved layout, schemas and topics):

- [ ] Parent `pom.xml` (Boot 4.1.1 BOM, Java 21) and all modules from DESIGN.md section 2
- [ ] `common`: event records, topic name constants, JSON type mappings
- [ ] `deploy/compose/docker-compose.yml`: Kafka (KRaft), Postgres, Redis, topic creation (12 partitions)
- [ ] `.env.example` with every config variable
- [ ] api-gateway: `POST /orders` -> `orders.requested`
- [ ] inventory-service: Redis Lua reserve (all-or-nothing) -> `tickets.reserved` or `orders.rejected`
- [ ] fake-payment-provider: charge endpoint, records charges by idempotency key
- [ ] payment-service: `tickets.reserved` -> provider -> `payments.result`
- [ ] order-service: `PENDING` on `orders.requested`, final state on `payments.result` / `orders.rejected`, emits `orders.confirmed`
- [ ] notification-service: logs `orders.confirmed`
- [ ] Unit tests for domain logic (written first for money, inventory, idempotency)
- [ ] `e2e-tests`: buys one ticket, asserts `OrderConfirmed` on `orders.confirmed` within 30s

**Acceptance command:** `mvn verify -pl e2e-tests -am`
Also: `docker compose -f deploy/compose/docker-compose.yml up` + one `curl` reaches `orders.confirmed`.

## Milestone reports

<!-- Append one section per finished milestone:
### Milestone N - <date>
- Verified with: `<command>` -> <result>
- What exists:
- NOT done / not verified:
- Bottlenecks or surprises:
-->
