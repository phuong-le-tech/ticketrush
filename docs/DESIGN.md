# TicketRush - Design Decisions

Decisions approved by the owner. Changing anything in sections 3 to 5 (schemas, topics,
partitions) needs the owner's approval first, because those choices are hard to reverse.

## 1. Versions

Checked against Maven Central and Docker Hub on 2026-10-16. Re-check tools for later milestones
(Strimzi, KEDA, Angular, k6, Chaos Mesh, OpenTelemetry) when that milestone starts, and add them here.

| Component | Version | Note |
|---|---|---|
| Java | 21 | Owner's choice. |
| Maven | 3.9.12 | |
| Spring Boot | 4.1.1 | Approved deviation from "3.x" (3.5 is out of OSS support). |
| Spring Kafka | 4.1.x | Managed by the Boot BOM. Do not pin separately. |
| Kafka broker | 4.3.1 (`apache/kafka` image, KRaft) | 4.4.0 had no official image yet on 2026-10-16. |
| PostgreSQL | 18.6 | |
| Redis | 8.10.2 | Fall back to 8.8.3 if 8.10.2 proves unstable. |
| Testcontainers | 2.0.5 | Managed by the Boot BOM. 2.x module names: `testcontainers-kafka`, `testcontainers-postgresql`. |

## 2. Repo layout

```
ticketrush/
  pom.xml                     parent: Boot BOM, Java 21, module list
  common/                     event records, topic names, JSON serde config
  api-gateway/                POST /orders -> orders.requested
  inventory-service/          Redis stock counter, orders.requested -> tickets.reserved | orders.rejected
  payment-service/            tickets.reserved -> fake provider (HTTP) -> payments.result
  fake-payment-provider/      tiny HTTP app, records charges per idempotency key
  order-service/              Postgres order table (saga state), -> orders.confirmed
  notification-service/       consumes orders.confirmed, logs a notification
  e2e-tests/                  Testcontainers test that runs every service in one JVM
  deploy/compose/docker-compose.yml
  .env.example
```

Rationale:

- **`e2e-tests` boots each service's Spring context in-process** against Testcontainers Kafka,
  Postgres and Redis. Faster and less flaky than building images per test run. Real images are
  exercised from M3 on.
- **One Postgres instance, one schema per service** (`orders`, `payments`, `inventory`). A service
  never reads another service's tables.
- **Data access with Spring `JdbcClient` + Flyway, no JPA.** The outbox and idempotency logic is
  SQL-heavy; plain SQL keeps the invariant-preserving statements visible.
- **The gateway has no database and no outbox.** It does a synchronous `send()` with `acks=all` and
  the idempotent producer, and returns 202 only after Kafka acknowledges. The Kafka write is the
  only state change, so there is nothing to keep atomic with it.
- **Order-service also consumes `orders.requested`** to create a `PENDING` row. That row is the saga
  state the reaper (M4) and the dashboard (M7) need. This is an extra consumer, not an extra service.
- **The reaper lives in inventory-service** (it owns reservations). Built in M4.

### Dependencies and why

| Dependency | Why |
|---|---|
| `spring-kafka` | Kafka producers/consumers, error handlers, dead-letter publishing. |
| `spring-boot-starter-web` | HTTP endpoints (gateway, fake provider) and the provider client. |
| `spring-boot-starter-data-redis` | Lettuce client for the atomic stock counter (Lua script). |
| `spring-boot-starter-jdbc` + `flyway` | Plain SQL access and versioned schema migrations. |
| Testcontainers | Real Kafka/Postgres/Redis in integration tests. |
| `awaitility` (test only) | Wait for async Kafka results without `Thread.sleep`. |
| `tools.jackson.core:jackson-databind` | Jackson 3 mapper; required by `JacksonJsonSerializer`, which is optional in spring-kafka. Used in `common`, BOM-managed. |
| `spring-boot-starter-test` (test only) | JUnit 5 and AssertJ for unit tests. |

Any new dependency must be added to this table with a reason.

## 3. Event schemas (`common`)

Java records, serialized as JSON.

- **`eventId` = the concert/show being sold.** `messageId` = unique id of one Kafka message, used
  by consumers to drop duplicates.
- **Money is `long amountCents` + `String currency`.** Never `double`.
- **Inventory sets the price, never the client.** The amount first appears in `TicketsReserved`.
- **`quantity` is 1 to 4, all-or-nothing.** Check and decrement happen in one Redis Lua script.
- **Type header is a short logical name** (e.g. `OrderRequested`) via Spring JSON type mappings,
  not the Java class name, so renaming a class does not break consumers.

```java
record OrderRequested(UUID messageId, UUID orderId, String idempotencyKey, String eventId,
                      String customerId, int quantity, Instant occurredAt)

record TicketsReserved(UUID messageId, UUID orderId, String eventId, int quantity,
                       long amountCents, String currency, Instant reservedUntil, Instant occurredAt)

record ReservationRejected(UUID messageId, UUID orderId, String eventId,
                           String reason /* SOLD_OUT */, Instant occurredAt)

record PaymentResult(UUID messageId, UUID orderId, String eventId, Status status /* SUCCEEDED | FAILED */,
                     long amountCents, String currency, String providerChargeId /* nullable */,
                     String failureReason /* nullable */, Instant occurredAt)

record OrderConfirmed(UUID messageId, UUID orderId, String eventId, String customerId,
                      int quantity, long amountCents, String currency, Instant occurredAt)

// Defined in M1, used from M4.
record TicketsReleased(UUID messageId, UUID orderId, String eventId, int quantity,
                       String reason /* PAYMENT_FAILED | TIMEOUT */, Instant occurredAt)
```

## 4. Topics and partitions

| Topic | Producer -> consumer(s) | Key | Partitions |
|---|---|---|---|
| `orders.requested` | gateway -> inventory, order | eventId | 12 |
| `tickets.reserved` | inventory -> payment | orderId | 12 |
| `orders.rejected` | inventory -> order | orderId | 12 |
| `payments.result` | payment -> order | orderId | 12 |
| `orders.confirmed` | order -> notification | orderId | 12 |
| `tickets.released` | reaper / order -> inventory (M4) | eventId | 12 |
| `<topic>.DLT` (M4) | Spring `DeadLetterPublishingRecoverer` | same key | 12 |

Rationale:

- **eventId key only on topics inventory consumes**: one writer per show, ordered processing.
- **orderId key everywhere else**: payment is the slow step (external HTTP call), so it must spread
  across partitions and pods. Keying it by eventId would put a whole sale on one partition.
- **Known limit (document it in M6):** a single show's sale goes through one inventory partition,
  so more inventory pods do not speed up one show. Acceptable because a Redis decrement takes
  microseconds. Payment and order processing do scale.
- **12 partitions**: divides evenly across 1, 2, 3, 4, 6 pods and covers the 8-pod chaos test.
  Partitions can be added but never removed, and adding them remaps keys.
- **Replication**: Compose = 1 broker, RF 1. Kubernetes (M3) = 3 brokers, RF 3, `min.insync.replicas=2`.
- **One DLT per source topic**: Spring's default; the failed record keeps its partition number.

## 5. Gateway API (M1)

- `POST /orders`, header `Idempotency-Key`, body `{ "eventId", "customerId", "quantity" }`
  -> `202 { "orderId" }` after Kafka acks `OrderRequested`.
- Idempotency-key deduplication arrives in M2; 429 + `Retry-After` arrives with overload handling.

## 6. Decision log

Append new decisions here (date, decision, reason). Keep entries one or two lines.

| Date | Decision | Reason |
|---|---|---|
| 2026-10-16 | Boot 4.1.1 | 3.5 out of OSS support. |
| 2026-10-16 | `orders.rejected` topic | Sold-out path to order-service without touching payment. |
| 2026-10-16 | Fake provider as its own container | Realistic timeouts; enables the double-charge test. |
| 2026-10-16 | 12 partitions; eventId key for inventory topics, orderId for the rest | Single writer per show, scalable payment. |
| 2026-10-16 | JSON events, no schema registry | Fewer moving parts; trade-off is no enforced schema evolution. |
