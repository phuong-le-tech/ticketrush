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
| Strimzi operator | 1.3.0 | M3. Installed by its Helm chart (`oci://quay.io/strimzi-helm/strimzi-kafka-operator`), version pinned in `scripts/m3-up.sh`. Supports Kafka 4.3.1; CR API `kafka.strimzi.io/v1`. |
| Helm | 4.x (local tool) | M3. Chart in `deploy/helm/ticketrush`. Installed with `brew install helm`. |
| kind | 0.33 (local tool) | M3. Local cluster `ticketrush`. Installed with `brew install kind`. |
| PostgreSQL / Redis in Kubernetes | 18.6 / 8.10.2 | M3. Same images as Compose, as plain single-replica StatefulSets with a PVC (no Bitnami, no HA). |

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
  deploy/helm/ticketrush/     M3 chart: Strimzi Kafka + topics, Postgres, Redis, 6 services
  scripts/m3-up.sh            kind cluster + Strimzi + images + chart, idempotent
  scripts/m3-chart-check.sh   static chart checks (helm lint + template assertions)
  scripts/m3-acceptance.sh    m3-up.sh twice, order to CONFIRMED, graceful rollout restart
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
- **The reaper lives in order-service** (M4, deviation from the first plan that put it in
  inventory-service; approved 2026-10-30). Order-service owns the saga state, so the timeout is one
  conditional update next to the payment transition, and a release is emitted at most once.
- **Transactional outbox in order-service only (M4).** `orders.outbox` rows are written in the same
  transaction as the status change; `OutboxRelay` publishes them. Payment and inventory keep direct
  sends: their replays are already safe (provider key = orderId, `reserve.lua` dedupe).

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
| `spring-boot-starter-actuator` (every service) | M3. Readiness and liveness endpoints (`/actuator/health/readiness`, `/actuator/health/liveness`) for the Kubernetes probes. Only the health endpoint is exposed (Boot default). |
| `spring-boot-starter-webmvc` (inventory, order, notification) | M3. These three had no web server; the probes need an HTTP port. Same starter as the gateway. |
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
| `tickets.released` | order (outbox: payment failure, saga timeout) -> inventory (M4) | eventId | 12 |
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
- M2: `orderId` = UUIDv5 (SHA-1, namespace `3fc18c52-d4b9-4f65-81fe-4ce095a8f1cc`) of
  `customerId.length() + ":" + customerId + ":" + eventId.length() + ":" + eventId + ":" + Idempotency-Key`.
  A retry or duplicate POST with the same key, customer and event gets the same `orderId` (202). The
  same key with a different `eventId` is a different order. A reused key with a different quantity
  is treated as a retry of the first request: first write wins downstream. Key longer than 255 chars,
  or blank -> 400. The gateway stores nothing; dedupe happens where state lives:
  - inventory: `reserve.lua` checks `inventory:order:{orderId}` (Redis hash: `reserved`,
    `priceCents`, `quantity`, `currency`, `reservedUntil`) in the same script as the stock check.
    Seen order -> the stored outcome is returned and re-published, stock is not touched. After a
    release (`released=1`), replay returns rejected instead: restored stock is not a held reservation. TTL
    `INVENTORY_DEDUP_TTL_SECONDS` (default 604800 = 7 days). An unknown event stores the rejection
    (`reserved=0`, same TTL) on the order key, so that orderId stays rejected if the event is seeded
    later; no `inventory:event:*` key is created.
  - payment: provider idempotency key = `orderId`; order-service: conditional upserts (M1).
- 429 + `Retry-After` arrives with overload handling.

## 6. Decision log

Append new decisions here (date, decision, reason). Keep entries one or two lines.

| Date | Decision | Reason |
|---|---|---|
| 2026-10-16 | Boot 4.1.1 | 3.5 out of OSS support. |
| 2026-10-16 | `orders.rejected` topic | Sold-out path to order-service without touching payment. |
| 2026-10-16 | Fake provider as its own container | Realistic timeouts; enables the double-charge test. |
| 2026-10-16 | 12 partitions; eventId key for inventory topics, orderId for the rest | Single writer per show, scalable payment. |
| 2026-10-16 | JSON events, no schema registry | Fewer moving parts; trade-off is no enforced schema evolution. |
| 2026-10-22 | orderId = UUIDv5(namespace, length-prefixed customerId + Idempotency-Key), SHA-1 via `MessageDigest`, key max 255 chars | Retries map to the same order with no gateway state and no new dependency; the length prefix makes the name unambiguous. |
| 2026-10-22 | Inventory dedupe in `reserve.lua`: `inventory:order:{orderId}` hash holds the first outcome, TTL `INVENTORY_DEDUP_TTL_SECONDS` (7 days) | Check, decrement and record in one atomic script, so a replay can never reserve twice; re-publishing the stored outcome covers a crash between Lua and send. |
| 2026-10-24 | eventId added to the orderId UUIDv5 name (length-prefixed, after customerId); unknown-event rejection stored as `reserved=0` with the dedupe TTL (option a) | `orders.requested` is keyed by eventId, so a key reused across events split one orderId across partitions and could reserve it twice; storing the unknown-event rejection keeps a rejected orderId rejected after the event is seeded. |
| 2026-10-25 | M3 on kind with Helm (not Kustomize); Strimzi 1.3.0 via its Helm chart; Postgres 18.6 and Redis 8.10.2 as plain single-replica StatefulSets with PVCs | Owner decision. Helm values give one place for sizes and credentials; no Bitnami charts to track. |
| 2026-10-25 | Kafka on Kubernetes: one KafkaNodePool of 3 nodes with roles broker+controller (KRaft), RF 3, `min.insync.replicas=2`, transaction state log ISR 2, auto-create off, topics as `KafkaTopic` CRs (12 partitions) | 3 brokers with a 3-node quorum without 3 extra controller pods on a laptop; the topic operator replaces the Compose `kafka-init` container. |
| 2026-10-25 | `spring-boot-starter-actuator` in every service, `management.endpoint.health.probes.enabled=true`; inventory, order and notification get `spring-boot-starter-webmvc` | Kubernetes probes need HTTP endpoints; these three had no web server. |
| 2026-10-25 | Credentials come from a chart-generated Secret (`ticketrush-credentials`) built from `values.yaml` dummy defaults; Deployments only reference it with `secretKeyRef` | No plaintext credential in a rendered Deployment; real values are supplied with `--set-string` and never committed. |
| 2026-10-25 | Image tag = first 12 hex of the Docker image id; `.dockerignore` keeps `.git`, `target`, docs and scripts out of the build context | An unchanged build gives the same tag, so a second `m3-up.sh` changes nothing. |
| 2026-10-25 | order-service and inventory-service get init containers that wait for Postgres or Redis (`waitFor` in values.yaml) | Both fail fast at startup when the dependency is not reachable yet; on a cold start that crash-looped them (7-8 restarts). Kafka needs no wait: the clients retry. |
| 2026-10-30 | M4 outbox in order-service only: Flyway V2 `orders.outbox(id, topic, msg_key, type, payload jsonb, created_at, published_at)`, written in the status-change transaction for OrderConfirmed (`orders.confirmed`, key orderId) and TicketsReleased (`tickets.released`, key eventId). The relay locks the oldest unpublished rows with `FOR UPDATE SKIP LOCKED`, sends with acks=all, sets `published_at` after the ack, all in one DB transaction | Owner decision. A crash can no longer lose an event between the DB write and the send; at-least-once, consumers dedupe. |
| 2026-10-30 | Saga reaper moved to order-service (deviation from section 2's first plan). `ORDER_SAGA_TIMEOUT_SECONDS` (default 120): PENDING/RESERVED orders created strictly more than that ago become FAILED(TIMEOUT) and emit TicketsReleased(TIMEOUT). PaymentResult FAILED makes the order FAILED and emits TicketsReleased(PAYMENT_FAILED). inventory-service restocks once per orderId (`release.lua`, only if the order holds a reservation) | Owner decision. Conditional updates make the timeout and the payment race safe: one terminal outcome, one release at most. |
| 2026-10-30 | Every consumer: `DefaultErrorHandler`, exponential backoff 1 s x2, 4 attempts (`KAFKA_RETRY_*`), then `DeadLetterPublishingRecoverer` to `<topic>.DLT`, same partition, 12 partitions (Compose RF 1, Helm KafkaTopic RF 3). Fake provider decline rate `FAKE_PROVIDER_FAILURE_RATE` (0.0 to 1.0, default 0) | Owner decision. A poison record leaves its partition after about 7 s instead of blocking it. |
| 2026-10-30 | Implementation choices for the decisions above: status `FAILED` replaces `PAYMENT_FAILED` (V2 migrates old rows; `failure_reason` says why) and `RESERVED` is an allowed active state; every exception is retryable, deserialization included (Spring treats those as fatal by default); the DLT resolver is explicit because Spring Kafka 4 defaults to a `-dlt` suffix; DLT records keep the original bytes (`ErrorHandlingDeserializer` plus a byte[]-aware serializer) | Match the approved names and attempt counts exactly; one status for every failed saga keeps the reaper and the payment path symmetric. |
| 2026-10-30 | Payment: a 200 SUCCEEDED or a decline (200 FAILED, or 4xx) is final; 5xx, timeouts, network errors and unreadable replies throw and are retried with the same Idempotency-Key, then dead-lettered with no PaymentResult (the order then times out). The fake provider stores declines under the key too, so a retry gets the same answer | A lost reply may hide a real charge: only a retry with the same key can tell, and it never charges twice. |
| 2026-10-30 | notification-service dedupes OrderConfirmed by orderId in memory (bounded, per pod) | The outbox is at-least-once. Shortcut: lost on restart and not shared between pods; a shared store is needed once a notification has a real effect. |
| 2026-10-30 | Known limitation (not fixed in M4): a PaymentResult SUCCEEDED for an order already FAILED(TIMEOUT) is not confirmed; it is logged (WARN, charge id) and stays on `payments.result` for a future refund. No refunds yet | Owner decision. The timeout default (120 s) is far above the worst-case payment time (4 x provider timeout 2 s + 7 s backoff = 15 s). |
| 2026-10-31 | PR 18 review compatibility option: V2 expands the status CHECK before converting existing PAYMENT_FAILED rows to FAILED, and retains PAYMENT_FAILED in the allowed set | The M3 writer must still accept payment failures after an application-image rollback; contraction is deferred. This is compatibility, not an automated schema downgrade. |
| 2026-10-31 | PR 18 review replay fix: reserve.lua returns rejected for an order marked released=1, without changing stock | A restored reservation must never be republished as held or trigger a new provider call. |
