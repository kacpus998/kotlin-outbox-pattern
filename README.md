# Kotlin Transactional Outbox

Demonstration project for persisting an order and its `OrderCreated` event atomically, then publishing pending events from a scheduled worker. It intentionally publishes to application logs rather than a broker.

## Stack and layout

Java 21, Kotlin 2.2, Spring Boot 3.5, Spring WebFlux, Project Reactor, Spring Data R2DBC, PostgreSQL 17, Flyway, JdbcTemplate, JUnit 5 and Testcontainers. The Orders API uses R2DBC; the scheduled outbox publisher remains JDBC-based.

```text
POST /orders → Mono<CreateOrderResponse> → R2DBC transaction → orders + outbox_events
scheduled publisher → JDBC transaction → log → processed_at
```

The publisher selects `processed_at IS NULL` rows ordered by `(created_at, id)` using PostgreSQL `FOR UPDATE SKIP LOCKED`. The selected rows remain locked until the transaction commits or rolls back.

## Run

Requirements: Java 21 and Docker with Compose.

```bash
docker compose up -d postgres
./gradlew bootRun --args='--spring.profiles.active=local'
```

PostgreSQL configuration for local execution is in `application-local.yml`. `DB_HOST`, `DB_PORT`, and `DB_NAME` configure JDBC and R2DBC together; credentials use `DB_USERNAME` and `DB_PASSWORD`. If overriding connection URLs directly, set both `DB_URL` and `R2DBC_URL` to the same database and schema. Flyway applies the existing schema migration over JDBC before application requests are served. Orders persistence uses R2DBC, while the scheduled publisher uses a separate JDBC connection pool against that same schema. Timestamps use PostgreSQL `TIMESTAMPTZ`.

```bash
curl -i -X POST http://localhost:8080/orders \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"123"}'
```

Returns HTTP 201, for example `{"orderId":"<UUID>"}`. `customerId` must be non-blank and at most 255 characters; invalid values and malformed JSON return HTTP 400. There is no HTTP idempotency key: repeating an identical request creates another order and another event, each with fresh UUIDs.

Interactive API docs: [Swagger UI](http://localhost:8080/swagger-ui.html); OpenAPI JSON: `/v3/api-docs`. The request schema and example are included.

## Persistence and publisher settings

Flyway migration `V1` defines `orders` and `outbox_events`. The latter has a UUID primary key, order foreign key, JSONB payload, event type, UTC timestamps, and a partial index for unprocessed events. Inspect state with:

```sql
SELECT id, event_type, aggregate_id, payload, processed_at
FROM outbox_events ORDER BY created_at, id;
```

Defaults: scheduler enabled, fixed delay 5000 ms, batch size 10. Set `outbox.publisher.enabled=false` to disable it, or configure `outbox.publisher.fixed-delay-ms` and positive `outbox.publisher.batch-size`. The batch is processed in one transaction: any failure rolls back its database updates. A repeatedly failing (“poison”) event remains pending and may repeatedly fail the batch, delaying later events; no DLQ or backoff is implemented. A long-running publication holds row locks and its transaction open.

Logs default to `outbox.publisher.logging.payload-mode=METADATA_ONLY`, logging only event ID, event type and aggregate ID. `REDACTED` adds only allowlisted `orderId` and masked `customerId`; unknown payload fields are omitted. There is no RAW mode. This does not alter the stored JSONB payload.

## Delivery semantics and limitations

**Implemented and tested:** the order and outbox row are persisted atomically; pending rows can be retried; `processed_at` is set only after publication returns successfully; normal subsequent cycles skip committed events. `SKIP LOCKED` prevents two active publisher transactions from selecting the same locked row. Ordering is deterministic among currently eligible rows, but multiple workers do not provide strict global FIFO.

Delivery is **at-least-once**, not exactly-once. If the logger performs its side effect and the database transaction subsequently fails before commit, the row remains pending and the same stable event ID/payload can be published again. `processed_at` records the publisher's committed state only; it cannot atomically commit an external consumer's business effect. The current application has no event consumer or deduplication mechanism. A real consumer should deduplicate by stable event ID, commonly by inserting the ID into an `inbox`/`processed_events` table in the same transaction as its business effect. Even then the publisher's log output itself can repeat. Restart safety follows from database-backed pending state, but an actual full process-restart scenario is not covered by an automated test.

This small example has no request idempotency keys, consumer inbox, broker, DLQ, retry backoff, authentication, or production-grade observability. These are deliberate scope limits, not implemented guarantees.

## Tests and CI

```bash
./gradlew clean test
./gradlew build
```

Unit tests do not require Spring or Docker. Integration tests use `WebTestClient`, `StepVerifier`, and a shared-lifecycle PostgreSQL 17 Testcontainers instance; Docker is required. GitHub Actions runs the Gradle Wrapper on GitHub-hosted `ubuntu-latest` with Java 21, validates the wrapper, runs build and integration tests, and uploads JUnit reports. No deployment occurs. CI workflow actions are SHA-pinned and token permission is read-only.

On environments requiring Docker API compatibility override:

```bash
OUTBOX_DOCKER_API_VERSION=1.40 ./gradlew clean test
```

Stop local PostgreSQL with `docker compose down`; do not use `-v` unless intentionally deleting its persistent data.
