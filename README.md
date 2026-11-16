# Kotlin Transactional Outbox

## Overview

This project demonstrates the Transactional Outbox Pattern. Creating an order writes the order and its `OrderCreated` event to PostgreSQL in one transaction. A scheduled publisher reads pending events and simulates publication by writing them to the application log.

## Technology stack

- Kotlin 2.2 and Java 21
- Spring Boot 3.5, Spring WebFlux, and Project Reactor
- Spring Data R2DBC with PostgreSQL 17
- Flyway for schema migrations
- JUnit 5 and PostgreSQL Testcontainers for tests
- OpenAPI and Swagger UI for API documentation

Orders and publisher database operations use R2DBC. JDBC is retained for Flyway's PostgreSQL migrations; integration tests also use JDBC assertions and fixtures.

## Architecture

```text
POST /orders
  → Mono<CreateOrderResponse>
  → R2DBC TransactionalOperator
  → INSERT orders + outbox_events (atomic)

reactive fixed-delay publisher
  → Flux of pending events (bounded SELECT ... FOR UPDATE SKIP LOCKED)
  → materialize batch, then publish sequentially
  → UPDATE processed_at
  → commit the batch transaction
```

The publisher holds row locks on its transaction connection until the batch commits or rolls back. Separate workers skip rows already locked by another worker.

## Run locally

Requirements: Java 21, Docker Engine, and Docker Compose.

```bash
docker compose up -d postgres
./gradlew bootRun --args='--spring.profiles.active=local'
```

The local profile connects to the Compose database. Configure `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USERNAME`, and `DB_PASSWORD` as needed. If setting connection URLs directly, configure both `DB_URL` (JDBC for Flyway) and `R2DBC_URL` to the same database.

Create an order:

```bash
curl -i -X POST http://localhost:8080/orders \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"123"}'
```

A successful request returns `201 Created`, for example:

```json
{"orderId":"<UUID>"}
```

`customerId` must be non-blank and no longer than 255 characters. Invalid input and malformed JSON return `400 Bad Request`. Repeating a request creates a new order and event; the API does not accept an idempotency key.

The publisher is enabled by default (fixed delay 5000 ms, batch size 10). Configure `outbox.publisher.enabled`, `outbox.publisher.fixed-delay-ms`, and `outbox.publisher.batch-size` in `application.yml` or override them at startup. To disable publishing, set `outbox.publisher.enabled=false`.

Inspect event state in PostgreSQL:

```sql
SELECT id, event_type, aggregate_id, payload, processed_at
FROM outbox_events
ORDER BY created_at, id;
```

## API documentation

- Swagger UI: <http://localhost:8080/swagger-ui.html>
- OpenAPI JSON: <http://localhost:8080/v3/api-docs>

## Transactional guarantees and delivery semantics

- The order and outbox event are committed or rolled back together.
- A publisher batch selects pending rows using PostgreSQL `FOR UPDATE SKIP LOCKED`, logs them sequentially, and updates `processed_at` within one R2DBC transaction. A publication or database error rolls back that batch's database updates.
- Delivery is **at-least-once**, not exactly-once. If log output succeeds but the transaction fails before commit, the same event can be logged again with the same event ID.
- `SKIP LOCKED` prevents two active transactions from claiming the same locked row. Ordering is deterministic among currently eligible rows, but multiple workers do not guarantee global FIFO.
- A real consumer should deduplicate by stable event ID. This sample has no consumer or inbox, so it does not provide exactly-once delivery.

## Logging

The default `outbox.publisher.logging.payload-mode=METADATA_ONLY` logs only event ID, event type, and aggregate ID. `REDACTED` additionally logs allowlisted `orderId` and a masked `customerId`; unknown payload fields are omitted. Line separators are sanitized. There is no raw-payload mode, and logging does not modify the stored JSONB payload.

## Tests and CI

Run the full test suite and build:

```bash
OUTBOX_DOCKER_API_VERSION=1.40 ./gradlew clean test --no-daemon
OUTBOX_DOCKER_API_VERSION=1.40 ./gradlew build --no-daemon
```

Integration tests use PostgreSQL Testcontainers and require Docker. They cover API validation and atomic writes, R2DBC mapping, publisher rollback and at-least-once retry, concurrent `SKIP LOCKED` workers, cancellation and lock release, scheduler recovery, and logging redaction. GitHub Actions runs the Gradle Wrapper on a GitHub-hosted Ubuntu runner with Java 21 and uploads JUnit reports.

A two-JVM restart check was run manually against a disposable PostgreSQL container using `tmpfs`; it is not part of the automated CI suite.

For local PostgreSQL connectivity, start and stop Compose with `docker compose up -d postgres` and `docker compose down`. **Do not use `docker compose down -v`** unless intentionally deleting the persistent local database volume.

## Limitations

Publication is simulated by logging; there is no message broker or event consumer. Log output can repeat after a failure or restart. The publisher has no dead-letter queue or retry backoff, so a repeatedly failing event can hold up later events in a batch. The project also omits request idempotency keys, authentication, and production-grade observability.

## Implementations

- `spring-mvc-v1.0.0` — existing tag for the previous MVC implementation.
- `reactive-v1.0.0` — planned release tag for the reactive implementation; it has not been created.
