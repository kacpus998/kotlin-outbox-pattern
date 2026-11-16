# Kotlin Outbox Pattern

Kotlin/Spring Boot example of the transactional outbox pattern. Creating an order persists both the order and its `OrderCreated` event in one database transaction. A scheduled publisher logs pending events and marks them processed.

## Run locally

Requirements: Java 21, Docker Engine, Docker Compose.

Start PostgreSQL and the application:

```bash
docker compose up -d postgres
./gradlew bootRun --args='--spring.profiles.active=local'
```

Create an order (HTTP `201 Created` returns its `orderId`):

```bash
curl -i -X POST http://localhost:8080/orders \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"123"}'
```

The publisher starts with the application. Configure it in `application.yml` or override the properties at startup:

```yaml
outbox:
  publisher:
    enabled: true
    fixed-delay-ms: 5000
    batch-size: 10
```

`enabled: false` disables the scheduler. One cycle processes at most `batch-size` pending events.

Example log:

```text
Publishing outbox event id=... type=OrderCreated aggregateId=... payload={"orderId":"...","customerId":"123"}
```

Check processing state in PostgreSQL:

```sql
SELECT id, event_type, aggregate_id, payload, processed_at
FROM outbox_events
ORDER BY created_at, id;
```

## Delivery and concurrency semantics

The order and outbox row are written atomically. The publisher selects pending rows in deterministic order using PostgreSQL `FOR UPDATE SKIP LOCKED`; row locks remain held while the batch is logged and updated in the same transaction. Other publisher instances skip rows already locked by a worker.

Delivery is **at-least-once**, not exactly-once. If the application exits after a line is written to the log but before the database transaction commits, the row remains pending and can be logged again after restart. `SKIP LOCKED` prevents two live transactions from acquiring the same row simultaneously; it cannot atomically commit an external side effect and the database update. `processed_at` is written only after successful logging so a logging failure leaves the event retryable. A production consumer must therefore be idempotent. Processing is atomic per batch: a failure rolls back every row update in that batch, so earlier events in the batch can be delivered again.

Stop PostgreSQL with `docker compose down`. Add `-v` only to delete its persisted data.

## Tests

```bash
./gradlew clean test
./gradlew build
```

Publisher unit tests run without Spring or Docker. Integration tests use PostgreSQL Testcontainers and require Docker.

## Continuous Integration

The GitHub Actions workflow in `.github/workflows/ci.yml` runs on pushes to `main` and `feat/**`, pull requests targeting `main`, and manual dispatch. It validates the Gradle Wrapper and runs `./gradlew clean build --no-daemon` on a GitHub-hosted Ubuntu runner with Eclipse Temurin Java 21. The build includes both unit and integration tests; Testcontainers starts its own PostgreSQL container, so CI does not need Docker Compose or a separately configured database.

The workflow uploads Gradle's JUnit XML and HTML test reports as the `test-reports` artifact even when the build fails. Download the artifact from the workflow run's **Artifacts** section; failed test details are also available in the **Build and test** job logs. Locally, run the same build with `./gradlew clean build --no-daemon` (Docker must be available for integration tests). On Docker Engine 29 or newer, where the minimum API is 1.40, use `OUTBOX_DOCKER_API_VERSION=1.40 ./gradlew clean build --no-daemon`; the workflow sets the same compatibility override.
