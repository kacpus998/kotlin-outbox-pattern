# Kotlin Outbox Pattern

Kotlin/Spring Boot implementation of the transactional outbox pattern. `POST /orders` stores an order and its `OrderCreated` outbox event atomically; publishing is intentionally not implemented yet.

## Requirements

- Java 21
- Docker Engine and Docker Compose

The Gradle Wrapper downloads the configured Gradle distribution on first use.

## Run locally

Start PostgreSQL:

```bash
docker compose up -d postgres
```

Run the application using the `local` profile (database defaults are configured for the Compose service):

```bash
./gradlew bootRun --args='--spring.profiles.active=local'
```

The application listens on port `8080`. Check readiness at <http://localhost:8080/actuator/health>.

Create an order:

```bash
curl -i -X POST http://localhost:8080/orders \
  -H 'Content-Type: application/json' \
  -d '{"customerId":"123"}'
```

A valid request returns `201 Created` with an `orderId`. A missing, null, empty, or blank `customerId` returns `400 Bad Request`. The order and its pending `OrderCreated` event are stored together; no event is published by the request.

Stop PostgreSQL with `docker compose down`. Add `-v` only if you also want to delete its persisted data.

## Test and build

```bash
./gradlew clean test
./gradlew build
```

The Spring integration test starts an isolated PostgreSQL instance using Testcontainers; Docker must be available when running tests.

## Next stages

1. OUTBOX-03: add scheduled processing for pending outbox events.
2. Log each processed event and set its `processed_at` timestamp.
