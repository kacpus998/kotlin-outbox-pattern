# Kotlin Outbox Pattern

Minimal Spring Boot foundation for a transactional outbox implementation. This bootstrap does not yet include order APIs, domain entities, or an event publisher.

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

Stop PostgreSQL with `docker compose down`. Add `-v` only if you also want to delete its persisted data.

## Test and build

```bash
./gradlew clean test
./gradlew build
```

The Spring integration test starts an isolated PostgreSQL instance using Testcontainers; Docker must be available when running tests.

## Next stages

1. OUTBOX-02: add Flyway schema migrations for `orders` and `outbox_events`.
2. Add transactional order creation and persist its outbox event in the same transaction.
3. Add scheduled outbox processing, event logging, and processed-state handling.
