package ing.recruitment.outbox.publisher

import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.stereotype.Repository
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.util.UUID

@Repository
class OutboxEventStore(
    private val databaseClient: DatabaseClient,
    private val objectMapper: ObjectMapper,
) {
    fun findAndLockUnprocessedReactive(batchSize: Int): Flux<OutboxPendingEvent> {
        require(batchSize > 0) { "batchSize must be positive" }

        return databaseClient.sql(
            """
            SELECT id, event_type, aggregate_id, payload::text AS payload, created_at
            FROM outbox_events
            WHERE processed_at IS NULL
            ORDER BY created_at, id
            LIMIT :batchSize
            FOR UPDATE SKIP LOCKED
            """.trimIndent(),
        )
            .bind("batchSize", batchSize)
            .map { row, _ ->
                OutboxPendingEvent(
                    id = requireNotNull(row.get("id", UUID::class.java)),
                    eventType = requireNotNull(row.get("event_type", String::class.java)),
                    aggregateId = requireNotNull(row.get("aggregate_id", UUID::class.java)),
                    payload = objectMapper.readTree(requireNotNull(row.get("payload", String::class.java))),
                    createdAt = requireNotNull(row.get("created_at", OffsetDateTime::class.java)).toInstant(),
                )
            }
            .all()
    }

    fun markProcessedReactive(id: UUID): Mono<Void> = databaseClient.sql(
        """
        UPDATE outbox_events
        SET processed_at = :processedAt
        WHERE id = :eventId AND processed_at IS NULL
        """.trimIndent(),
    )
        .bind("processedAt", OffsetDateTime.now(ZoneOffset.UTC))
        .bind("eventId", id)
        .fetch()
        .rowsUpdated()
        .flatMap { updated ->
            if (updated == 1L) {
                Mono.empty()
            } else {
                Mono.error(
                    IllegalStateException("Expected to update exactly one outbox event as processed: $id, updated=$updated"),
                )
            }
        }

}
