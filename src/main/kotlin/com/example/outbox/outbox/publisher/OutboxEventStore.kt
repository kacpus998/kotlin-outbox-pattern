package com.example.outbox.outbox.publisher

import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Repository
import java.sql.Timestamp
import java.time.Instant
import java.util.UUID

@Repository
class OutboxEventStore(
    private val jdbcTemplate: JdbcTemplate,
    private val objectMapper: ObjectMapper,
) {
    fun findAndLockUnprocessed(batchSize: Int): List<OutboxPendingEvent> = jdbcTemplate.query(
        """
        SELECT id, event_type, aggregate_id, payload::text AS payload, created_at
        FROM outbox_events
        WHERE processed_at IS NULL
        ORDER BY created_at, id
        LIMIT ?
        FOR UPDATE SKIP LOCKED
        """.trimIndent(),
        { rs, _ ->
            OutboxPendingEvent(
                id = rs.getObject("id", UUID::class.java),
                eventType = rs.getString("event_type"),
                aggregateId = rs.getObject("aggregate_id", UUID::class.java),
                payload = objectMapper.readTree(rs.getString("payload")),
                createdAt = rs.getTimestamp("created_at").toInstant(),
            )
        },
        batchSize,
    )

    fun markProcessed(id: UUID) {
        val updated = jdbcTemplate.update(
            """
        UPDATE outbox_events
        SET processed_at = ?
        WHERE id = ? AND processed_at IS NULL
        """.trimIndent(),
            Timestamp.from(Instant.now()),
            id,
        )

        check(updated == 1) {
            "Expected to mark one outbox event as processed: $id, updated=$updated"
        }
    }
}
