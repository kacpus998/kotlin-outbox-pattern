package ing.recruitment.outbox.publisher

import com.fasterxml.jackson.databind.ObjectMapper
import ing.recruitment.testsupport.PostgresIntegrationTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.transaction.reactive.TransactionalOperator
import reactor.test.StepVerifier
import java.time.Instant
import java.util.UUID

@SpringBootTest(properties = ["outbox.publisher.enabled=false"])
class ReactiveOutboxEventStoreIntegrationTest : PostgresIntegrationTest() {
    @Autowired
    private lateinit var store: OutboxEventStore

    @Autowired
    private lateinit var transactionalOperator: TransactionalOperator

    @Autowired
    private lateinit var jdbcTemplate: JdbcTemplate

    @Autowired
    private lateinit var objectMapper: ObjectMapper

    @BeforeEach
    fun clearEvents() {
        jdbcTemplate.update("DELETE FROM outbox_events")
        jdbcTemplate.update("DELETE FROM orders")
    }

    @Test
    fun `reactively selects a bounded ordered batch and maps PostgreSQL values`() {
        val earliest = insertEvent(Instant.parse("2026-01-01T00:00:00Z"))
        val next = insertEvent(Instant.parse("2026-01-01T00:00:01Z"))
        insertEvent(Instant.parse("2026-01-01T00:00:02Z"))

        StepVerifier.create(
            transactionalOperator.transactional(store.findAndLockUnprocessedReactive(2).collectList()),
        )
            .assertNext { events ->
                assertEquals(listOf(earliest.id, next.id), events.map { it.id })
                assertEquals(earliest.aggregateId, events.first().aggregateId)
                assertEquals("OrderCreated", events.first().eventType)
                assertEquals("customer-${earliest.aggregateId}", events.first().payload["customerId"].asText())
                assertEquals(Instant.parse("2026-01-01T00:00:00Z"), events.first().createdAt)
            }
            .verifyComplete()
    }

    @Test
    fun `reactive selection returns empty batch when no events are pending`() {
        StepVerifier.create(
            transactionalOperator.transactional(store.findAndLockUnprocessedReactive(10).collectList()),
        )
            .assertNext { assertTrue(it.isEmpty()) }
            .verifyComplete()
    }

    @Test
    fun `reactive processed update changes exactly one pending row`() {
        val event = insertEvent(Instant.parse("2026-01-01T00:00:00Z"))

        StepVerifier.create(transactionalOperator.transactional(store.markProcessedReactive(event.id)))
            .verifyComplete()

        val processedAt = jdbcTemplate.queryForObject(
            "SELECT processed_at FROM outbox_events WHERE id = ?",
            { rs, _ -> rs.getTimestamp("processed_at") },
            event.id,
        )
        assertNotNull(processedAt)
    }

    @Test
    fun `reactive processed update errors when no pending row matches`() {
        val missingId = UUID.randomUUID()

        StepVerifier.create(store.markProcessedReactive(missingId))
            .expectErrorSatisfies { failure ->
                assertTrue(failure.message.orEmpty().contains("updated=0"))
            }
            .verify()

        assertFalse(eventExists(missingId))
    }

    private fun insertEvent(createdAt: Instant): OutboxPendingEvent {
        val id = UUID.randomUUID()
        val aggregateId = UUID.randomUUID()
        val payload = objectMapper.writeValueAsString(
            mapOf("orderId" to aggregateId.toString(), "customerId" to "customer-$aggregateId"),
        )
        jdbcTemplate.update(
            "INSERT INTO orders (id, customer_id, created_at) VALUES (?, ?, ?)",
            aggregateId,
            "customer-$aggregateId",
            java.sql.Timestamp.from(createdAt),
        )
        jdbcTemplate.update(
            """
            INSERT INTO outbox_events (id, aggregate_id, event_type, payload, created_at)
            VALUES (?, ?, ?, CAST(? AS jsonb), ?)
            """.trimIndent(),
            id,
            aggregateId,
            "OrderCreated",
            payload,
            java.sql.Timestamp.from(createdAt),
        )
        return OutboxPendingEvent(
            id = id,
            eventType = "OrderCreated",
            aggregateId = aggregateId,
            payload = objectMapper.readTree(payload),
            createdAt = createdAt,
        )
    }

    private fun eventExists(id: UUID): Boolean =
        jdbcTemplate.queryForObject("SELECT COUNT(*) FROM outbox_events WHERE id = ?", Int::class.java, id) == 1
}
