package ing.recruitment.outbox.publisher

import ing.recruitment.orders.application.OrderService
import ing.recruitment.testsupport.PostgresIntegrationTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.doAnswer
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.context.bean.override.mockito.MockitoBean
import reactor.test.StepVerifier
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

@SpringBootTest(
    properties = [
        "outbox.publisher.enabled=true",
        "outbox.publisher.fixed-delay-ms=100",
        "outbox.publisher.batch-size=1",
    ],
)
class ReactiveOutboxSchedulingIntegrationTest : PostgresIntegrationTest() {
    @Autowired
    private lateinit var orderService: OrderService

    @Autowired
    private lateinit var jdbcTemplate: JdbcTemplate

    @MockitoBean
    private lateinit var eventLogger: OutboxEventLogger

    @BeforeEach
    fun prepareDatabase() {
        dropTransactionCapture()
        jdbcTemplate.update("DELETE FROM outbox_events")
        jdbcTemplate.update("DELETE FROM orders")
        jdbcTemplate.execute(
            "CREATE TABLE outbox_test_transaction_ids (event_id UUID NOT NULL, transaction_id BIGINT NOT NULL)",
        )
        jdbcTemplate.execute(
            """
            CREATE FUNCTION capture_outbox_transaction_id() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                INSERT INTO outbox_test_transaction_ids(event_id, transaction_id)
                VALUES (NEW.id, txid_current());
                RETURN NEW;
            END;
            $$
            """.trimIndent(),
        )
        jdbcTemplate.execute(
            "CREATE TRIGGER capture_outbox_transaction_id BEFORE UPDATE OF processed_at ON outbox_events " +
                "FOR EACH ROW EXECUTE FUNCTION capture_outbox_transaction_id()",
        )
        org.mockito.Mockito.reset(eventLogger)
    }

    @AfterEach
    fun removeTransactionCapture() {
        dropTransactionCapture()
    }

    @Test
    fun `each scheduled cycle processes a new batch in a distinct PostgreSQL transaction`() {
        val firstOrderId = createOrder("scheduled-one")
        val secondOrderId = createOrder("scheduled-two")
        val eventIds = listOf(firstOrderId, secondOrderId).map { orderId ->
            jdbcTemplate.queryForObject(
                "SELECT id FROM outbox_events WHERE aggregate_id = ?",
                UUID::class.java,
                orderId,
            )!!
        }

        awaitCondition {
            eventIds.all { id -> processedAt(id) != null } && capturedTransactionIds(eventIds).size == 2
        }

        assertEquals(2, capturedTransactionIds(eventIds).size)
    }

    @Test
    fun `failed scheduled cycle is retried by a later cycle`() {
        val orderId = createOrder("scheduled-retry")
        val eventId = jdbcTemplate.queryForObject(
            "SELECT id FROM outbox_events WHERE aggregate_id = ?",
            UUID::class.java,
            orderId,
        )!!
        val attempts = AtomicInteger()
        val publishedIds = CopyOnWriteArrayList<UUID>()
        doAnswer {
            publishedIds.add(it.getArgument<OutboxPendingEvent>(0).id)
            if (attempts.incrementAndGet() == 1) throw IllegalStateException("simulated cycle failure")
            null
        }.`when`(eventLogger).publish(anyEvent())

        awaitCondition { processedAt(eventId) != null && attempts.get() >= 2 }

        assertEquals(2, attempts.get())
        assertEquals(listOf(eventId, eventId), publishedIds)
        assertNotNull(processedAt(eventId))
    }

    private fun createOrder(customerId: String): UUID {
        var orderId: UUID? = null
        StepVerifier.create(orderService.createOrder(customerId))
            .assertNext { orderId = it }
            .verifyComplete()
        return requireNotNull(orderId)
    }

    private fun processedAt(id: UUID): java.sql.Timestamp? = jdbcTemplate.queryForObject(
        "SELECT processed_at FROM outbox_events WHERE id = ?",
        { rs, _ -> rs.getTimestamp("processed_at") },
        id,
    )

    private fun capturedTransactionIds(ids: List<UUID>): Set<Long> = jdbcTemplate.query(
        "SELECT DISTINCT transaction_id FROM outbox_test_transaction_ids WHERE event_id IN (?, ?)",
        { rs, _ -> rs.getLong("transaction_id") },
        ids[0],
        ids[1],
    ).toSet()

    private fun awaitCondition(condition: () -> Boolean) {
        val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10)
        while (System.nanoTime() < deadline) {
            if (condition()) return
            Thread.sleep(10)
        }
        assertTrue(condition(), "scheduled publisher condition was not reached within 10 seconds")
    }

    private fun dropTransactionCapture() {
        jdbcTemplate.execute("DROP TRIGGER IF EXISTS capture_outbox_transaction_id ON outbox_events")
        jdbcTemplate.execute("DROP FUNCTION IF EXISTS capture_outbox_transaction_id()")
        jdbcTemplate.execute("DROP TABLE IF EXISTS outbox_test_transaction_ids")
    }

    private fun anyEvent(): OutboxPendingEvent =
        org.mockito.ArgumentMatchers.any(OutboxPendingEvent::class.java)
            ?: OutboxPendingEvent(
                UUID.randomUUID(),
                "matcher",
                UUID.randomUUID(),
                com.fasterxml.jackson.databind.node.JsonNodeFactory.instance.objectNode(),
                java.time.Instant.EPOCH,
            )
}
