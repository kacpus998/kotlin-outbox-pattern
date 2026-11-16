package ing.recruitment.outbox.publisher

import ing.recruitment.orders.application.OrderService
import ing.recruitment.testsupport.PostgresIntegrationTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.doAnswer
import org.mockito.Mockito.doThrow
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.context.bean.override.mockito.MockitoBean
import reactor.test.StepVerifier
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

@SpringBootTest(
    properties = [
        "outbox.publisher.enabled=false",
        "outbox.publisher.batch-size=2",
    ],
)
class OutboxPublisherIntegrationTest : PostgresIntegrationTest() {
    @Autowired private lateinit var orderService: OrderService
    @Autowired private lateinit var processingService: OutboxProcessingService
    @Autowired private lateinit var jdbcTemplate: JdbcTemplate

    @MockitoBean
    private lateinit var eventLogger: OutboxEventLogger

    @BeforeEach
    fun cleanDatabase() {
        jdbcTemplate.update("DELETE FROM outbox_events")
        jdbcTemplate.update("DELETE FROM orders")
        org.mockito.Mockito.reset(eventLogger)
    }

    @AfterEach
    fun clearLogger() {
        org.mockito.Mockito.reset(eventLogger)
    }

    @Test
    fun `publishes order event and marks it processed`() {
        val orderId = createOrder("customer-123")
        val eventId = jdbcTemplate.queryForObject(
            "SELECT id FROM outbox_events WHERE aggregate_id = ?",
            UUID::class.java,
            orderId,
        )!!
        assertNull(processedAt(eventId))

        assertEquals(1, processingService.processBatch())

        assertNotNull(processedAt(eventId))
        val event = OutboxPendingEvent(
            eventId,
            "OrderCreated",
            orderId,
            jdbcTemplate.queryForObject(
                "SELECT payload FROM outbox_events WHERE id = ?",
                { rs, _ -> com.fasterxml.jackson.databind.ObjectMapper().readTree(rs.getString(1)) },
                eventId,
            )!!,
            jdbcTemplate.queryForObject(
                "SELECT created_at FROM outbox_events WHERE id = ?",
                { rs, _ -> rs.getTimestamp(1).toInstant() },
                eventId,
            )!!,
        )
        verify(eventLogger).publish(event)
    }

    @Test
    fun `second cycle does not reprocess committed event`() {
        val orderId = createOrder("customer-123")

        assertEquals(1, processingService.processBatch())
        assertEquals(0, processingService.processBatch())

        verify(eventLogger, times(1)).publish(anyEvent())
        assertEquals(1, jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = ? AND processed_at IS NOT NULL",
            Int::class.java,
            orderId,
        ))
    }

    @Test
    fun `logger failure rolls back and event can be retried`() {
        val orderId = createOrder("customer-123")
        doThrow(IllegalStateException("simulated logger failure"))
            .doNothing()
            .`when`(eventLogger).publish(anyEvent())

        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException::class.java) {
            processingService.processBatch()
        }
        val eventId = jdbcTemplate.queryForObject(
            "SELECT id FROM outbox_events WHERE aggregate_id = ?",
            UUID::class.java,
            orderId,
        )!!
        assertNull(processedAt(eventId))

        assertEquals(1, processingService.processBatch())
        assertNotNull(processedAt(eventId))
    }

    @Test
    fun `failure after external publication before commit retries same event id`() {
        val orderId = createOrder("customer-123")
        val publishedIds = java.util.Collections.synchronizedList(mutableListOf<UUID>())
        doAnswer {
            publishedIds.add(it.getArgument<OutboxPendingEvent>(0).id)
            null
        }.`when`(eventLogger).publish(anyEvent())
        val eventId = jdbcTemplate.queryForObject(
            "SELECT id FROM outbox_events WHERE aggregate_id = ?", UUID::class.java, orderId,
        )!!
        jdbcTemplate.execute(
            """
            CREATE FUNCTION fail_processed_update() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN RAISE EXCEPTION 'forced failure after publication'; END;
            $$
            """.trimIndent(),
        )
        jdbcTemplate.execute(
            "CREATE TRIGGER fail_processed_update BEFORE UPDATE OF processed_at ON outbox_events " +
                "FOR EACH ROW EXECUTE FUNCTION fail_processed_update()",
        )
        try {
            org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException::class.java) {
                processingService.processBatch()
            }
            assertNull(processedAt(eventId))
            assertEquals(listOf(eventId), publishedIds)
        } finally {
            jdbcTemplate.execute("DROP TRIGGER IF EXISTS fail_processed_update ON outbox_events")
            jdbcTemplate.execute("DROP FUNCTION IF EXISTS fail_processed_update()")
        }

        assertEquals(1, processingService.processBatch())
        assertNotNull(processedAt(eventId))
        assertEquals(listOf(eventId, eventId), publishedIds)
    }

    @Test
    fun `concurrent workers skip locked records and process all records once`() {
        repeat(4) { createOrder("customer-$it") }
        val firstLoggerEntered = CountDownLatch(1)
        val releaseFirstWorker = CountDownLatch(1)
        val calls = AtomicInteger()
        val publishedIds = ConcurrentHashMap.newKeySet<UUID>()
        doAnswer {
            val event = it.getArgument<OutboxPendingEvent>(0)
            check(publishedIds.add(event.id)) { "event ${event.id} was concurrently published twice" }
            if (calls.incrementAndGet() == 1) {
                firstLoggerEntered.countDown()
                check(releaseFirstWorker.await(10, TimeUnit.SECONDS))
            }
            null
        }.`when`(eventLogger).publish(anyEvent())

        val workers = Executors.newFixedThreadPool(2)
        try {
            val first = workers.submit<Int> { processingService.processBatch() }
            assertTrue(firstLoggerEntered.await(10, TimeUnit.SECONDS), "first worker did not acquire a row lock")
            val second = workers.submit<Int> { processingService.processBatch() }

            assertEquals(2, second.get(10, TimeUnit.SECONDS))
            releaseFirstWorker.countDown()
            assertEquals(2, first.get(10, TimeUnit.SECONDS))
        } finally {
            releaseFirstWorker.countDown()
            workers.shutdownNow()
        }

        assertEquals(4, jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM outbox_events WHERE processed_at IS NOT NULL",
            Int::class.java,
        ))
        assertEquals(4, calls.get())
        assertEquals(4, publishedIds.size)
    }

    @Test
    fun `one cycle processes no more than configured batch size`() {
        repeat(3) { createOrder("customer-$it") }

        assertEquals(2, processingService.processBatch())
        assertEquals(2, jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM outbox_events WHERE processed_at IS NOT NULL",
            Int::class.java,
        ))
        assertEquals(1, jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM outbox_events WHERE processed_at IS NULL",
            Int::class.java,
        ))
    }

    private fun processedAt(id: UUID): java.sql.Timestamp? = jdbcTemplate.queryForObject(
        "SELECT processed_at FROM outbox_events WHERE id = ?",
        { rs, _ -> rs.getTimestamp(1) },
        id,
    )

    private fun createOrder(customerId: String): UUID {
        var orderId: UUID? = null
        StepVerifier.create(orderService.createOrder(customerId))
            .assertNext { orderId = it }
            .verifyComplete()
        return requireNotNull(orderId)
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
