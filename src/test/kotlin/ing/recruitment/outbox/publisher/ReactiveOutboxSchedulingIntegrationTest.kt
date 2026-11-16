package ing.recruitment.outbox.publisher

import ing.recruitment.orders.application.OrderService
import ing.recruitment.testsupport.PostgresIntegrationTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.mockito.Mockito.doAnswer
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.context.annotation.AnnotationConfigApplicationContext
import org.springframework.core.env.MapPropertySource
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.test.context.bean.override.mockito.MockitoBean
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean
import reactor.core.publisher.Flux
import reactor.test.StepVerifier
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

@SpringBootTest(
    properties = [
        "outbox.publisher.enabled=false",
        "outbox.publisher.fixed-delay-ms=100",
        "outbox.publisher.batch-size=1",
    ],
)
class ReactiveOutboxSchedulingIntegrationTest : PostgresIntegrationTest() {
    @Autowired
    private lateinit var orderService: OrderService

    @Autowired
    private lateinit var jdbcTemplate: JdbcTemplate

    @Autowired
    private lateinit var databaseClient: DatabaseClient

    @Autowired
    private lateinit var processingService: OutboxProcessingService

    @MockitoSpyBean
    private lateinit var store: OutboxEventStore

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

        val scheduler = startScheduler()
        try {
            awaitCondition {
                eventIds.all { id -> processedAt(id) != null } && capturedTransactionIds(eventIds).size == 2
            }

            assertEquals(2, capturedTransactionIds(eventIds).size)
        } finally {
            scheduler.close()
        }
    }

    @Test
    fun `failed scheduled cycle is retried by a later cycle`() {
        val attempts = AtomicInteger()
        val publishedIds = CopyOnWriteArrayList<UUID>()
        doAnswer {
            publishedIds.add(it.getArgument<OutboxPendingEvent>(0).id)
            if (attempts.incrementAndGet() == 1) throw IllegalStateException("simulated cycle failure")
            null
        }.`when`(eventLogger).publish(anyEvent())

        val orderId = createOrder("scheduled-retry")
        val eventId = jdbcTemplate.queryForObject(
            "SELECT id FROM outbox_events WHERE aggregate_id = ?",
            UUID::class.java,
            orderId,
        )!!

        val scheduler = startScheduler()
        try {
            awaitCondition { processedAt(eventId) != null && attempts.get() >= 2 }

            assertEquals(2, attempts.get())
            assertEquals(listOf(eventId, eventId), publishedIds)
            assertNotNull(processedAt(eventId))
        } finally {
            scheduler.close()
        }
    }

    @Test
    fun `PostgreSQL SELECT failure rolls back and a later scheduled transaction recovers`() {
        val failNextSelection = AtomicBoolean(false)
        val allowRecovery = AtomicBoolean(false)
        val selectionFailed = CountDownLatch(1)
        val selectionFailure = AtomicReference<Throwable>()
        val failedBackendPid = AtomicInteger()
        val failedTransactionId = AtomicReference<Long>()
        val publishedIds = CopyOnWriteArrayList<UUID>()
        val orderId = createOrder("scheduled-select-recovery")
        val eventId = jdbcTemplate.queryForObject(
            "SELECT id FROM outbox_events WHERE aggregate_id = ?", UUID::class.java, orderId,
        )!!
        doAnswer { invocation ->
            Flux.defer {
                if (failNextSelection.compareAndSet(true, false)) {
                    databaseClient.sql(
                        "SELECT pg_backend_pid() AS backend_pid, txid_current() AS transaction_id",
                    )
                        .map { row, _ ->
                            row.get("backend_pid", Int::class.java)!! to row.get("transaction_id", Long::class.java)!!
                        }
                        .one()
                        .flatMapMany { (backendPid, transactionId) ->
                            failedBackendPid.set(backendPid)
                            failedTransactionId.set(transactionId)
                            databaseClient.sql(
                                "INSERT INTO outbox_test_transaction_ids(event_id, transaction_id) " +
                                    "VALUES (:eventId, :transactionId)",
                            )
                                .bind("eventId", eventId)
                                .bind("transactionId", transactionId)
                                .fetch()
                                .rowsUpdated()
                                .thenMany(
                                    databaseClient.sql(
                                        "SELECT id FROM outbox_test_missing_relation_06d FOR UPDATE SKIP LOCKED",
                                    )
                                        .map<OutboxPendingEvent> { _, _ ->
                                            error("missing relation SELECT must not return a row")
                                        }
                                        .all(),
                                )
                        }
                        .doOnError {
                            selectionFailure.set(it)
                            selectionFailed.countDown()
                        }
                } else if (allowRecovery.get()) {
                    @Suppress("UNCHECKED_CAST")
                    (invocation.callRealMethod() as Flux<OutboxPendingEvent>)
                } else {
                    Flux.empty()
                }
            }
        }.`when`(store).findAndLockUnprocessedReactive(1)
        doAnswer {
            publishedIds.add(it.getArgument<OutboxPendingEvent>(0).id)
            null
        }.`when`(eventLogger).publish(anyEvent())

        failNextSelection.set(true)
        val scheduler = startScheduler()
        try {
            assertTrue(selectionFailed.await(10, TimeUnit.SECONDS), "scheduled SELECT did not fail")
            val rootCause = generateSequence(selectionFailure.get()) { it.cause }.last()
            assertTrue(rootCause is io.r2dbc.spi.R2dbcException)
            assertEquals("42P01", (rootCause as io.r2dbc.spi.R2dbcException).sqlState)
            assertTrue(publishedIds.isEmpty(), "failed selection must not publish anything")
            awaitCondition {
                jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM pg_stat_activity WHERE pid = ? AND backend_xid::text = ?",
                    Int::class.java,
                    failedBackendPid.get(),
                    failedTransactionId.get().toString(),
                ) == 0
            }
            assertEquals(
                0,
                jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM outbox_test_transaction_ids WHERE event_id = ?",
                    Int::class.java,
                    eventId,
                ),
                "the diagnostic write made before SELECT failure must roll back",
            )
            assertEquals(1, jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM outbox_events WHERE id = ? AND processed_at IS NULL", Int::class.java, eventId,
            ))

            allowRecovery.set(true)
            awaitCondition { processedAt(eventId) != null }
            assertEquals(listOf(eventId), publishedIds)
            val recoveryTransactionId = capturedTransactionId(eventId)
            assertNotEquals(failedTransactionId.get(), recoveryTransactionId)
        } finally {
            scheduler.close()
        }
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

    private fun capturedTransactionId(id: UUID): Long = jdbcTemplate.queryForObject(
        "SELECT transaction_id FROM outbox_test_transaction_ids WHERE event_id = ?",
        Long::class.java,
        id,
    )!!

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

    private fun startScheduler(): AnnotationConfigApplicationContext = AnnotationConfigApplicationContext().apply {
        environment.propertySources.addFirst(
            MapPropertySource(
                "outbox-test-scheduler",
                mapOf("outbox.publisher.fixed-delay-ms" to "100"),
            ),
        )
        register(SchedulingOnly::class.java)
        registerBean(
            "scheduledPublisher",
            OutboxPublisher::class.java,
            java.util.function.Supplier { OutboxPublisher(processingService) },
        )
        refresh()
    }

    @EnableScheduling
    private class SchedulingOnly
}
