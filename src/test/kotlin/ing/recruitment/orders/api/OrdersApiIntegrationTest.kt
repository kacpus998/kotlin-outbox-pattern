package ing.recruitment.orders.api

import com.fasterxml.jackson.databind.ObjectMapper
import ing.recruitment.orders.application.OrderService
import ing.recruitment.testsupport.PostgresIntegrationTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.http.MediaType
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.context.ApplicationContext
import org.springframework.boot.web.reactive.context.ReactiveWebServerApplicationContext
import org.springframework.test.web.reactive.server.WebTestClient
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import java.util.UUID
import org.springframework.test.context.bean.override.mockito.MockitoBean
import ing.recruitment.outbox.publisher.OutboxEventLogger
import ing.recruitment.outbox.publisher.OutboxProcessingService
import java.util.concurrent.CopyOnWriteArrayList

@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = ["outbox.publisher.enabled=false"],
)
class OrdersApiIntegrationTest : PostgresIntegrationTest() {
    @Autowired
    private lateinit var webTestClient: WebTestClient

    @Autowired
    private lateinit var jdbcTemplate: JdbcTemplate

    @Autowired
    private lateinit var objectMapper: ObjectMapper

    @Autowired
    private lateinit var applicationContext: ApplicationContext

    @Autowired
    private lateinit var orderService: OrderService

    @Autowired
    private lateinit var processingService: OutboxProcessingService

    @MockitoBean
    private lateinit var eventLogger: OutboxEventLogger

    @BeforeEach
    fun cleanDatabase() {
        removeFailureTrigger()
        jdbcTemplate.update("DELETE FROM outbox_events")
        jdbcTemplate.update("DELETE FROM orders")
    }

    @AfterEach
    fun removeTestTriggerAndFunction() {
        removeFailureTrigger()
    }

    @Test
    fun `POST orders creates an order and matching pending OrderCreated event`() {
        val orderId = createOrder("""{"customerId":"customer-123"}""")
        assertEquals(1, jdbcTemplate.queryForObject("SELECT COUNT(*) FROM orders", Int::class.java))
        assertEquals(1, jdbcTemplate.queryForObject("SELECT COUNT(*) FROM outbox_events", Int::class.java))

        val order = jdbcTemplate.queryForMap("SELECT id, customer_id, created_at FROM orders WHERE id = ?", orderId)
        assertEquals(orderId, order["id"])
        assertEquals("customer-123", order["customer_id"])
        assertNotNull(order["created_at"])

        val event = jdbcTemplate.queryForMap(
            "SELECT aggregate_id, event_type, payload::text AS payload, created_at, processed_at FROM outbox_events",
        )
        assertEquals(orderId, event["aggregate_id"])
        assertEquals("OrderCreated", event["event_type"])
        assertNotNull(event["created_at"])
        assertNull(event["processed_at"])
        val payload = objectMapper.readTree(event["payload"].toString())
        assertEquals(orderId.toString(), payload.get("orderId").asText())
        assertEquals("customer-123", payload.get("customerId").asText())
    }

    @Test
    fun `application runs on the reactive WebFlux server`() {
        assertTrue(applicationContext is ReactiveWebServerApplicationContext)
    }

    @Test
    fun `order creation publisher is lazy and writes only when subscribed`() {
        val publisher: Mono<UUID> = orderService.createOrder("lazy-customer")
        assertNoOrderWrites()

        StepVerifier.create(publisher)
            .assertNext { assertNotNull(it) }
            .verifyComplete()

        assertEquals(1, jdbcTemplate.queryForObject("SELECT COUNT(*) FROM orders", Int::class.java))
        assertEquals(1, jdbcTemplate.queryForObject("SELECT COUNT(*) FROM outbox_events", Int::class.java))
    }

    @Test
    fun `resubscribing to the same order publisher creates independent orders and events`() {
        val publisher = orderService.createOrder("resubscribed-customer")
        val orderIds = mutableListOf<UUID>()

        repeat(2) {
            StepVerifier.create(publisher)
                .assertNext(orderIds::add)
                .verifyComplete()
        }

        assertEquals(2, orderIds.distinct().size)
        assertEquals(2, jdbcTemplate.queryForObject("SELECT COUNT(*) FROM orders", Int::class.java))
        assertEquals(2, jdbcTemplate.queryForObject("SELECT COUNT(*) FROM outbox_events", Int::class.java))
        assertEquals(
            2,
            jdbcTemplate.queryForObject("SELECT COUNT(DISTINCT id) FROM outbox_events", Int::class.java),
        )
        assertEquals(
            orderIds.toSet(),
            jdbcTemplate.query("SELECT id FROM orders") { rs, _ -> rs.getObject("id", UUID::class.java) }.toSet(),
        )
        assertEquals(
            orderIds.toSet(),
            jdbcTemplate.query("SELECT aggregate_id FROM outbox_events") { rs, _ ->
                rs.getObject("aggregate_id", UUID::class.java)
            }.toSet(),
        )
    }

    @Test
    fun `HTTP order creation returns with pending event and reactive publisher processes it later`() {
        val publishedEventIds = CopyOnWriteArrayList<UUID>()
        org.mockito.Mockito.doAnswer {
            publishedEventIds.add(it.getArgument<ing.recruitment.outbox.publisher.OutboxPendingEvent>(0).id)
            null
        }.`when`(eventLogger).publish(anyEvent())
        val orderId = createOrder("""{"customerId":"http-outbox-publisher"}""")
        val eventId = jdbcTemplate.queryForObject(
            "SELECT id FROM outbox_events WHERE aggregate_id = ?",
            UUID::class.java,
            orderId,
        )!!
        assertNull(jdbcTemplate.queryForObject(
            "SELECT processed_at FROM outbox_events WHERE id = ?",
            { rs, _ -> rs.getTimestamp("processed_at") },
            eventId,
        ))

        StepVerifier.create(processingService.processBatch())
            .expectNext(1)
            .verifyComplete()

        assertNotNull(jdbcTemplate.queryForObject(
            "SELECT processed_at FROM outbox_events WHERE id = ?",
            { rs, _ -> rs.getTimestamp("processed_at") },
            eventId,
        ))
        assertEquals(listOf(eventId), publishedEventIds)
    }

    @Test
    fun `repeated identical requests create distinct orders and events`() {
        val first = createOrder("""{"customerId":"123"}""")
        val second = createOrder("""{"customerId":"123"}""")

        assertNotEquals(first, second)
        assertEquals(2, jdbcTemplate.queryForObject("SELECT COUNT(*) FROM orders", Int::class.java))
        assertEquals(2, jdbcTemplate.queryForObject("SELECT COUNT(*) FROM outbox_events", Int::class.java))
        assertEquals(2, jdbcTemplate.queryForObject("SELECT COUNT(DISTINCT id) FROM outbox_events", Int::class.java))
    }

    @Test
    fun `customerId longer than database column limit returns bad request`() {
        val body = exchange("""{"customerId":"${"x".repeat(256)}"}""")
            .expectStatus()
            .isBadRequest
            .expectBody(String::class.java)
            .returnResult()
            .responseBody
        assertSafeErrorBody(body)
        assertNoOrderWrites()
    }

    @Test
    fun `customerId at database column limit is accepted`() {
        val customerId = "x".repeat(255)

        val orderId = createOrder("""{"customerId":"$customerId"}""")

        assertEquals(
            customerId,
            jdbcTemplate.queryForObject("SELECT customer_id FROM orders WHERE id = ?", String::class.java, orderId),
        )
    }

    @ParameterizedTest
    @ValueSource(strings = ["{}", """{"customerId":null}""", """{"customerId":""}""", """{"customerId":"   "}"""])
    fun `invalid customerId returns bad request without writes`(request: String) {
        val body = exchange(request)
            .expectStatus()
            .isBadRequest
            .expectBody(String::class.java)
            .returnResult()
            .responseBody
        assertSafeErrorBody(body)
        assertNoOrderWrites()
    }

    @Test
    fun `malformed JSON returns bad request without writes`() {
        val body = webTestClient.post()
            .uri("/orders")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue("""{"customerId":"""")
            .exchange()
            .expectStatus()
            .isBadRequest
            .expectBody(String::class.java)
            .returnResult()
            .responseBody

        assertSafeErrorBody(body)
        assertNoOrderWrites()
    }

    @ParameterizedTest
    @ValueSource(
        strings = [
            """{"customerId":123}""",
            """{"customerId":true}""",
            """{"customerId":false}""",
            """{"customerId":[]}""",
            """{"customerId":{}}""",
        ],
    )
    fun `customerId with invalid JSON type returns bad request without writes`(request: String) {
        val body = exchange(request)
            .expectStatus()
            .isBadRequest
            .expectBody(String::class.java)
            .returnResult()
            .responseBody
        assertSafeErrorBody(body)
        assertNoOrderWrites()
    }

    @Test
    fun `unsupported content type returns error without writes`() {
        val body = webTestClient.post()
            .uri("/orders")
            .contentType(MediaType.TEXT_PLAIN)
            .bodyValue("customer-123")
            .exchange()
            .expectStatus()
            .isEqualTo(415)
            .expectBody(String::class.java)
            .returnResult()
            .responseBody

        assertSafeErrorBody(body)
        assertNoOrderWrites()
    }

    @Test
    fun `outbox insert failure returns an error and rolls back the order`() {
        jdbcTemplate.execute(
            """
            CREATE FUNCTION fail_outbox_insert() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                RAISE EXCEPTION 'forced outbox insert failure';
            END;
            $$
            """.trimIndent(),
        )
        jdbcTemplate.execute(
            "CREATE TRIGGER fail_outbox_insert BEFORE INSERT ON outbox_events " +
                "FOR EACH ROW EXECUTE FUNCTION fail_outbox_insert()",
        )

        StepVerifier.create(orderService.createOrder("customer-direct-error"))
            .expectError()
            .verify()
        assertNoOrderWrites()

        val responseBody = exchange("""{"customerId":"customer-rollback"}""")
            .expectStatus()
            .is5xxServerError
            .expectBody(String::class.java)
            .returnResult()
            .responseBody

        assertSafeErrorBody(responseBody)
        assertNoOrderWrites()
    }

    @Test
    fun `zero-row order insert returns an error without writes`() {
        suppressInsert("orders", "suppress_order_insert")
        try {
            StepVerifier.create(orderService.createOrder("customer-zero-order"))
                .expectErrorMatches {
                    it.message == "Expected to insert one order, but inserted 0"
                }
                .verify()
            assertNoOrderWrites()

            val responseBody = exchange("""{"customerId":"customer-zero-order-http"}""")
                .expectStatus()
                .is5xxServerError
                .expectBody(String::class.java)
                .returnResult()
                .responseBody
            assertSafeErrorBody(responseBody)
            assertNoOrderWrites()
        } finally {
            removeSuppressInsertTriggers()
        }
    }

    @Test
    fun `zero-row outbox insert returns an error and rolls back the order`() {
        suppressInsert("outbox_events", "suppress_outbox_insert")

        val responseBody = exchange("""{"customerId":"customer-zero-outbox"}""")
            .expectStatus()
            .is5xxServerError
            .expectBody(String::class.java)
            .returnResult()
            .responseBody

        assertSafeErrorBody(responseBody)
        assertNoOrderWrites()
    }

    @Test
    fun `OpenAPI and Swagger UI describe the reactive HTTP contract`() {
        val docs = webTestClient.get()
            .uri("/v3/api-docs")
            .exchange()
            .expectStatus()
            .isOk
            .expectBody(String::class.java)
            .returnResult()
            .responseBody!!
        val schema = objectMapper.readTree(docs)

        assertTrue(schema.get("openapi").asText().startsWith("3."))
        assertFalse(schema.at("/paths/~1orders/post").isMissingNode)
        assertFalse(schema.at("/components/schemas/CreateOrderRequest").isMissingNode)
        assertTrue(schema.at("/paths/~1orders/post/responses").has("201"))
        assertTrue(schema.at("/paths/~1orders/post/responses").has("400"))
        assertTrue(
            schema.at("/paths/~1orders/post/responses/201/content/*~1*/schema/\$ref")
                .asText().endsWith("/CreateOrderResponse"),
        )
        assertFalse(schema.at("/paths/~1orders/post/responses/400").toString().contains("CreateOrderResponse"))

        webTestClient.get()
            .uri("/swagger-ui.html")
            .exchange()
            .expectStatus()
            .value { assertTrue(it in 200..399) }
    }

    @Test
    fun `actuator health endpoint remains available`() {
        webTestClient.get()
            .uri("/actuator/health")
            .exchange()
            .expectStatus()
            .isOk
            .expectBody()
            .jsonPath("$.status")
            .isEqualTo("UP")
    }

    private fun exchange(request: String) =
        webTestClient.post()
            .uri("/orders")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(request)
            .exchange()

    private fun createOrder(request: String): UUID {
        val customerId = objectMapper.readTree(request).get("customerId").asText()
        val response = exchange(request)
            .expectStatus()
            .isCreated
            .expectBody(CreateOrderResponse::class.java)
            .returnResult()
            .responseBody!!
        assertEquals(
            customerId,
            jdbcTemplate.queryForObject("SELECT customer_id FROM orders WHERE id = ?", String::class.java, response.orderId),
        )
        return response.orderId
    }

    private fun assertNoOrderWrites() {
        assertEquals(0, jdbcTemplate.queryForObject("SELECT COUNT(*) FROM orders", Int::class.java))
        assertEquals(0, jdbcTemplate.queryForObject("SELECT COUNT(*) FROM outbox_events", Int::class.java))
    }

    private fun assertSafeErrorBody(body: String?) {
        val response = body.orEmpty()
        listOf(
            "stacktrace",
            "exception",
            "INSERT INTO",
            "outbox_events",
            "forced outbox insert failure",
            "jdbc:",
            "r2dbc:",
            "postgresql://",
            "spring.datasource",
            "username",
            "password",
        ).forEach { sensitiveText ->
            assertFalse(response.contains(sensitiveText, ignoreCase = true), "error response leaked '$sensitiveText'")
        }
    }

    private fun removeFailureTrigger() {
        try {
            jdbcTemplate.execute("DROP TRIGGER IF EXISTS fail_outbox_insert ON outbox_events")
            removeSuppressInsertTriggers()
        } finally {
            jdbcTemplate.execute("DROP FUNCTION IF EXISTS fail_outbox_insert()")
        }
    }

    private fun removeSuppressInsertTriggers() {
        jdbcTemplate.execute("DROP TRIGGER IF EXISTS suppress_order_insert ON orders")
        jdbcTemplate.execute("DROP TRIGGER IF EXISTS suppress_outbox_insert ON outbox_events")
        jdbcTemplate.execute("DROP FUNCTION IF EXISTS suppress_insert()")
    }

    private fun anyEvent(): ing.recruitment.outbox.publisher.OutboxPendingEvent =
        org.mockito.ArgumentMatchers.any(ing.recruitment.outbox.publisher.OutboxPendingEvent::class.java)
            ?: ing.recruitment.outbox.publisher.OutboxPendingEvent(
                UUID.randomUUID(),
                "matcher",
                UUID.randomUUID(),
                com.fasterxml.jackson.databind.node.JsonNodeFactory.instance.objectNode(),
                java.time.Instant.EPOCH,
            )

    private fun suppressInsert(table: String, trigger: String) {
        require(table in setOf("orders", "outbox_events"))
        require(trigger in setOf("suppress_order_insert", "suppress_outbox_insert"))
        jdbcTemplate.execute(
            """
            CREATE OR REPLACE FUNCTION suppress_insert() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                RETURN NULL;
            END;
            $$
            """.trimIndent(),
        )
        jdbcTemplate.execute(
            "CREATE TRIGGER $trigger BEFORE INSERT ON $table " +
                "FOR EACH ROW EXECUTE FUNCTION suppress_insert()",
        )
    }
}
