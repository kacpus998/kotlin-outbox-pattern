package ing.recruitment.orders.api

import ing.recruitment.testsupport.PostgresIntegrationTest
import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.web.client.TestRestTemplate
import org.springframework.http.HttpEntity
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.http.ResponseEntity
import org.springframework.jdbc.core.JdbcTemplate
import java.util.UUID

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class OrdersApiIntegrationTest : PostgresIntegrationTest() {
    @Autowired
    private lateinit var http: TestRestTemplate

    @Autowired
    private lateinit var jdbcTemplate: JdbcTemplate

    @Autowired
    private lateinit var objectMapper: ObjectMapper

    @BeforeEach
    fun cleanDatabase() {
        jdbcTemplate.update("DELETE FROM outbox_events")
        jdbcTemplate.update("DELETE FROM orders")
    }

    @Test
    fun `POST orders creates an order and one pending OrderCreated event`() {
        val response = postJson("""{"customerId":"customer-123"}""")

        assertEquals(HttpStatus.CREATED, response.statusCode)
        val orderId = UUID.fromString(objectMapper.readTree(response.body).get("orderId").asText())
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
    fun `repeated identical requests create distinct orders and events`() {
        val first = postJson("""{"customerId":"123"}""")
        val second = postJson("""{"customerId":"123"}""")
        assertEquals(HttpStatus.CREATED, first.statusCode)
        assertEquals(HttpStatus.CREATED, second.statusCode)
        val firstId = UUID.fromString(objectMapper.readTree(first.body).get("orderId").asText())
        val secondId = UUID.fromString(objectMapper.readTree(second.body).get("orderId").asText())
        org.junit.jupiter.api.Assertions.assertNotEquals(firstId, secondId)
        assertEquals(2, jdbcTemplate.queryForObject("SELECT COUNT(*) FROM orders", Int::class.java))
        assertEquals(2, jdbcTemplate.queryForObject("SELECT COUNT(*) FROM outbox_events", Int::class.java))
        assertEquals(2, jdbcTemplate.queryForObject("SELECT COUNT(DISTINCT id) FROM outbox_events", Int::class.java))
    }

    @Test
    fun `customerId longer than database column limit returns bad request`() {
        val response = postJson("""{"customerId":"${"x".repeat(256)}"}""")
        assertEquals(HttpStatus.BAD_REQUEST, response.statusCode)
        assertEquals(0, jdbcTemplate.queryForObject("SELECT COUNT(*) FROM orders", Int::class.java))
    }

    @Test
    fun `openapi and swagger ui are available`() {
        val docs = http.getForEntity("/v3/api-docs", String::class.java)
        assertEquals(HttpStatus.OK, docs.statusCode)
        val schema = objectMapper.readTree(docs.body)
        assertTrue(schema.get("openapi").asText().startsWith("3."))
        assertTrue(!schema.at("/paths/~1orders/post").isMissingNode)
        assertTrue(!schema.at("/components/schemas/CreateOrderRequest").isMissingNode)
        assertTrue(schema.at("/paths/~1orders/post/responses").has("201"))
        assertTrue(schema.at("/paths/~1orders/post/responses").has("400"))
        assertTrue(
            schema.at("/paths/~1orders/post/responses/201/content/*~1*/schema/\$ref")
                .asText().endsWith("/CreateOrderResponse"),
        )
        val badRequestResponse = schema.at("/paths/~1orders/post/responses/400")
        assertFalse(badRequestResponse.toString().contains("CreateOrderResponse"))

        val invalidRequest = postJson("""{"customerId":" "}""")
        assertEquals(HttpStatus.BAD_REQUEST, invalidRequest.statusCode)
        assertEquals(MediaType.APPLICATION_JSON, invalidRequest.headers.contentType)
        assertEquals(400, objectMapper.readTree(invalidRequest.body).get("status").asInt())

        val swagger = http.getForEntity("/swagger-ui.html", String::class.java)
        assertTrue(swagger.statusCode.is2xxSuccessful || swagger.statusCode.is3xxRedirection)
    }

    @ParameterizedTest
    @ValueSource(strings = ["{}", """{"customerId":null}""", """{"customerId":""}""", """{"customerId":"   "}"""])
    fun `invalid customerId returns bad request without writes`(request: String) {
        val response = postJson(request)

        assertEquals(HttpStatus.BAD_REQUEST, response.statusCode)
        assertEquals(0, jdbcTemplate.queryForObject("SELECT COUNT(*) FROM orders", Int::class.java))
        assertEquals(0, jdbcTemplate.queryForObject("SELECT COUNT(*) FROM outbox_events", Int::class.java))
    }

    private fun postJson(request: String): ResponseEntity<String> {
        val headers = HttpHeaders().apply { contentType = MediaType.APPLICATION_JSON }
        return http.postForEntity("/orders", HttpEntity(request, headers), String::class.java)
    }
}
