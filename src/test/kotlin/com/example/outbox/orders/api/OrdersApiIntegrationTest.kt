package com.example.outbox.orders.api

import com.example.outbox.testsupport.PostgresIntegrationTest
import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
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
