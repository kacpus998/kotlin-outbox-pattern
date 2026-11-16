package ing.recruitment.orders.application

import ing.recruitment.testsupport.PostgresIntegrationTest
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.dao.DataAccessException
import org.springframework.jdbc.core.JdbcTemplate
import java.util.UUID

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class OrderServiceIntegrationTest : PostgresIntegrationTest() {
    @Autowired
    private lateinit var orderService: OrderService

    @Autowired
    private lateinit var jdbcTemplate: JdbcTemplate

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
    fun `service persists order and its outbox event`() {
        val orderId = orderService.createOrder("customer-service-test")

        assertEquals(1, jdbcTemplate.queryForObject("SELECT COUNT(*) FROM orders", Int::class.java))
        assertEquals(1, jdbcTemplate.queryForObject("SELECT COUNT(*) FROM outbox_events", Int::class.java))
        assertEquals(
            orderId,
            jdbcTemplate.queryForObject("SELECT aggregate_id FROM outbox_events", UUID::class.java),
        )
    }

    @Test
    fun `outbox insert failure rolls back the order`() {
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

        assertThrows(DataAccessException::class.java) {
            orderService.createOrder("customer-rollback")
        }

        assertEquals(0, jdbcTemplate.queryForObject("SELECT COUNT(*) FROM orders", Int::class.java))
        assertEquals(0, jdbcTemplate.queryForObject("SELECT COUNT(*) FROM outbox_events", Int::class.java))
    }

    private fun removeFailureTrigger() {
        try {
            jdbcTemplate.execute("DROP TRIGGER IF EXISTS fail_outbox_insert ON outbox_events")
        } finally {
            jdbcTemplate.execute("DROP FUNCTION IF EXISTS fail_outbox_insert()")
        }
    }
}
