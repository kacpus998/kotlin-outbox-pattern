package ing.recruitment.reactive

import ing.recruitment.testsupport.PostgresIntegrationTest
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.transaction.reactive.TransactionalOperator
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import java.util.UUID

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("local")
class ReactiveDatabaseFoundationIntegrationTest : PostgresIntegrationTest() {
    @Autowired
    private lateinit var databaseClient: DatabaseClient

    @Autowired
    private lateinit var transactionalOperator: TransactionalOperator

    @Test
    fun `R2DBC connects to PostgreSQL and executes a query`() {
        StepVerifier.create(
            databaseClient.sql("SELECT 1 AS result")
                .map { row, _ -> row.get("result", Int::class.java)!! }
                .one(),
        )
            .expectNext(1)
            .verifyComplete()
    }

    @Test
    fun `Flyway tables are visible through R2DBC`() {
        StepVerifier.create(
            databaseClient.sql(
                "SELECT COUNT(*) FROM information_schema.tables " +
                    "WHERE table_schema = current_schema() AND table_name IN ('orders', 'outbox_events')",
            )
                .map { row, _ -> row.get(0, Long::class.java)!! }
                .one(),
        )
            .expectNext(2L)
            .verifyComplete()
    }

    @Test
    fun `reactive transaction commits an order row`() {
        val orderId = UUID.randomUUID()

        try {
            StepVerifier.create(
                transactionalOperator.transactional(insertOrder(orderId))
                    .then(countOrder(orderId)),
            )
                .expectNext(1L)
                .verifyComplete()
        } finally {
            deleteOrder(orderId)
        }
    }

    @Test
    fun `reactive transaction rolls back an order row on failure`() {
        val orderId = UUID.randomUUID()
        val failedTransaction = transactionalOperator.transactional(
            insertOrder(orderId)
                .then(Mono.error<Long>(IllegalStateException("force rollback"))),
        )

        try {
            StepVerifier.create(failedTransaction)
                .expectErrorMatches { it is IllegalStateException && it.message == "force rollback" }
                .verify()

            StepVerifier.create(countOrder(orderId))
                .expectNext(0L)
                .verifyComplete()
        } finally {
            deleteOrder(orderId)
        }
    }

    private fun insertOrder(orderId: UUID): Mono<Long> =
        databaseClient.sql("INSERT INTO orders (id, customer_id, created_at) VALUES (:id, :customerId, now())")
            .bind("id", orderId)
            .bind("customerId", "reactive-foundation-test")
            .fetch()
            .rowsUpdated()

    private fun countOrder(orderId: UUID): Mono<Long> =
        databaseClient.sql("SELECT COUNT(*) FROM orders WHERE id = :id")
            .bind("id", orderId)
            .map { row, _ -> row.get(0, Long::class.java)!! }
            .one()

    private fun deleteOrder(orderId: UUID) {
        StepVerifier.create(
            databaseClient.sql("DELETE FROM orders WHERE id = :id")
                .bind("id", orderId)
                .fetch()
                .rowsUpdated()
                .then(),
        )
            .verifyComplete()
    }
}
