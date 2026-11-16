package ing.recruitment.orders.application

import ing.recruitment.orders.domain.OrderCreated
import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.stereotype.Service
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.transaction.reactive.TransactionalOperator
import reactor.core.publisher.Mono
import java.time.Instant
import java.util.UUID

@Service
class OrderService(
    private val databaseClient: DatabaseClient,
    private val objectMapper: ObjectMapper,
    private val transactionalOperator: TransactionalOperator,
) {
    fun createOrder(customerId: String): Mono<UUID> = Mono.defer {
        val orderId = UUID.randomUUID()
        val eventId = UUID.randomUUID()
        val createdAt = Instant.now()
        val payload = objectMapper.writeValueAsString(
            OrderCreated(orderId = orderId, customerId = customerId),
        )

        val writes = insertOrder(orderId, customerId, createdAt)
            .then(insertOutboxEvent(eventId, orderId, payload, createdAt))
            .thenReturn(orderId)

        transactionalOperator.transactional(writes)
    }

    private fun insertOrder(orderId: UUID, customerId: String, createdAt: Instant): Mono<Long> =
        databaseClient.sql(
            "INSERT INTO orders (id, customer_id, created_at) VALUES (:id, :customerId, :createdAt)",
        )
            .bind("id", orderId)
            .bind("customerId", customerId)
            .bind("createdAt", createdAt)
            .fetch()
            .rowsUpdated()

    private fun insertOutboxEvent(eventId: UUID, orderId: UUID, payload: String, createdAt: Instant): Mono<Long> =
        databaseClient.sql(
            "INSERT INTO outbox_events (id, aggregate_id, event_type, payload, created_at) " +
                "VALUES (:id, :aggregateId, :eventType, CAST(:payload AS jsonb), :createdAt)",
        )
            .bind("id", eventId)
            .bind("aggregateId", orderId)
            .bind("eventType", "OrderCreated")
            .bind("payload", payload)
            .bind("createdAt", createdAt)
            .fetch()
            .rowsUpdated()
}
