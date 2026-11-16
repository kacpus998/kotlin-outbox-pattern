package com.example.outbox.orders.application

import com.example.outbox.orders.domain.OrderCreated
import com.example.outbox.orders.persistence.OrderEntity
import com.example.outbox.orders.persistence.OrderRepository
import com.example.outbox.outbox.persistence.OutboxEventEntity
import com.example.outbox.outbox.persistence.OutboxEventRepository
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import java.time.Instant
import java.util.UUID

@Service
class OrderService(
    private val orderRepository: OrderRepository,
    private val outboxEventRepository: OutboxEventRepository,
    private val objectMapper: ObjectMapper,
) {
    @Transactional
    fun createOrder(customerId: String): UUID {
        val orderId = UUID.randomUUID()
        val createdAt = Instant.now()

        orderRepository.save(
            OrderEntity(
                id = orderId,
                customerId = customerId,
                createdAt = createdAt,
            ),
        )

        val payload = objectMapper.valueToTree<JsonNode>(
            OrderCreated(orderId = orderId, customerId = customerId),
        )
        outboxEventRepository.save(
            OutboxEventEntity(
                id = UUID.randomUUID(),
                aggregateId = orderId,
                eventType = "OrderCreated",
                payload = payload,
                createdAt = createdAt,
            ),
        )

        return orderId
    }
}
