package ing.recruitment.orders.application

import ing.recruitment.outbox.persistence.OutboxEventEntity
import ing.recruitment.outbox.persistence.OutboxEventRepository
import ing.recruitment.orders.persistence.OrderEntity
import ing.recruitment.orders.persistence.OrderRepository
import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.mockito.ArgumentCaptor
import org.mockito.Mockito.doThrow
import org.mockito.Mockito.mock
import org.mockito.Mockito.verify

class OrderServiceTest {
    private val orderRepository = mock(OrderRepository::class.java)
    private val outboxEventRepository = mock(OutboxEventRepository::class.java)
    private val orderService = OrderService(orderRepository, outboxEventRepository, ObjectMapper())

    @Test
    fun `saves order and matching pending OrderCreated event`() {
        val customerId = "customer-123"
        val orderId = orderService.createOrder(customerId)
        val orderCaptor = ArgumentCaptor.forClass(OrderEntity::class.java)
        val eventCaptor = ArgumentCaptor.forClass(OutboxEventEntity::class.java)

        verify(orderRepository).save(orderCaptor.capture())
        verify(outboxEventRepository).save(eventCaptor.capture())

        val order = orderCaptor.value
        val event = eventCaptor.value
        assertEquals(orderId, order.id)
        assertEquals(customerId, order.customerId)
        assertEquals(order.id, event.aggregateId)
        assertEquals("OrderCreated", event.eventType)
        assertEquals(orderId.toString(), event.payload.get("orderId").asText())
        assertEquals(customerId, event.payload.get("customerId").asText())
        assertEquals(order.createdAt, event.createdAt)
        assertNull(event.processedAt)
    }

    @Test
    fun `propagates failure when saving outbox event`() {
        val failure = IllegalStateException("outbox unavailable")
        doThrow(failure).`when`(outboxEventRepository).save(org.mockito.ArgumentMatchers.any())

        assertSame(failure, assertThrows(IllegalStateException::class.java) {
            orderService.createOrder("customer-123")
        })
        verify(orderRepository).save(org.mockito.ArgumentMatchers.any())
    }
}
