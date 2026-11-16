package com.example.outbox.outbox.publisher

import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.mockito.Mockito.inOrder
import org.mockito.Mockito.mock
import org.mockito.Mockito
import org.mockito.Mockito.verify
import java.time.Instant
import java.util.UUID

class OutboxProcessingServiceTest {
    private val repository = mock(OutboxEventStore::class.java)
    private val eventLogger = mock(OutboxEventLogger::class.java)
    private val service = OutboxProcessingService(repository, eventLogger, 10)

    @Test
    fun `empty batch does not publish`() {
        org.mockito.Mockito.`when`(repository.findAndLockUnprocessed(10)).thenReturn(emptyList())

        assertEquals(0, service.processBatch())

        Mockito.verifyNoInteractions(eventLogger)
    }

    @Test
    fun `publishes event details before marking processed`() {
        val event = pendingEvent()
        org.mockito.Mockito.`when`(repository.findAndLockUnprocessed(10)).thenReturn(listOf(event))

        assertEquals(1, service.processBatch())

        val order = inOrder(eventLogger, repository)
        order.verify(eventLogger).publish(event)
        order.verify(repository).markProcessed(event.id)
    }

    @Test
    fun `publication failure does not mark event processed`() {
        val event = pendingEvent()
        org.mockito.Mockito.`when`(repository.findAndLockUnprocessed(10)).thenReturn(listOf(event))
        org.mockito.Mockito.doThrow(IllegalStateException("logging failed"))
            .`when`(eventLogger).publish(event)

        org.junit.jupiter.api.Assertions.assertThrows(IllegalStateException::class.java) {
            service.processBatch()
        }

        Mockito.verify(repository, Mockito.never()).markProcessed(event.id)
    }

    private fun pendingEvent() = OutboxPendingEvent(
        id = UUID.randomUUID(),
        eventType = "OrderCreated",
        aggregateId = UUID.randomUUID(),
        payload = ObjectMapper().readTree("""{"orderId":"test","customerId":"123"}"""),
        createdAt = Instant.parse("2026-01-01T00:00:00Z"),
    )
}
