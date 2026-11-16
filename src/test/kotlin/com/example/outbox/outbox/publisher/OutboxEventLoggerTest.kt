package com.example.outbox.outbox.publisher

import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import java.time.Instant
import java.util.UUID

class OutboxEventLoggerTest {
    @Test
    fun `logs event id type aggregate id and payload without claiming transaction committed`() {
        val logger = LoggerFactory.getLogger(OutboxEventLogger::class.java) as Logger
        val appender = ListAppender<ILoggingEvent>().apply { start() }
        logger.addAppender(appender)
        val event = OutboxPendingEvent(
            UUID.randomUUID(),
            "OrderCreated",
            UUID.randomUUID(),
            ObjectMapper().readTree("""{"customerId":"123"}"""),
            Instant.now(),
        )

        try {
            Slf4jOutboxEventLogger().publish(event)

            val messages = appender.list.map { it.formattedMessage }
            assertEquals(1, messages.size)
            assertTrue(messages.single().contains(event.id.toString()))
            assertTrue(messages.single().contains(event.eventType))
            assertTrue(messages.single().contains(event.aggregateId.toString()))
            assertTrue(messages.single().contains("customerId"))
            assertTrue(messages.none { it.contains("Processed outbox event") })
        } finally {
            logger.detachAppender(appender)
            appender.stop()
        }
    }
}
