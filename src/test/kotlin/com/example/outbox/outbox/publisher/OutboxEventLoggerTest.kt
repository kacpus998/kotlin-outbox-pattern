package com.example.outbox.outbox.publisher

import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.junit.jupiter.api.assertThrows
import org.springframework.boot.context.properties.bind.BindException
import org.springframework.boot.context.properties.bind.Binder
import org.springframework.boot.context.properties.bind.Bindable
import org.springframework.boot.context.properties.source.ConfigurationPropertySources
import org.springframework.core.env.MapPropertySource
import java.time.Instant
import java.util.UUID

class OutboxEventLoggerTest {
    @Test
    fun `metadata only logging does not expose customer data`() {
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
            Slf4jOutboxEventLogger(OutboxLoggingProperties()).publish(event)

            val messages = appender.list.map { it.formattedMessage }
            assertEquals(1, messages.size)
            assertTrue(messages.single().contains(event.id.toString()))
            assertTrue(messages.single().contains(event.eventType))
            assertTrue(messages.single().contains(event.aggregateId.toString()))
            assertTrue(!messages.single().contains("customerId"))
            assertTrue(!messages.single().contains("123"))
            assertTrue(messages.none { it.contains("Processed outbox event") })
        } finally {
            logger.detachAppender(appender)
            appender.stop()
        }
    }

    @Test
    fun `redacted logging masks customer and excludes unknown fields`() {
        val appender = captureLogs()
        val payload = ObjectMapper().readTree("""{"orderId":"order-1","customerId":"secret","token":"private"}""")
        val originalPayload = payload.toString()
        val event = event(payload)
        try {
            Slf4jOutboxEventLogger(OutboxLoggingProperties().apply { payloadMode = PayloadMode.REDACTED }).publish(event)
            val message = appender.list.single().formattedMessage
            assertTrue(message.contains("[REDACTED]"))
            assertTrue(!message.contains("secret"))
            assertTrue(!message.contains("private"))
            assertTrue(message.contains("order-1"))
            assertEquals(originalPayload, event.payload.toString())
        } finally { closeLogs(appender) }
    }

    @Test
    fun `invalid payload mode is rejected by configuration binding`() {
        val source = MapPropertySource("test", mapOf("outbox.publisher.logging.payload-mode" to "RAW"))
        val binder = Binder(ConfigurationPropertySources.from(source))
        assertThrows<BindException> {
            binder.bind("outbox.publisher.logging", Bindable.of(OutboxLoggingProperties::class.java)).get()
        }
    }

    @Test
    fun `metadata fields sanitize line breaks`() {
        val appender = captureLogs()
        try {
            Slf4jOutboxEventLogger(OutboxLoggingProperties()).publish(event(eventType = "OrderCreated\r\nFORGED=true"))
            assertEquals(1, appender.list.size)
            assertTrue(!appender.list.single().formattedMessage.contains("\r"))
            assertTrue(!appender.list.single().formattedMessage.contains("\n"))
        } finally { closeLogs(appender) }
    }

    private fun captureLogs() = ListAppender<ILoggingEvent>().apply {
        start()
        (LoggerFactory.getLogger(OutboxEventLogger::class.java) as Logger).addAppender(this)
    }

    private fun closeLogs(appender: ListAppender<ILoggingEvent>) {
        (LoggerFactory.getLogger(OutboxEventLogger::class.java) as Logger).detachAppender(appender)
        appender.stop()
    }

    private fun event(payload: com.fasterxml.jackson.databind.JsonNode = ObjectMapper().readTree("""{"customerId":"123"}"""), eventType: String = "OrderCreated") =
        OutboxPendingEvent(UUID.randomUUID(), eventType, UUID.randomUUID(), payload, Instant.now())
}
