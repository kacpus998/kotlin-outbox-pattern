package com.example.outbox.outbox.publisher

import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component

interface OutboxEventLogger {
    fun publish(event: OutboxPendingEvent)
}

@Component
class Slf4jOutboxEventLogger : OutboxEventLogger {
    private val logger = LoggerFactory.getLogger(OutboxEventLogger::class.java)

    override fun publish(event: OutboxPendingEvent) {
        logger.info(
            "Publishing outbox event id={} type={} aggregateId={} payload={}",
            event.id,
            event.eventType,
            event.aggregateId,
            event.payload,
        )
    }
}
