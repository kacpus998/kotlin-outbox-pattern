package com.example.outbox.outbox.publisher

import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

@Component
@ConditionalOnProperty(prefix = "outbox.publisher", name = ["enabled"], havingValue = "true", matchIfMissing = true)
class OutboxPublisher(private val processingService: OutboxProcessingService) {
    private val logger = LoggerFactory.getLogger(javaClass)

    @Scheduled(fixedDelayString = "\${outbox.publisher.fixed-delay-ms:5000}")
    fun publishPendingEvents() {
        try {
            val processed = processingService.processBatch()
            if (processed > 0) logger.debug("Outbox publisher processed {} event(s)", processed)
        } catch (failure: Exception) {
            logger.error("Outbox publisher cycle failed", failure)
        }
    }
}
