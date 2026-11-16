package ing.recruitment.outbox.publisher

import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono

@Component
@ConditionalOnProperty(prefix = "outbox.publisher", name = ["enabled"], havingValue = "true", matchIfMissing = true)
class OutboxPublisher(private val processingService: OutboxProcessingService) {
    private val logger = LoggerFactory.getLogger(javaClass)

    @Scheduled(fixedDelayString = "\${outbox.publisher.fixed-delay-ms:5000}")
    fun publishPendingEvents(): Mono<Void> = processingService.processBatch()
        .doOnNext { processed ->
            if (processed > 0) logger.debug("Outbox publisher processed {} event(s)", processed)
        }
        .then()
}
