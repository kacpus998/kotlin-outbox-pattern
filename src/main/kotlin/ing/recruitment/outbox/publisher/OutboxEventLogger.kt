package ing.recruitment.outbox.publisher

import com.fasterxml.jackson.databind.node.JsonNodeFactory
import org.slf4j.LoggerFactory
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.stereotype.Component

enum class PayloadMode { METADATA_ONLY, REDACTED }

@Component
@ConfigurationProperties(prefix = "outbox.publisher.logging")
class OutboxLoggingProperties {
    var payloadMode: PayloadMode = PayloadMode.METADATA_ONLY
}

interface OutboxEventLogger {
    fun publish(event: OutboxPendingEvent)
}

@Component
class Slf4jOutboxEventLogger(private val properties: OutboxLoggingProperties) : OutboxEventLogger {
    private val logger = LoggerFactory.getLogger(OutboxEventLogger::class.java)
    private val lineBreakPattern = Regex("[\\r\\n\\u0085\\u2028\\u2029]")

    override fun publish(event: OutboxPendingEvent) {
        logger.info(
            "Publishing outbox event id={} type={} aggregateId={}{}",
            safe(event.id.toString()),
            safe(event.eventType),
            safe(event.aggregateId.toString()),
            loggedPayload(event),
        )
    }

    private fun loggedPayload(event: OutboxPendingEvent): String = when (properties.payloadMode) {
        PayloadMode.METADATA_ONLY -> ""
        PayloadMode.REDACTED -> {
            val safePayload = JsonNodeFactory.instance.objectNode()
            event.payload.get("orderId")?.takeIf { it.isTextual }?.let { safePayload.put("orderId", safe(it.asText())) }
            if (event.payload.has("customerId")) safePayload.put("customerId", "[REDACTED]")
            " payload=$safePayload"
        }
    }

    private fun safe(value: String): String = value.replace(lineBreakPattern, " ")
}
