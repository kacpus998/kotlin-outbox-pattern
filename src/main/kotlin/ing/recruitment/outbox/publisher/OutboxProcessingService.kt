package ing.recruitment.outbox.publisher

import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional

@Service
class OutboxProcessingService(
    private val repository: OutboxEventStore,
    private val eventLogger: OutboxEventLogger,
    @param:Value("\${outbox.publisher.batch-size:10}") private val batchSize: Int,
) {
    init {
        require(batchSize > 0) { "outbox.publisher.batch-size must be positive" }
    }

    @Transactional
    fun processBatch(): Int {
        val events = repository.findAndLockUnprocessed(batchSize)
        events.forEach { event ->
            try {
                eventLogger.publish(event)
                repository.markProcessed(event.id)
            } catch (failure: Exception) {
                throw IllegalStateException("Failed to publish outbox event ${event.id}", failure)
            }
        }
        return events.size
    }
}
