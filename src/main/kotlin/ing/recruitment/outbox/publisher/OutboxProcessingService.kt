package ing.recruitment.outbox.publisher

import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import org.springframework.transaction.reactive.TransactionalOperator
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers

@Service
class OutboxProcessingService(
    private val repository: OutboxEventStore,
    private val eventLogger: OutboxEventLogger,
    @param:Value("\${outbox.publisher.batch-size:10}") private val batchSize: Int,
    private val transactionalOperator: TransactionalOperator,
) {
    init {
        require(batchSize > 0) { "outbox.publisher.batch-size must be positive" }
    }

    fun processBatch(): Mono<Int> = Mono.defer {
        val work: Mono<Int> = repository.findAndLockUnprocessedReactive(batchSize)
            .collectList()
            .flatMap { events ->
                val updates: Flux<Void> = Flux.fromIterable(events)
                    .concatMap<Void> { event ->
                        Mono.fromRunnable<Void> { eventLogger.publish(event) }
                            .subscribeOn(Schedulers.boundedElastic())
                            .then(repository.markProcessedReactive(event.id))
                            .onErrorMap { failure ->
                                IllegalStateException("Failed to process outbox event ${event.id}", failure)
                            }
                    }
                updates.then(Mono.just(events.size))
            }

        transactionalOperator.transactional(work)
    }
}
