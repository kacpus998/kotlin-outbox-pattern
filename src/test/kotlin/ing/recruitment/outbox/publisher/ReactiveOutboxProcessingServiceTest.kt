package ing.recruitment.outbox.publisher

import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Test
import org.mockito.Mockito.doAnswer
import org.mockito.Mockito.mock
import org.mockito.Mockito.verify
import org.mockito.Mockito.verifyNoInteractions
import org.mockito.Mockito.`when`
import org.springframework.transaction.ReactiveTransaction
import org.springframework.transaction.ReactiveTransactionManager
import org.springframework.transaction.TransactionDefinition
import org.springframework.transaction.reactive.TransactionalOperator
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import java.time.Instant
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicBoolean

class ReactiveOutboxProcessingServiceTest {
    private val repository = mock(OutboxEventStore::class.java)
    private val eventLogger = mock(OutboxEventLogger::class.java)
    private val transactionManager = mock(ReactiveTransactionManager::class.java)
    private val transaction = mock(ReactiveTransaction::class.java)
    private val transactionalOperator = TransactionalOperator.create(transactionManager)
    private val service = OutboxProcessingService(repository, eventLogger, 10, transactionalOperator)

    init {
        `when`(transactionManager.getReactiveTransaction(org.mockito.ArgumentMatchers.any(TransactionDefinition::class.java)))
            .thenReturn(Mono.just(transaction))
        `when`(transactionManager.commit(transaction)).thenReturn(Mono.empty())
        `when`(transactionManager.rollback(transaction)).thenReturn(Mono.empty())
    }

    @Test
    fun `batch publisher is cold until subscription`() {
        `when`(repository.findAndLockUnprocessedReactive(10)).thenReturn(Flux.empty())

        val batch = service.processBatch()

        verifyNoInteractions(repository, eventLogger)
        StepVerifier.create(batch)
            .expectNext(0)
            .verifyComplete()
        verify(repository).findAndLockUnprocessedReactive(10)
    }

    @Test
    fun `materialized batch publishes and marks each event sequentially`() {
        val first = pendingEvent()
        val second = pendingEvent()
        val executed = CopyOnWriteArrayList<String>()
        `when`(repository.findAndLockUnprocessedReactive(10)).thenReturn(Flux.just(first, second))
        `when`(repository.markProcessedReactive(first.id)).thenReturn(
            Mono.fromRunnable<Void> { executed.add("mark-${first.id}") },
        )
        `when`(repository.markProcessedReactive(second.id)).thenReturn(
            Mono.fromRunnable<Void> { executed.add("mark-${second.id}") },
        )
        doAnswer { executed.add("publish-${first.id}"); null }.`when`(eventLogger).publish(first)
        doAnswer { executed.add("publish-${second.id}"); null }.`when`(eventLogger).publish(second)

        StepVerifier.create(service.processBatch())
            .expectNext(2)
            .verifyComplete()

        assertEquals(
            listOf("publish-${first.id}", "mark-${first.id}", "publish-${second.id}", "mark-${second.id}"),
            executed,
        )
        verify(repository).findAndLockUnprocessedReactive(10)
    }

    @Test
    fun `publication failure is propagated and prevents update`() {
        val event = pendingEvent()
        val failure = IllegalStateException("publication failed")
        val updateSubscribed = AtomicBoolean(false)
        `when`(repository.findAndLockUnprocessedReactive(10)).thenReturn(Flux.just(event))
        `when`(repository.markProcessedReactive(event.id)).thenReturn(
            Mono.fromRunnable<Void> { updateSubscribed.set(true) },
        )
        org.mockito.Mockito.doThrow(failure).`when`(eventLogger).publish(event)

        StepVerifier.create(service.processBatch())
            .expectErrorMatches { it.cause === failure }
            .verify()

        assertFalse(updateSubscribed.get(), "update publisher must not be subscribed after publication fails")
    }

    @Test
    fun `update failure is propagated for transaction rollback`() {
        val event = pendingEvent()
        `when`(repository.findAndLockUnprocessedReactive(10)).thenReturn(Flux.just(event))
        `when`(repository.markProcessedReactive(event.id)).thenReturn(Mono.error(IllegalStateException("update failed")))

        StepVerifier.create(service.processBatch())
            .expectErrorMatches { it.message.orEmpty().contains(event.id.toString()) }
            .verify()

        verify(transactionManager).rollback(transaction)
    }

    private fun pendingEvent() = OutboxPendingEvent(
        id = UUID.randomUUID(),
        eventType = "OrderCreated",
        aggregateId = UUID.randomUUID(),
        payload = ObjectMapper().readTree("""{"orderId":"order","customerId":"123"}"""),
        createdAt = Instant.parse("2026-01-01T00:00:00Z"),
    )
}
