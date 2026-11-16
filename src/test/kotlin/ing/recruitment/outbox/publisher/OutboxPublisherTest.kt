package ing.recruitment.outbox.publisher

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.mockito.Mockito.times
import org.mockito.Mockito.verify
import org.mockito.Mockito.`when`
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import java.util.concurrent.atomic.AtomicInteger

class OutboxPublisherTest {
    private val processingService = mock(OutboxProcessingService::class.java)
    private val publisher = OutboxPublisher(processingService)

    @Test
    fun `each subscription to a scheduled cycle triggers fresh processing`() {
        val executions = AtomicInteger()
        `when`(processingService.processBatch()).thenReturn(
            Mono.defer { Mono.just(executions.incrementAndGet()) },
        )

        val cycle = publisher.publishPendingEvents()
        assertEquals(0, executions.get())

        StepVerifier.create(cycle).verifyComplete()
        StepVerifier.create(cycle).verifyComplete()

        assertEquals(2, executions.get())
        verify(processingService, times(1)).processBatch()
    }

    @Test
    fun `cycle failure propagates so Spring can log and recover for next schedule`() {
        val failure = IllegalStateException("cycle failed")
        `when`(processingService.processBatch()).thenReturn(Mono.error(failure))

        StepVerifier.create(publisher.publishPendingEvents())
            .expectErrorSatisfies { assertEquals(failure, it) }
            .verify()
    }
}
