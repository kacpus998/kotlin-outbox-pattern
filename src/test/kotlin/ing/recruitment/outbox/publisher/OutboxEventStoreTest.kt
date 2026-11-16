package ing.recruitment.outbox.publisher

import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import org.springframework.r2dbc.core.DatabaseClient

class OutboxEventStoreTest {
    private val databaseClient = mock(DatabaseClient::class.java)
    private val store = OutboxEventStore(databaseClient, ObjectMapper())

    @Test
    fun `rejects non-positive batch size`() {
        assertThrows(IllegalArgumentException::class.java) {
            store.findAndLockUnprocessedReactive(0)
        }
    }
}
