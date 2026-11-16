package com.example.outbox.outbox.publisher

import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.mockito.ArgumentMatchers.any
import org.mockito.ArgumentMatchers.eq
import org.mockito.Mockito.mock
import org.mockito.Mockito.`when`
import org.springframework.jdbc.core.JdbcTemplate
import java.sql.Timestamp
import java.util.UUID

class OutboxEventStoreTest {
    private val jdbcTemplate = mock(JdbcTemplate::class.java)
    private val store = OutboxEventStore(jdbcTemplate, ObjectMapper())

    @Test
    fun `throws when marking processed updates no rows`() {
        val id = UUID.randomUUID()
        `when`(
            jdbcTemplate.update(
                eq("""
                    UPDATE outbox_events
                    SET processed_at = ?
                    WHERE id = ? AND processed_at IS NULL
                """.trimIndent()),
                any(Timestamp::class.java),
                eq(id),
            ),
        ).thenReturn(0)

        assertThrows(IllegalStateException::class.java) {
            store.markProcessed(id)
        }
    }
}
