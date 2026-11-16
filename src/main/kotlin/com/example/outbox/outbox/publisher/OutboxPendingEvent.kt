package com.example.outbox.outbox.publisher

import com.fasterxml.jackson.databind.JsonNode
import java.time.Instant
import java.util.UUID

data class OutboxPendingEvent(
    val id: UUID,
    val eventType: String,
    val aggregateId: UUID,
    val payload: JsonNode,
    val createdAt: Instant,
)
