package com.example.outbox.outbox.persistence

import com.fasterxml.jackson.databind.JsonNode
import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import org.hibernate.annotations.JdbcTypeCode
import org.hibernate.type.SqlTypes
import java.time.Instant
import java.util.UUID

@Entity
@Table(name = "outbox_events")
class OutboxEventEntity(
    @Id
    val id: UUID,
    @Column(name = "aggregate_id", nullable = false)
    val aggregateId: UUID,
    @Column(name = "event_type", nullable = false)
    val eventType: String,
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(nullable = false, columnDefinition = "jsonb")
    val payload: JsonNode,
    @Column(name = "created_at", nullable = false)
    val createdAt: Instant,
    @Column(name = "processed_at")
    val processedAt: Instant? = null,
)
