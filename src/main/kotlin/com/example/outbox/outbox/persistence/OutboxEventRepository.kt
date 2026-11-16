package com.example.outbox.outbox.persistence

import org.springframework.data.jpa.repository.JpaRepository
import java.util.UUID

interface OutboxEventRepository : JpaRepository<OutboxEventEntity, UUID>
