package ing.recruitment.orders.persistence

import jakarta.persistence.Column
import jakarta.persistence.Entity
import jakarta.persistence.Id
import jakarta.persistence.Table
import java.time.Instant
import java.util.UUID

@Entity
@Table(name = "orders")
class OrderEntity(
    @Id
    val id: UUID,
    @Column(name = "customer_id", nullable = false)
    val customerId: String,
    @Column(name = "created_at", nullable = false)
    val createdAt: Instant,
)
