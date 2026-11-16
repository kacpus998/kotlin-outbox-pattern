package ing.recruitment.orders.persistence

import org.springframework.data.jpa.repository.JpaRepository
import java.util.UUID

interface OrderRepository : JpaRepository<OrderEntity, UUID>
