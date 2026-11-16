package ing.recruitment.orders.domain

import java.util.UUID

data class OrderCreated(
    val orderId: UUID,
    val customerId: String,
)
