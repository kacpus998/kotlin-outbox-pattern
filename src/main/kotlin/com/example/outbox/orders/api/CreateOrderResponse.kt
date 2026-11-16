package com.example.outbox.orders.api

import java.util.UUID

data class CreateOrderResponse(
    val orderId: UUID,
)
