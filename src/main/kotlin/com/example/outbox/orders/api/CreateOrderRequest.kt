package com.example.outbox.orders.api

import jakarta.validation.constraints.NotBlank

data class CreateOrderRequest(
    @field:NotBlank
    val customerId: String,
)
