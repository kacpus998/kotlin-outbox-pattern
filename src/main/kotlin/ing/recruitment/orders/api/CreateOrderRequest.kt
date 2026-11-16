package ing.recruitment.orders.api

import jakarta.validation.constraints.NotBlank
import jakarta.validation.constraints.Size

data class CreateOrderRequest(
    @field:NotBlank
    @field:Size(max = 255)
    val customerId: String,
)
