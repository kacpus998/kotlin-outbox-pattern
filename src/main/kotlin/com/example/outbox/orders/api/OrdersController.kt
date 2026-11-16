package com.example.outbox.orders.api

import com.example.outbox.orders.application.OrderService
import jakarta.validation.Valid
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.ResponseStatus
import org.springframework.web.bind.annotation.RestController
import io.swagger.v3.oas.annotations.Operation
import io.swagger.v3.oas.annotations.media.Content
import io.swagger.v3.oas.annotations.media.ExampleObject
import io.swagger.v3.oas.annotations.media.Schema
import io.swagger.v3.oas.annotations.responses.ApiResponse

@RestController
@RequestMapping("/orders")
class OrdersController(
    private val orderService: OrderService,
) {
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    @Operation(
        summary = "Create an order",
        description = "Creates an order and its OrderCreated outbox event atomically. customerId must be non-blank and at most 255 characters.",
        requestBody = io.swagger.v3.oas.annotations.parameters.RequestBody(
            required = true,
            content = [Content(schema = Schema(implementation = CreateOrderRequest::class), examples = [ExampleObject(value = """{"customerId":"123"}""")])],
        ),
        responses = [
            ApiResponse(responseCode = "201", description = "Order created", content = [Content(schema = Schema(implementation = CreateOrderResponse::class))]),
            ApiResponse(responseCode = "400", description = "Invalid request or malformed JSON"),
        ],
    )
    fun createOrder(@Valid @RequestBody request: CreateOrderRequest): CreateOrderResponse =
        CreateOrderResponse(orderId = orderService.createOrder(request.customerId))
}
