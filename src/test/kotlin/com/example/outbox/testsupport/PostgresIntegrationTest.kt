package com.example.outbox.testsupport

import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.containers.PostgreSQLContainer

abstract class PostgresIntegrationTest {
    companion object {
        private val postgres by lazy {
            PostgreSQLContainer<Nothing>("postgres:17-alpine").apply { start() }
        }

        @JvmStatic
        @DynamicPropertySource
        fun configureDataSource(registry: DynamicPropertyRegistry) {
            val container = postgres
            registry.add("spring.datasource.url", container::getJdbcUrl)
            registry.add("spring.datasource.username", container::getUsername)
            registry.add("spring.datasource.password", container::getPassword)
        }
    }
}
