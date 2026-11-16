package com.example.outbox

import com.example.outbox.testsupport.PostgresIntegrationTest
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationContext
import org.springframework.boot.test.context.SpringBootTest

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class OutboxApplicationIntegrationTest : PostgresIntegrationTest() {
    @Autowired
    private lateinit var applicationContext: ApplicationContext

    @Test
    fun `application context starts with PostgreSQL`() {
        assertNotNull(applicationContext.getBean(OutboxApplication::class.java))
    }

}
