package ing.recruitment

import ing.recruitment.testsupport.PostgresIntegrationTest
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationContext
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.ReactiveTransactionManager

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class OutboxApplicationIntegrationTest : PostgresIntegrationTest() {
    @Autowired
    private lateinit var applicationContext: ApplicationContext

    @Test
    fun `application context starts with PostgreSQL`() {
        assertNotNull(applicationContext.getBean(OutboxApplication::class.java))
    }

    @Test
    fun `business transaction configuration is reactive without a JDBC transaction manager`() {
        assertTrue(applicationContext.getBeansOfType(ReactiveTransactionManager::class.java).isNotEmpty())
        assertTrue(applicationContext.getBeansOfType(PlatformTransactionManager::class.java).isEmpty())
    }

}
