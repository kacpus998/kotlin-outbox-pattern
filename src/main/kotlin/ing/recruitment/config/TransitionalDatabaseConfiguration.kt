package ing.recruitment.config

import io.r2dbc.spi.ConnectionFactory
import org.springframework.boot.autoconfigure.jdbc.DataSourceProperties
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.r2dbc.connection.R2dbcTransactionManager
import org.springframework.jdbc.support.JdbcTransactionManager
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.ReactiveTransactionManager
import org.springframework.transaction.reactive.TransactionalOperator
import javax.sql.DataSource

@Configuration(proxyBeanMethods = false)
class TransitionalDatabaseConfiguration {
    @Bean
    @ConfigurationProperties("spring.datasource")
    fun dataSourceProperties() = DataSourceProperties()

    @Bean
    fun dataSource(properties: DataSourceProperties): DataSource =
        properties.initializeDataSourceBuilder().build()

    @Bean
    @Primary
    fun transactionManager(dataSource: DataSource): PlatformTransactionManager =
        JdbcTransactionManager(dataSource)

    @Bean
    fun reactiveTransactionManager(connectionFactory: ConnectionFactory): ReactiveTransactionManager =
        R2dbcTransactionManager(connectionFactory)

    @Bean
    fun reactiveTransactionalOperator(transactionManager: ReactiveTransactionManager): TransactionalOperator =
        TransactionalOperator.create(transactionManager)
}
