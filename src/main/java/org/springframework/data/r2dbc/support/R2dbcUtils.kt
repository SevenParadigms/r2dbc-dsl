package org.springframework.data.r2dbc.support

import io.r2dbc.spi.ConnectionFactories
import org.springframework.data.r2dbc.core.DefaultReactiveDataAccessStrategy
import org.springframework.data.r2dbc.dialect.DialectResolver
import org.springframework.data.r2dbc.repository.support.R2dbcRepositoryFactory
import org.springframework.r2dbc.core.DatabaseClient

object R2dbcUtils {
    fun <T> getRepository(r2dbcUrl: String, cls: Class<T>): T {
        val connectionFactory = ConnectionFactories.get(r2dbcUrl)
        val databaseClient = DatabaseClient.builder().connectionFactory(connectionFactory).build()
        return R2dbcRepositoryFactory(
            databaseClient,
            DefaultReactiveDataAccessStrategy(DialectResolver.getDialect(connectionFactory))
        ).getRepository(cls)
    }
}