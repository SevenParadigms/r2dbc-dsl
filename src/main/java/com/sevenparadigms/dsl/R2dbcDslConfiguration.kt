package com.sevenparadigms.dsl

import com.fasterxml.jackson.databind.ObjectMapper
import io.r2dbc.spi.ConnectionFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Configuration
import org.springframework.core.convert.converter.Converter
import org.springframework.data.convert.CustomConversions.StoreConversions
import org.springframework.data.r2dbc.convert.R2dbcCustomConversions
import org.springframework.data.r2dbc.dialect.DialectResolver
import java.util.*

@Configuration
@ComponentScan("com.sevenparadigms")
class R2dbcDslConfiguration {
    @Bean
    fun r2dbcCustomConversions(objectMapper: ObjectMapper, connectionFactory: ConnectionFactory): R2dbcCustomConversions? {
        val customConversions: MutableList<Converter<*, *>?> = ArrayList()
        customConversions.add(JsonToNodeConverter(objectMapper))
        customConversions.add(NodeToJsonConverter())
        val dialect = DialectResolver.getDialect(connectionFactory)
        val converters: MutableList<Any> = ArrayList(dialect.converters)
        converters.addAll(R2dbcCustomConversions.STORE_CONVERTERS)
        return R2dbcCustomConversions(StoreConversions.of(dialect.simpleTypeHolder, converters), customConversions)
    }
}