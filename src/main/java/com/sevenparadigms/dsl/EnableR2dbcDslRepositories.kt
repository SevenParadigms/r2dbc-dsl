package com.sevenparadigms.dsl

import org.springframework.boot.autoconfigure.ImportAutoConfiguration
import org.springframework.core.annotation.AliasFor
import org.springframework.data.r2dbc.repository.config.EnableR2dbcRepositories

@EnableR2dbcRepositories(repositoryFactoryBeanClass = R2dbcDslRepositoryFactoryBean::class, databaseClientRef = "databaseClient")
@ImportAutoConfiguration(R2dbcDslConfiguration::class)
annotation class EnableR2dbcDslRepositories(
        @get:AliasFor(annotation = EnableR2dbcRepositories::class, value = "basePackages") val basePackages: Array<String> = [])