package com.sevenparadigms.dsl

import io.r2dbc.postgresql.api.Notification
import org.springframework.data.domain.Pageable
import org.springframework.data.repository.NoRepositoryBean
import org.springframework.data.repository.reactive.ReactiveCrudRepository
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

@NoRepositoryBean
interface R2dbcDslRepository<T, ID> : ReactiveCrudRepository<T, ID> {
    fun <S : Any> saveBatch(entities: Iterable<S>): Flux<Int>
    fun listen(): Flux<Notification>
    fun findAll(dsl: R2dbcDsl, pageable: Pageable): Flux<T>
    fun findAll(pageable: Pageable): Flux<T>
    fun findBy(dsl: R2dbcDsl): Mono<T>
    fun fts(dsl: R2dbcDsl, pageable: Pageable): Flux<T>
    fun oper(dsl: R2dbcDsl, pageable: Pageable): Flux<T>
}