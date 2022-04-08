/*
 * Copyright 2018-2021 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.r2dbc.repository;

import io.r2dbc.postgresql.api.Notification;
import io.r2dbc.spi.Result;
import org.springframework.data.r2dbc.repository.query.Dsl;
import org.springframework.data.r2dbc.repository.query.MementoPage;
import org.springframework.data.repository.NoRepositoryBean;
import org.springframework.data.repository.query.ReactiveQueryByExampleExecutor;
import org.springframework.data.repository.reactive.ReactiveSortingRepository;
import org.springframework.lang.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

/**
 * R2DBC specific {@link org.springframework.data.repository.Repository} interface with reactive support.
 *
 * @author Mark Paluch
 * @author Stephen Cohen
 * @author Greg Turnquist
 */
@NoRepositoryBean
public interface R2dbcRepository<T, ID> extends ReactiveSortingRepository<T, ID>, ReactiveQueryByExampleExecutor<T> {
    Flux<T> findAll(Dsl dsl);
    Mono<MementoPage<T>> findAllPaged(Dsl dsl);
    Mono<T> findOne(Dsl dsl);
    Mono<Integer> delete(Dsl dsl);
    Mono<Long> count(Dsl dsl);

    Flux<Notification> listener();
    Flux<Result> saveBatch(Iterable<T> models);

    R2dbcRepository<T, ID> evict(Dsl dsl);
    R2dbcRepository<T, ID> evict(ID id);
    R2dbcRepository<T, ID> evictAll();
    R2dbcRepository<T, ID> put(@Nullable T value);
    R2dbcRepository<T, ID> put(Dsl dsl, List<T> value);
    @Nullable T get(Dsl dsl);
    @Nullable T get(ID id);
    @Nullable List<T> getList(Dsl dsl);
    Mono<T> putAndGet(T value);
}
