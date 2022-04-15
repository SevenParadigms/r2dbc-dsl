package org.springframework.data.r2dbc.repository.cache;

import org.springframework.data.r2dbc.repository.R2dbcRepository;
import org.springframework.data.r2dbc.repository.query.Dsl;
import org.springframework.lang.Nullable;
import reactor.core.publisher.Mono;

import java.util.List;

public interface CacheApi<T, ID> {
    R2dbcRepository<T, ID> evict(Dsl dsl);
    R2dbcRepository<T, ID> evict(@Nullable ID id);
    R2dbcRepository<T, ID> evictAll();
    R2dbcRepository<T, ID> put(@Nullable T value);
    R2dbcRepository<T, ID> put(Dsl dsl, List<T> value);
    @Nullable T get(Dsl dsl);
    @Nullable T get(@Nullable ID id);
    @Nullable List<T> getList(Dsl dsl);
    @Nullable Mono<T> putAndGet(T value);
}
