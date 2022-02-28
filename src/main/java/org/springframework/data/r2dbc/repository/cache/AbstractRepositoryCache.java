package org.springframework.data.r2dbc.repository.cache;

import io.r2dbc.spi.R2dbcType;
import org.jetbrains.annotations.Nullable;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.context.ApplicationContext;
import org.springframework.data.r2dbc.config.Beans;
import org.springframework.data.r2dbc.repository.query.Dsl;
import org.springframework.data.r2dbc.support.DslUtils;
import org.springframework.data.relational.repository.query.RelationalEntityInformation;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;

abstract public class AbstractRepositoryCache<T, ID> {
    private final CacheManager cacheManager;
    private final RelationalEntityInformation<T, ID> entity;

    public AbstractRepositoryCache(RelationalEntityInformation<T, ID> entity, @Nullable ApplicationContext applicationContext) {
        applicationContext = Beans.setContext(applicationContext);
        this.cacheManager = Beans.of(CacheManager.class, new CaffeineGuidedCacheManager(applicationContext));
        this.entity = entity;
    }

    protected String getHash(Class<?> type, Dsl dsl) {
        var generic = getClass().getEnclosingClass();
        var genericClass = generic == null ? getClass().getSuperclass() : generic;
        var entityType = entity == null ? null : entity.getJavaType().getSimpleName();
        return genericClass.getSimpleName() + entityType + type.getSimpleName() + DslUtils.generateHash(dsl);
    }

    protected Cache getCache() {
        return cacheManager.getCache(R2dbcType.class.getSimpleName());
    }

    @Nullable
    protected T get(Class<?> keyType, Dsl dsl) {
        return getCache().get(getHash(keyType, dsl), entity == null ? null : entity.getJavaType());
    }

    protected Object put(Class<?> keyType, Dsl dsl, Object value) {
        getCache().put(getHash(keyType, dsl), value);
        return value;
    }

    protected void evict(Class<?> keyType, Dsl dsl) {
        getCache().evict(getHash(keyType, dsl));
    }

    public boolean contains(Class<?> type, Dsl dsl) {
        return get(type, dsl) != null;
    }

    public Mono<T> getMono(Dsl dsl, Mono<T> reload) {
        if (contains(Mono.class, dsl)) {
            return Mono.just(Objects.requireNonNull(get(Mono.class, dsl)));
        } else
            return reload.flatMap(value -> putMono(dsl, value));
    }

    public Mono<T> putMono(Dsl dsl, T value) {
        put(Mono.class, dsl, value);
        return Mono.just(value);
    }

    public boolean containsMono(Dsl dsl) {
        return get(Mono.class, dsl) != null;
    }

    public Flux<T> getFlux(Dsl dsl, Flux<T> reload) {
        if (contains(Flux.class, dsl)) {
            return Flux.fromIterable((List<T>) Objects.requireNonNull(get(Flux.class, dsl)));
        } else
            return reload.collectList().flatMapMany(value -> putFlux(dsl, value));
    }

    public Flux<T> putFlux(Dsl dsl, List<T> value) {
        put(Flux.class, dsl, value);
        return Flux.fromIterable(value);
    }

    public boolean containsFlux(Dsl dsl) {
        return get(Flux.class, dsl) != null;
    }

    public void evictAll() {
        getCache().clear();
    }
}
