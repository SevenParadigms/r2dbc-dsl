package org.springframework.data.r2dbc.repository.cache;

import io.r2dbc.spi.R2dbcType;
import org.jetbrains.annotations.Nullable;
import org.springframework.aop.support.AopUtils;
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

import static org.springframework.data.r2dbc.support.DslUtils.DOT;

abstract public class AbstractRepositoryCache<T, ID> {
    private final CacheManager cacheManager;
    private final RelationalEntityInformation<T, ID> entity;

    public AbstractRepositoryCache(RelationalEntityInformation<T, ID> entity, @Nullable ApplicationContext applicationContext) {
        applicationContext = Beans.setContext(applicationContext);
        this.cacheManager = Beans.of(CacheManager.class, new CaffeineGuidedCacheManager(applicationContext));
        this.entity = entity;
    }

    protected String getHash(Class<?> type, Dsl dsl) {
        var genericClass = AopUtils.getTargetClass(this).getSimpleName();
        var entityType = entity == null ? null : entity.getJavaType().getSimpleName();
        return entityType + DOT + type.getSimpleName() + DOT + genericClass + DOT + DslUtils.generateHash(dsl);
    }

    protected Cache getCache() {
        return cacheManager.getCache(R2dbcType.class.getSimpleName());
    }

    @Nullable
    protected T get(Class<?> keyType, Dsl dsl) {
        return getCache().get(getHash(keyType, dsl), entity == null ? null : entity.getJavaType());
    }

    @Nullable
    protected List<T> getList(Class<?> keyType, Dsl dsl) {
        return getCache().get(getHash(keyType, dsl), List.class);
    }

    protected Object put(Class<?> keyType, Dsl dsl, Object value) {
        getCache().put(getHash(keyType, dsl), value);
        return value;
    }

    protected List<T> putList(Class<?> keyType, Dsl dsl, List<T> value) {
        getCache().put(getHash(keyType, dsl), value);
        return value;
    }

    protected void evict(Class<?> keyType, Dsl dsl) {
        getCache().evict(getHash(keyType, dsl));
    }

    protected boolean contains(Class<?> type, Dsl dsl) {
        return get(type, dsl) != null;
    }

    public Mono<T> getMono(Dsl dsl, Mono<T> reload) {
        if (containsMono(dsl)) {
            return Mono.just(Objects.requireNonNull(get(Mono.class, dsl)));
        } else
            return reload.flatMap(value -> putAndGetMono(dsl, value));
    }

    public Mono<T> putAndGetMono(Dsl dsl, T value) {
        put(Mono.class, dsl, value);
        return Mono.just(value);
    }

    public void putMono(Dsl dsl, T value) {
        put(Mono.class, dsl, value);
    }

    public boolean containsMono(Dsl dsl) {
        return get(Mono.class, dsl) != null;
    }

    public Flux<T> getFlux(Dsl dsl, Flux<T> reload) {
        if (containsFlux(dsl)) {
            return Flux.fromIterable(Objects.requireNonNull(getList(Flux.class, dsl)));
        } else
            return reload.collectList().flatMapMany(value -> putAndGetFlux(dsl, value));
    }

    public Flux<T> putAndGetFlux(Dsl dsl, List<T> value) {
        putList(Flux.class, dsl, value);
        return Flux.fromIterable(value);
    }

    public void putFlux(Dsl dsl, List<T> value) {
        putList(Flux.class, dsl, value);
    }

    public boolean containsFlux(Dsl dsl) {
        var list = getList(Flux.class, dsl);
        return list != null && !list.isEmpty();
    }

    public void evictAll() {
        getCache().clear();
    }
}