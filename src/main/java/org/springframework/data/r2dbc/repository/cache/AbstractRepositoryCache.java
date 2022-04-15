package org.springframework.data.r2dbc.repository.cache;

import org.apache.commons.lang3.ObjectUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.context.ApplicationContext;
import org.springframework.data.r2dbc.config.Beans;
import org.springframework.data.r2dbc.config.R2dbcDslProperties;
import org.springframework.data.r2dbc.repository.query.Dsl;
import org.springframework.data.r2dbc.support.DslUtils;
import org.springframework.data.r2dbc.support.FastMethodInvoker;
import org.springframework.data.r2dbc.support.SqlField;
import org.springframework.data.relational.repository.query.RelationalEntityInformation;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple3;
import reactor.util.function.Tuples;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.springframework.data.r2dbc.support.DslUtils.JSONB;

abstract public class AbstractRepositoryCache<T, ID> {
    private static final Logger log = LoggerFactory.getLogger(AbstractRepositoryCache.class);

    private final CacheManager cacheManager;
    @Nullable
    private final RelationalEntityInformation<T, ID> entity;

    public AbstractRepositoryCache(@Nullable RelationalEntityInformation<T, ID> entity, @Nullable ApplicationContext applicationContext) {
        applicationContext = Beans.setAndGetContext(applicationContext);
        var dslProperties = Beans.of(R2dbcDslProperties.class);
        if (dslProperties.getCacheManager()) {
            this.cacheManager = Beans.of(CacheManager.class, new CaffeineGuidedCacheManager(applicationContext));
        } else {
            this.cacheManager = new CaffeineGuidedCacheManager(applicationContext);
        }
        this.entity = entity;
        if (entity != null) {
            if (cacheManager instanceof CaffeineGuidedCacheManager) {
                if (dslProperties.getSecondCache()) {
                    ((CaffeineGuidedCacheManager) cacheManager).setDefaultExpireAfterAccess("300000");
                }
            } else {
                assert cacheManager != null;
                log.info("R2dbcRepository<" + entity.getJavaType().getSimpleName() + "> initialize with cache: " + cacheManager.getClass().getSimpleName());
            }
        }
    }

    protected String getHash(final Class<?> type, final Dsl dsl) {
        var entityType = entity == null ? null : entity.getJavaType().getSimpleName();
        return entityType + JSONB + type.getSimpleName() + JSONB + DslUtils.generateHash(dsl);
    }

    protected Tuple3<String, String, Dsl> extractHash(final String hash) {
        var arr = hash.split(JSONB);
        return Tuples.of(arr[0], arr[1], DslUtils.generateDsl(arr[2]));
    }

    protected Cache getCache() {
        return cacheManager.getCache(entity != null ? entity.getJavaType().getSimpleName() : AbstractRepositoryCache.class.getSimpleName());
    }

    @Nullable
    protected T get(final Class<?> keyType, final Dsl dsl) {
        return getCache().get(getHash(keyType, dsl), entity == null ? null : entity.getJavaType());
    }

    @Nullable
    protected List<T> getList(final Class<?> keyType, final Dsl dsl) {
        return getCache().get(getHash(keyType, dsl), List.class);
    }

    protected Object put(final Class<?> keyType, final Dsl dsl, final Object value) {
        evict(keyType, dsl);
        getCache().put(getHash(keyType, dsl), value);
        return value;
    }

    protected List<T> putList(final Class<?> keyType, final Dsl dsl, List<T> value) {
        evict(keyType, dsl);
        getCache().put(getHash(keyType, dsl), value);
        return value;
    }

    protected void evict(final Class<?> keyType, final Dsl dsl) {
        getCache().evict(getHash(keyType, dsl));
    }

    protected boolean contains(final Class<?> type, final Dsl dsl) {
        return get(type, dsl) != null;
    }

    public Mono<T> getMono(final Dsl dsl, final Mono<T> reload) {
        if (containsMono(dsl)) {
            return Mono.just(Objects.requireNonNull(get(Mono.class, dsl)));
        } else
            return reload.flatMap(value -> putAndGetMono(dsl, value));
    }

    public Mono<T> putAndGetMono(final Dsl dsl, final T value) {
        putMono(dsl, value);
        return Mono.just(value);
    }

    public void putMono(final Dsl dsl, final T value) {
        var temp = value;
        if (cacheManager instanceof CaffeineGuidedCacheManager && entity != null) {
            temp = FastMethodInvoker.clone(value);
        }
        put(Mono.class, dsl, temp);
        putList(Flux.class, dsl, new ArrayList<>(List.of(temp)));
    }

    public boolean containsMono(final Dsl dsl) {
        return get(Mono.class, dsl) != null;
    }

    public void evictMono(final Dsl dsl) {
        getCache().evict(getHash(Mono.class, dsl));
    }

    public Flux<T> getFlux(final Dsl dsl, final Flux<T> reload) {
        if (containsFlux(dsl)) {
            return Flux.fromIterable(Objects.requireNonNull(getList(Flux.class, dsl)));
        } else
            return reload.collectList().flatMapMany(value -> putAndGetFlux(dsl, value));
    }

    public Flux<T> putAndGetFlux(final Dsl dsl, final List<T> value) {
        putFlux(dsl, value);
        return Flux.fromIterable(value);
    }

    public void putFlux(final Dsl dsl, final List<T> value) {
        putList(Flux.class, dsl, value);
        if (ObjectUtils.isEmpty(dsl.getFields())) {
            for (T it : value) {
                var id = (ID) FastMethodInvoker.getValue(it, SqlField.id);
                if (id != null) {
                    put(Mono.class, Dsl.create().id(id), it);
                }
            }
        }
    }

    public boolean containsFlux(final Dsl dsl) {
        var list = getList(Flux.class, dsl);
        return list != null && !list.isEmpty();
    }

    public void evictFlux(final Dsl dsl) {
        getCache().evict(getHash(Flux.class, dsl));
    }

    @Nullable
    public T get(@NonNull final Dsl dsl) {
        return get(Mono.class, dsl);
    }

    @Nullable
    public T get(@Nullable final ID id) {
        if (ObjectUtils.isEmpty(id)) {
            return null;
        }
        return get(Mono.class, Dsl.create().id(id));
    }

    @Nullable
    public List<T> getList(@NonNull final Dsl dsl) {
        return getList(Flux.class, dsl);
    }

    @Nullable
    public Mono<T> putAndGet(@NonNull final T value) {
        var id = FastMethodInvoker.getValue(value, SqlField.id);
        return putAndGetMono(Dsl.create().id(id), value);
    }
}
