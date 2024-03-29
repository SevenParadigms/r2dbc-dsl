package org.springframework.data.r2dbc.repository.cache;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.CaffeineSpec;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.cache.caffeine.CaffeineCache;
import org.springframework.context.ApplicationContext;
import org.springframework.data.r2dbc.support.Beans;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;

public class CaffeineGuidedCacheManager implements CacheManager {

    private Caffeine<Object, Object> cacheBuilder = Caffeine.newBuilder();
    @Nullable private CacheLoader<Object, Object> cacheLoader;
    private boolean allowNullValues = true;
    private final Map<String, Cache> cacheMap = new ConcurrentHashMap<>(16);
    private final Collection<String> customCacheNames = new CopyOnWriteArrayList<>();
    private final ApplicationContext applicationContext;
    private Long defaultExpireAfterAccess = 500L;
    private Long defaultExpireAfterWrite = -1L;
    private Long defaultMaximumSize = 10000L;

    public CaffeineGuidedCacheManager() {
        this.applicationContext = Beans.getApplicationContext();
    }

    public CaffeineGuidedCacheManager(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    public CaffeineGuidedCacheManager(ApplicationContext applicationContext, String... cacheNames) {
        this.applicationContext = applicationContext;
        setCacheNames(Arrays.asList(cacheNames));
    }

    public void setDefaultExpireAfterAccess(Long defaultExpireAfterAccess) {
        this.defaultExpireAfterAccess = defaultExpireAfterAccess;
    }

    public void setDefaultExpireAfterWrite(Long defaultExpireAfterWrite) {
        this.defaultExpireAfterWrite = defaultExpireAfterWrite;
    }

    public void setDefaultMaximumSize(Long defaultMaximumSize) {
        this.defaultMaximumSize = defaultMaximumSize;
    }

    public void setCacheNames(@Nullable Collection<String> cacheNames) {
        if (cacheNames != null) {
            for (String name : cacheNames) {
                this.cacheMap.put(name, createCaffeineCache(name));
            }
        }
    }

    public void setCaffeine(Caffeine<Object, Object> caffeine) {
        Assert.notNull(caffeine, "Caffeine must not be null");
        doSetCaffeine(caffeine);
    }

    public void setCaffeineSpec(CaffeineSpec caffeineSpec) {
        doSetCaffeine(Caffeine.from(caffeineSpec));
    }

    public void setCacheSpecification(String cacheSpecification) {
        doSetCaffeine(Caffeine.from(cacheSpecification));
    }

    private void doSetCaffeine(Caffeine<Object, Object> cacheBuilder) {
        if (!ObjectUtils.nullSafeEquals(this.cacheBuilder, cacheBuilder)) {
            this.cacheBuilder = cacheBuilder;
            refreshCommonCaches();
        }
    }

    public void setCacheLoader(CacheLoader<Object, Object> cacheLoader) {
        if (!ObjectUtils.nullSafeEquals(this.cacheLoader, cacheLoader)) {
            this.cacheLoader = cacheLoader;
            refreshCommonCaches();
        }
    }

    public void setAllowNullValues(boolean allowNullValues) {
        if (this.allowNullValues != allowNullValues) {
            this.allowNullValues = allowNullValues;
            refreshCommonCaches();
        }
    }

    public boolean isAllowNullValues() {
        return this.allowNullValues;
    }

    @NonNull
    @Override
    public Collection<String> getCacheNames() {
        return Collections.unmodifiableSet(this.cacheMap.keySet());
    }

    @NonNull
    @Override
    public Cache getCache(@NonNull String name) {
        return this.cacheMap.computeIfAbsent(name, this::createCaffeineCache);
    }

    public void registerCustomCache(String name, com.github.benmanes.caffeine.cache.Cache<Object, Object> cache) {
        this.customCacheNames.add(name);
        this.cacheMap.put(name, adaptCaffeineCache(name, cache));
    }

    protected Cache adaptCaffeineCache(String name, com.github.benmanes.caffeine.cache.Cache<Object, Object> cache) {
        return new CaffeineCache(name, cache, isAllowNullValues());
    }

    protected Cache createCaffeineCache(String name) {
        return adaptCaffeineCache(name, createNativeCaffeineCache(name));
    }

    protected com.github.benmanes.caffeine.cache.Cache<Object, Object> createNativeCaffeineCache(String name) {
        var context = Beans.setAndGetContext(applicationContext);
        var expireAfterAccess = context.getEnvironment()
                .getProperty("spring.cache." + name + ".expireAfterAccess", Long.class, defaultExpireAfterAccess);
        var expireAfterWrite = context.getEnvironment()
                .getProperty("spring.cache." + name + ".expireAfterWrite", Long.class, defaultExpireAfterWrite);
        var maximumSize = context.getEnvironment()
                .getProperty("spring.cache." + name + ".maximumSize", Long.class, defaultMaximumSize);
        cacheBuilder.expireAfterAccess(expireAfterAccess, TimeUnit.MILLISECONDS);
        if (expireAfterWrite > 0) {
            cacheBuilder.expireAfterWrite(expireAfterWrite, TimeUnit.MILLISECONDS);
        }
        cacheBuilder.maximumSize(maximumSize);
        cacheBuilder.initialCapacity(50);
        return (this.cacheLoader != null ? this.cacheBuilder.build(this.cacheLoader) : this.cacheBuilder.build());
    }

    private void refreshCommonCaches() {
        for (Map.Entry<String, Cache> entry : this.cacheMap.entrySet()) {
            if (!this.customCacheNames.contains(entry.getKey())) {
                entry.setValue(createCaffeineCache(entry.getKey()));
            }
        }
    }

}