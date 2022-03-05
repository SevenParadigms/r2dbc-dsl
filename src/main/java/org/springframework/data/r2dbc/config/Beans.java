package org.springframework.data.r2dbc.config;

import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.lang.Nullable;
import org.springframework.util.ConcurrentReferenceHashMap;

import java.util.AbstractMap;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.Supplier;

@Configuration(proxyBeanMethods = false)
public class Beans implements ApplicationContextAware {
    private static final AbstractMap<Object, Object> OBJECTS_CACHE = new ConcurrentReferenceHashMap<>(720);
    @Nullable private static ApplicationContext applicationContext = null;

    public static <T> T of(Class<T> beanType) {
        return cache(beanType, () -> getApplicationContext().getBean(beanType));
    }

    public static <T> T of(Class<T> beanType, T defaultValue) {
        try {
            return of(beanType);
        } catch (Exception ignore) {
            return defaultValue;
        }
    }

    @Nullable
    public static <T> T add(@Nullable T bean) {
        if (!Objects.isNull(bean)) {
            OBJECTS_CACHE.put(bean.getClass(), bean);
        }
        return bean;
    }

    public static String getProperty(String name, String defaultValue) {
        return getApplicationContext() != null ?
                Objects.requireNonNull(getApplicationContext().getEnvironment().getProperty(name)) : defaultValue;
    }

    public static <T> T getProperty(String name, Class<T> target, T defaultValue) {
        return getApplicationContext() != null ?
                Objects.requireNonNull(getApplicationContext().getEnvironment().getProperty(name, target)) : defaultValue;
    }

    public static <T> T register(T bean) {
        if (getApplicationContext() instanceof GenericApplicationContext) {
            var context = (GenericApplicationContext) getApplicationContext();
            context.registerBean(bean.getClass().getName(), bean.getClass(), (Supplier<T>) () -> bean);
            return bean;
        } else
            throw new RuntimeException("Context is not GenericApplicationContext");
    }

    public static void register(Class<?> bean, Object... args) {
        if (getApplicationContext() instanceof GenericApplicationContext) {
            var context = (GenericApplicationContext) getApplicationContext();
            context.registerBean(bean, args);
        } else
            throw new RuntimeException("Context is not GenericApplicationContext");
    }

    private static <T> T cache(Class<T> requiredType, Callable<T> callable) {
        if (OBJECTS_CACHE.get(requiredType) == null) {
            try {
                T result = callable.call();
                if (result != null) {
                    OBJECTS_CACHE.put(requiredType, result);
                    return result;
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return (T) OBJECTS_CACHE.get(requiredType);
    }

    @Nullable
    public static ApplicationContext getApplicationContext() {
        return applicationContext;
    }

    @Override
    public void setApplicationContext(@Nullable ApplicationContext applicationContext) {
        setAndGetContext(applicationContext);
    }

    public static ApplicationContext setAndGetContext(@Nullable ApplicationContext applicationContext) {
        if (applicationContext != null) {
            Beans.applicationContext = applicationContext;
        }
        assert Beans.applicationContext != null;
        return Beans.applicationContext;
    }
}