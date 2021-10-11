package org.springframework.data.r2dbc.config.beans;

import org.jetbrains.annotations.NotNull;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
import org.springframework.util.ConcurrentReferenceHashMap;

import java.util.concurrent.Callable;

@Component
public class Beans implements ApplicationContextAware {
    private static ConcurrentReferenceHashMap OBJECTS_CACHE = new ConcurrentReferenceHashMap(720);
    private static ApplicationContext applicationContext = null;

    public static <T> T of(Class<T> beanType) {
        return cache(beanType, () -> getApplicationContext().getBean(beanType));
    }

    public static <T> T register(T bean, Object... args) {
        GenericApplicationContext context = of(GenericApplicationContext.class);
        context.registerBean(bean.getClass(), args);
        return bean;
    }

    @NonNull
    public static String getProperty(String name, String defaultValue) {
        String result = getApplicationContext().getEnvironment().getProperty(name);
        if (result == null) {
            return defaultValue;
        }
        return result;
    }

    @NonNull
    public static <T> T getProperty(String name, Class<T> target, T defaultValue) {
        try {
            return getApplicationContext().getEnvironment().getProperty(name, target);
        } catch (Exception e1) {
            return defaultValue;
        }
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

    public static ApplicationContext getApplicationContext() {
        return applicationContext;
    }

    @Override
    public void setApplicationContext(@NotNull ApplicationContext applicationContext) throws BeansException {
        Beans.applicationContext = applicationContext;
    }
}