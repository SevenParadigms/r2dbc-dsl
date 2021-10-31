package org.springframework.data.r2dbc.config.beans;

import org.jetbrains.annotations.NotNull;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.util.ConcurrentReferenceHashMap;

import java.util.concurrent.Callable;

@Configuration
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

    public static String getProperty(String name, String defaultValue) {
        try {
            String result = getApplicationContext().getEnvironment().getProperty(name);
            if (result != null) {
                return result;
            }
        } catch (Exception e) {
        }
        return defaultValue;
    }

    public static <T> T getProperty(String name, Class<T> target, T defaultValue) {
        try {
            T result = getApplicationContext().getEnvironment().getProperty(name, target);
            if (result != null) {
                return result;
            }
        } catch (Exception e) {
        }
        return defaultValue;
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