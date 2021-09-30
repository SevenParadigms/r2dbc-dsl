package org.springframework.data.r2dbc.config.beans;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.stereotype.Component;
import org.springframework.util.ConcurrentReferenceHashMap;

import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentMap;

@Component
public class Beans implements ApplicationContextAware {
    private static ConcurrentMap<Class<?>, Object> OBJECTS_CACHE = new ConcurrentReferenceHashMap(720);
    private static ApplicationContext applicationContext = null;

    public static <T> T of(Class<T> beanType) {
        return cache(beanType, () -> getApplicationContext().getBean(beanType));
    }

    public static <T> T register(T bean, Object... args) {
        GenericApplicationContext context = of(GenericApplicationContext.class);
        context.registerBean(bean.getClass(), args);
        return bean;
    }

    public static String getProperty(String name, String... defaultValue) {
        String result = getApplicationContext().getEnvironment().getProperty(name);
        if (result == null && defaultValue.length > 0) {
            return defaultValue[0];
        }
        return result;
    }

    public static <T> T getProperty(String name, Class<T> target, T... defaultValue) {
        try {
            return getApplicationContext().getEnvironment().getProperty(name, target);
        } catch (Exception e1) {
            if (defaultValue.length > 0) {
                return defaultValue[0];
            } else {
                if (target.equals(String.class)) return (T) StringUtils.EMPTY;
                if (target.equals(Integer.class)) return (T) Integer.valueOf(-1);
                if (target.equals(Long.class)) return (T) Long.valueOf(-1);
                if (target.equals(Double.class)) return (T) Double.valueOf(-1);
                if (target.equals(Boolean.class)) return (T) Boolean.FALSE;
                try {
                    return target.getConstructor().newInstance();
                } catch (Exception e2) {
                    throw new RuntimeException(e2);
                }
            }
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
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }
}