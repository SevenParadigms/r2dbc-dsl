package com.sevenparadigms.common

import com.fasterxml.jackson.databind.ObjectMapper
import io.netty.util.internal.StringUtil
import org.springframework.beans.BeansException
import org.springframework.beans.factory.NoSuchBeanDefinitionException
import org.springframework.beans.factory.support.GenericBeanDefinition
import org.springframework.context.ApplicationContext
import org.springframework.context.ApplicationContextAware
import org.springframework.stereotype.Component
import org.springframework.util.ConcurrentReferenceHashMap
import org.springframework.web.context.support.GenericWebApplicationContext
import java.util.concurrent.Callable
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantReadWriteLock

/**
 * Application static context
 *
 * @author Lao Tsing
 */
@Component
class ApplicationContext : ApplicationContextAware {

    @Throws(BeansException::class)
    override fun setApplicationContext(applicationContext: ApplicationContext) {
        Companion.applicationContext = applicationContext
    }

    companion object {
        @JvmStatic
        fun <T> getBean(beanType: Class<T>): T? {
            return cache(beanType, Callable<T> {
                var bean: T? = getApplicationContext().getBean(beanType)
                val counter = AtomicInteger(awaitDelay)
                while (bean == null && counter.getAndDecrement() > 0) {
                    lock.readLock().lock()
                    try {
                        TimeUnit.MILLISECONDS.sleep(100)
                    } finally {
                        lock.readLock().unlock()
                    }
                    bean = getApplicationContext().getBean(beanType)
                }
                bean
            })
        }

        @JvmStatic
        fun <T> register(beanType: Class<T>, vararg args: Any?) {
            val context = getBean(GenericWebApplicationContext::class.java)!!
            val definition = GenericBeanDefinition()
            definition.setBeanClass(beanType)
            if (args.isNotEmpty()) {
                val chunkedList = args.asList().chunked(2)
                for (chunk in chunkedList) {
                    definition.propertyValues.add(chunk.first() as String, chunk.last())
                }
            }
            context.registerBeanDefinition(beanType.simpleName, definition)
        }

        @JvmStatic
        fun getObjectMapper(): ObjectMapper {
            var bean: ObjectMapper? = getApplicationContext().getBean(ObjectMapper::class.java)
            val counter = AtomicInteger(awaitDelay)
            while (bean == null && counter.getAndDecrement() > 0) {
                lock.readLock().lock()
                try {
                    TimeUnit.MILLISECONDS.sleep(100)
                    bean = getApplicationContext().getBean(ObjectMapper::class.java)
                } finally {
                    lock.readLock().unlock()
                }
            }
            return bean!!
        }

        @JvmStatic
        fun getProperty(name: String, vararg defaultValue: String?): String {
            lock.readLock().lock()
            try {
                return getApplicationContext().environment.getProperty(name) ?: if (defaultValue.isNotEmpty())
                    defaultValue.first()!!
                else
                    StringUtil.EMPTY_STRING
            } finally {
                lock.readLock().unlock()
            }
        }

        @JvmStatic
        fun <T> getProperty(name: String, target: Class<T>): T? {
            lock.readLock().lock()
            try {
                return applicationContext!!.environment.getProperty(name, target)
            } finally {
                lock.readLock().unlock()
            }
        }

        @Suppress("UNCHECKED_CAST")
        private fun <T> cache(requiredType: Class<T>, callable: Callable<T>): T? {
            if (OBJECTS_CACHE.containsKey(requiredType)) return OBJECTS_CACHE[requiredType] as T?
            return try {
                OBJECTS_CACHE[requiredType] = callable.call()
                OBJECTS_CACHE[requiredType] as T?
            } catch (ex: NoSuchBeanDefinitionException) {
                return null
            }
        }

        private fun getApplicationContext(): ApplicationContext {
            val counter = AtomicInteger(awaitDelay)
            while (applicationContext == null && counter.getAndDecrement() > 0) {
                lock.writeLock().lock()
                try {
                    TimeUnit.MILLISECONDS.sleep(100)
                } finally {
                    lock.writeLock().unlock()
                }
            }
            if (applicationContext == null) {
                throw RuntimeException("Spring context not initialized")
            }
            return applicationContext!!
        }

        private val lock = ReentrantReadWriteLock();
        private val OBJECTS_CACHE: ConcurrentMap<Class<*>, Any> = ConcurrentReferenceHashMap(720)
        private var applicationContext: ApplicationContext? = null

        private const val awaitDelay = 50
    }
}