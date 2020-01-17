package com.sevenparadigms.common

import org.springframework.cglib.reflect.FastMethod
import org.springframework.util.ConcurrentReferenceHashMap
import java.lang.reflect.Field

class FastMethodInvoker {
    companion object {
        @JvmStatic
        fun reflectionStorage(classKey: Class<*>): List<Field> {
            return if (reflectionStorage.containsKey(classKey))
                reflectionStorage[classKey]!!
            else {
                val reflectionDeclaredFields = ArrayList<Field>()
                var recursion: Class<*>? = classKey
                do {
                    reflectionDeclaredFields.addAll(recursion!!.declaredFields)
                } while (recursion!!.superclass.also { recursion = it } != null)
                reflectionStorage[classKey] = reflectionDeclaredFields
                reflectionDeclaredFields
            }
        }

        @JvmStatic
        fun getFastMethod(key: String): FastMethod? {
            return methodStorage[key]
        }

        @JvmStatic
        fun setFastMethod(key: String, fastMethod: FastMethod) {
            methodStorage[key] = fastMethod
        }

        private val reflectionStorage = ConcurrentReferenceHashMap<Class<*>, List<Field>>(720)
        private val methodStorage = ConcurrentReferenceHashMap<String, FastMethod>(720)
    }
}

fun Any.isField(name: String): Boolean {
    for (field in FastMethodInvoker.reflectionStorage(this.javaClass)) {
        if (field.name == name) return true
    }
    return false
}

fun Any.getField(name: String): Field? {
    for (field in FastMethodInvoker.reflectionStorage(this.javaClass)) {
        if (field.name == name) return field
    }
    return null
}

fun Any.copyTo(target: Any): Any {
    for (sourceField in FastMethodInvoker.reflectionStorage(this.javaClass)) {
        if (target.isField(sourceField.name)) {
            target.setValue(sourceField.name, this.getValue(sourceField.name))
        }
    }
    return target
}