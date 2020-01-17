package com.sevenparadigms.common

import org.springframework.cglib.reflect.FastClass

fun Any.getMapValues(): Map<String, Any?> {
    return FastMethodInvoker.reflectionStorage(this.javaClass).map { it.name to it.getValue(it.name) }.toMap()
}

fun Collection<Any>.getMapValues(keyName: String, valueName: String): Map<String, Any?> {
    return this.map { it.getValue(keyName) as String to it.getValue(valueName) as Any }.toMap()
}

fun <S> Any.setValue(name: String, value: S?) {
    for (reflectionField in FastMethodInvoker.reflectionStorage(this.javaClass)) {
        if (reflectionField.name == name) {
            val methodName = "set${name.capitalize()}"
            val fastMethodKey = "${this.javaClass.name}.$methodName"
            var fastMethod = FastMethodInvoker.getFastMethod(fastMethodKey)
            if (fastMethod == null) {
                fastMethod = FastClass.create(this.javaClass).getMethod(methodName, arrayOf(reflectionField.javaClass))
                FastMethodInvoker.setFastMethod(fastMethodKey, fastMethod)
            }
            fastMethod!!.invoke(this, arrayOf(value as Any))
        }
    }
}

fun Any.getValue(name: String): Any? {
    for (reflectionField in FastMethodInvoker.reflectionStorage(this.javaClass)) {
        if (reflectionField.name == name) {
            for(prefix in arrayOf("get", "is")) {
                val methodName = "$prefix${name.capitalize()}"
                val fastMethodKey = "${this.javaClass.name}.$methodName"
                var fastMethod = FastMethodInvoker.getFastMethod(fastMethodKey)
                if (fastMethod == null) {
                    try {
                        fastMethod = FastClass.create(this.javaClass).getMethod(methodName, null)
                    } catch (ex: NoSuchMethodError) {
                    }
                    if (fastMethod != null) {
                        FastMethodInvoker.setFastMethod(fastMethodKey, fastMethod)
                    }
                }
                if (fastMethod != null) {
                    return fastMethod.invoke(this, null)
                }
            }
        }
    }
    return null
}