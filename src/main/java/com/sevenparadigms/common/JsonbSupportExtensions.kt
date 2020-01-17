package com.sevenparadigms.common

import com.fasterxml.jackson.databind.JsonNode

fun String.toJsonbPath(): String {
    return if (this.contains('.')) {
        val paths = this.split('.')
        var order = paths.first()
        for (i in 1 until paths.size) {
            order = if (i < paths.size - 1)
                "$order->'${paths[i]}'"
            else
                "$order->>'${paths[i]}'"
        }
        order
    } else this
}

fun <T> String.toJsonbPath(cls: Class<T>): String {
    if (this.contains('.')) {
        val fieldName = this.split('.').first()
        val reflectionStorage = FastMethodInvoker.reflectionStorage(cls)
        for (field in reflectionStorage) {
            if (fieldName == field.name && field.type == JsonNode::class.java) {
                return this.toJsonbPath()
            }
        }
    }
    return this
}