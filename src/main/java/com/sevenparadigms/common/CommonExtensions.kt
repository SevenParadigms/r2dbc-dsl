package com.sevenparadigms.common

import com.fasterxml.jackson.databind.JsonNode
import org.slf4j.LoggerFactory

fun ByteArray.hex() = this.joinToString(separator = "") { String.format("%02X", (it.toInt() and 0xFF)) }

fun String.hexToByteArray() = ByteArray(this.length / 2) { this.substring(it * 2, it * 2 + 2).toInt(16).toByte() }

fun Any.info(message: String, vararg args: Any) {
    LoggerFactory.getLogger(this.javaClass).info(message, args)
}

fun Any.warn(message: String, vararg args: Any) {
    LoggerFactory.getLogger(this.javaClass).warn(message, args)
}

fun Any.error(message: String, vararg args: Any) {
    LoggerFactory.getLogger(this.javaClass).error(message, args)
}

fun Any.debug(message: String, vararg args: Any) {
    LoggerFactory.getLogger(this.javaClass).debug(message, args)
}

fun Any.toJsonNode(): JsonNode {
    when(this) {
        is String -> return ApplicationContext.getObjectMapper().readTree(this)
        is ByteArray -> return ApplicationContext.getObjectMapper().readTree(this)
    }
    return ApplicationContext.getObjectMapper().convertValue(this, JsonNode::class.java)
}

fun <T> Any.parseJson(cls: Class<T>): T {
    return ApplicationContext.getObjectMapper().convertValue(this, cls)
}

fun Any.toJsonString(): String {
    return toJsonNode().toString()
}