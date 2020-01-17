package com.sevenparadigms.common

import com.fasterxml.jackson.databind.JsonNode
import org.slf4j.LoggerFactory

fun ByteArray.hex(): String {
    val sb = StringBuilder()
    for (b in this) {
        sb.append(String.format("%02x", b))
    }
    return sb.toString()
}

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