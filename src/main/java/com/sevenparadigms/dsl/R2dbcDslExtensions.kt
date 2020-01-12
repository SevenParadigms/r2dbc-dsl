package com.sevenparadigms.dsl

fun String.toCamel(): String {
    return "_([a-z])".toRegex().replace(this) {
        it.value.toUpperCase().removePrefix("_")
    }
}

fun String.replaceDot(): String {
    return this.replace(".", "_")
}

fun String.trimInline(): String {
    return this.replace("[\\s\\n]".toRegex(), " ")
}

fun String.deCamel(): String {
    return "([A-Z])".toRegex().replace(this) {
        "_" + it.value.toLowerCase()
    }
}