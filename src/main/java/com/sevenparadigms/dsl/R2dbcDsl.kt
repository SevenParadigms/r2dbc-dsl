package com.sevenparadigms.dsl

import com.fasterxml.jackson.annotation.JsonIgnore
import com.sevenparadigms.common.FastMethodInvoker
import com.sevenparadigms.common.toJsonbPath
import io.netty.util.internal.StringUtil
import org.apache.logging.log4j.util.Strings
import org.springframework.data.r2dbc.query.Criteria
import java.math.BigInteger
import java.net.URLDecoder
import java.time.LocalDate
import java.time.LocalDateTime
import java.util.*

class R2dbcDsl {
    var query: String = StringUtil.EMPTY_STRING
    get() = URLDecoder.decode(field, Charsets.UTF_8.name()).trimInline()

    constructor()

    constructor(query: String) {
        this.query = query
    }

    var ftsLang: String = "english"
    var fields: Array<String> = arrayOf()

    fun `in`(field: Enum<*>, ids: Set<Long>): R2dbcDsl {
        if (Strings.isNotEmpty(field.name) && ids.isNotEmpty()) {
            query = start(query) + field + "##" + ids.joinToString(separator = " ")
        }
        return this
    }

    fun notIn(field: Enum<*>, ids: Set<Long>): R2dbcDsl {
        if (Strings.isNotEmpty(field.name) && ids.isNotEmpty()) {
            query = start(query) + field + "!#" + ids.joinToString(separator = " ")
        }
        return this
    }

    fun id(id: Long): R2dbcDsl {
        if (id > 0) {
            query = start(query) + "id==$id"
        }
        return this
    }

    fun isTrue(field: Enum<*>): R2dbcDsl {
        if (Strings.isNotEmpty(field.name)) {
            query = start(query) + field
        }
        return this
    }

    fun isFalse(field: Enum<*>): R2dbcDsl {
        if (Strings.isNotEmpty(field.name)) {
            query = start(query) + "!" + field
        }
        return this
    }

    fun equals(field: Enum<*>, value: Any): R2dbcDsl {
        if (Strings.isNotEmpty(field.name)) {
            query = start(query) + field + "==" + value
        }
        return this
    }

    fun notEquals(field: Enum<*>, value: Any): R2dbcDsl {
        if (Strings.isNotEmpty(field.name)) {
            query = start(query) + field + "!=" + value
        }
        return this
    }

    fun greaterThan(field: Enum<*>, value: Long): R2dbcDsl {
        if (Strings.isNotEmpty(field.name)) {
            query = start(query) + field + ">>" + value
        }
        return this
    }

    fun greaterThanOrEquals(field: Enum<*>, value: Long): R2dbcDsl {
        if (Strings.isNotEmpty(field.name)) {
            query = start(query) + field + ">=" + value
        }
        return this
    }

    fun lessThan(field: Enum<*>, value: Long): R2dbcDsl {
        if (Strings.isNotEmpty(field.name)) {
            query = start(query) + field + "<<" + value
        }
        return this
    }

    fun lessThanOrEquals(field: Enum<*>, value: Long): R2dbcDsl {
        if (Strings.isNotEmpty(field.name)) {
            query = start(query) + field + "<=" + value
        }
        return this
    }

    fun isNull(field: Enum<*>): R2dbcDsl {
        equals(field, "null")
        return this
    }

    fun isNotNull(field: Enum<*>): R2dbcDsl {
        notEquals(field, "null")
        return this
    }

    fun like(field: Enum<*>, filter: String): R2dbcDsl {
        if (!Strings.isEmpty(filter.trim())) {
            query = start(query) + field.name + "~~" + filter.trim()
        }
        return this
    }

    fun fts(field: Enum<*>, fts: String): R2dbcDsl {
        if (!Strings.isEmpty(fts)) {
            query = start(query) + field.name  + "@@" + fts.trim()
        }
        return this
    }

    fun jsonPath(field: Enum<*>, path: String): R2dbcDsl {
        if (!Strings.isEmpty(path)) {
            query = start(query) + field.name  + "@>" + path.trim()
        }
        return this
    }

    private fun start(string: String): String {
        return if (string.trim().isEmpty()) StringUtil.EMPTY_STRING else "$string,"
    }

    @JsonIgnore
    fun <T> getCriteriaBy(cls: Class<T>): Criteria? {
        var query: Criteria? = null
        if (this.query.isNotEmpty()) {
            val criterias = this.query.split(delimiter)
            for (criteria in criterias) {
                val parts = criteria.split(COMMANDS.toRegex())
                val field = parts.first().replace(PREFIX.toRegex(), "").toJsonbPath(cls)
                val step: Criteria.CriteriaStep = query?.and(field.deCamel()) ?: Criteria.where(field.deCamel())
                val value = parts.last()
                query = when (criteria.replace(CLEAN.toRegex(), "")) {
                    "##" -> step.`in`(value.split(' '))
                    "!#" -> step.notIn(value.split(' '))
                    "==" -> step.`is`(value.getType(field, cls))
                    "!=" -> step.not(value.getType(field, cls))
                    "" -> step.`is`(true)
                    "!" -> step.not(true)
                    "@" -> step.isNull
                    "!@" -> step.isNotNull
                    ">>" -> step.greaterThan(value.toLong())
                    ">=" -> step.greaterThanOrEquals(value.toLong())
                    "<<" -> step.lessThan(value.toLong())
                    "<=" -> step.lessThanOrEquals(value.toLong())
                    "~~" -> step.like("%$value%")
                    else -> null
                }
            }
        }
        return query
    }

    @JsonIgnore
    fun getQueryFields(): List<String> {
        val list = ArrayList<String>()
        if (this.query.isNotEmpty()) {
            val criterias = this.query.split(delimiter)
            for (criteria in criterias) {
                val parts = criteria.split(COMMANDS.toRegex())
                list.add(parts.first().replace(PREFIX.toRegex(), ""))
            }
        }
        return list
    }

    private fun <T> String.getType(fieldName: String, cls: Class<T>): Any {
        val reflectionStorage = FastMethodInvoker.reflectionStorage(cls)
        for (field in reflectionStorage) {
            if (fieldName.replaceDot().toCamel() == field.name) {
                when (field.type) {
                    UUID::class.java -> return UUID.fromString(this)
                    LocalDateTime::class.java -> return "'$this'::timestamp"
                    LocalDate::class.java -> return "'$this'::date"
                    Short::class.java -> return this.toShort()
                    Int::class.java -> return this.toInt()
                    Long::class.java -> return this.toLong()
                    Double::class.java -> return this.toDouble()
                    BigInteger::class.java -> return this.toBigInteger()
                    Boolean::class.java -> return this.toBoolean()
                    ByteArray::class.java -> return this.toByteArray()
                }
            }
        }
        return this
    }

    companion object {
        @JvmStatic
        fun create(): R2dbcDsl {
            return R2dbcDsl()
        }

        const val COMMANDS = "(##|!#|==|!=|>>|>=|<<|<=|~~)"
        const val PREFIX = "(!|@|!@)"
        const val CLEAN = "[^#!=><~@]"
        const val delimiter = ';'
    }
}