package com.sevenparadigms.common

import org.springframework.boot.autoconfigure.data.web.SpringDataWebProperties
import org.springframework.data.domain.PageRequest
import org.springframework.data.domain.Pageable
import org.springframework.data.domain.Sort
import org.springframework.util.MultiValueMap
import org.springframework.web.reactive.function.server.ServerRequest
import kotlin.streams.toList

fun ServerRequest.getPageable(): Pageable {
    val params: MultiValueMap<String, String>? = this.exchange().request.queryParams
    return if (params != null && params[SpringDataWebProperties.Pageable().pageParameter] != null
            && params[SpringDataWebProperties.Pageable().sizeParameter] != null) {
        val page = params[SpringDataWebProperties.Pageable().pageParameter]?.first()!!.toInt()
        val size = params[SpringDataWebProperties.Pageable().sizeParameter]?.first()!!.toInt()
        PageRequest.of(page, size, this.getSort())
    } else
        Pageable.unpaged()
}

fun ServerRequest.getSort(): Sort {
    val params: MultiValueMap<String, String>? = this.exchange().request.queryParams
    return if (params != null && params[SpringDataWebProperties.Sort().sortParameter] != null) {
        val sort = params[SpringDataWebProperties.Sort().sortParameter]?.first().toString()
        val orders = sort.split(',').stream().map {
            val parts = it.split(Regex(":"))
            val name = if (parts.first().contains('.')) {
                parts.first().toJsonbPath()
            } else
                parts.first()
            Sort.Order(Sort.Direction.valueOf(parts.last().toUpperCase()), name)
        }.toList()
        Sort.by(orders)
    } else
        Sort.unsorted()
}