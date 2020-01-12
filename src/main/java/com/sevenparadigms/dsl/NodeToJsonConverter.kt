package com.sevenparadigms.dsl

import com.fasterxml.jackson.databind.JsonNode
import io.r2dbc.postgresql.codec.Json
import org.springframework.core.convert.converter.Converter
import org.springframework.data.convert.WritingConverter

@WritingConverter
class NodeToJsonConverter : Converter<JsonNode, Json> {
    override fun convert(source: JsonNode): Json {
        return Json.of(source.toString())
    }
}