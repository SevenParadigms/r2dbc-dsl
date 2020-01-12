package com.sevenparadigms.dsl

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.r2dbc.postgresql.codec.Json
import org.springframework.core.convert.converter.Converter
import org.springframework.data.convert.ReadingConverter

@ReadingConverter
class JsonToNodeConverter(private val objectMapper: ObjectMapper) : Converter<Json, JsonNode> {
    override fun convert(json: Json): JsonNode {
        try {
            return objectMapper.readValue<JsonNode>(json.asString(), object : TypeReference<JsonNode?>() {})
        } catch (ex: JsonProcessingException) {
            ex.printStackTrace()
        }
        return objectMapper.createObjectNode() as JsonNode
    }

}