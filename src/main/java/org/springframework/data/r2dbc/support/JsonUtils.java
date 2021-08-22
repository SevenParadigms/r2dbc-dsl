package org.springframework.data.r2dbc.support;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.datatype.jsr310.deser.InstantDeserializer;
import com.fasterxml.jackson.datatype.jsr310.ser.ZonedDateTimeSerializer;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;
import reactor.util.annotation.Nullable;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Utilities for json interaction.
 *
 * @author Lao Tsing
 */
public abstract class JsonUtils {
    public static final ThreadLocal<ObjectMapper> OBJECT_MAPPER_THREAD_LOCAL = ThreadLocal.withInitial(() -> {
        final ObjectMapper mapper = new ObjectMapper();
        JavaTimeModule javaTimeModule = new JavaTimeModule();
        javaTimeModule.addSerializer(ZonedDateTime.class, new ZonedDateTimeSerializer(DateTimeFormatter.ISO_ZONED_DATE_TIME));
        javaTimeModule.addDeserializer(ZonedDateTime.class, InstantDeserializer.ZONED_DATE_TIME);
        mapper.registerModule(javaTimeModule);
        mapper.registerModule(new Jdk8Module());
        mapper.registerModule(new ParameterNamesModule());
        mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        mapper.configure(DeserializationFeature.FAIL_ON_MISSING_EXTERNAL_TYPE_ID_PROPERTY, false);
        mapper.configure(DeserializationFeature.FAIL_ON_UNRESOLVED_OBJECT_IDS, false);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(DeserializationFeature.READ_ENUMS_USING_TO_STRING, true);
        mapper.configure(DeserializationFeature.WRAP_EXCEPTIONS, false);
        mapper.configure(SerializationFeature.WRITE_ENUMS_USING_TO_STRING, true);
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        return mapper;
    });

    public static ObjectMapper getMapper() {
        return OBJECT_MAPPER_THREAD_LOCAL.get();
    }

    public static JsonNode mapToJson(final Map<String, Object> map) {
        return getMapper().valueToTree(map);
    }

    public static Map<String, ?> objectToMap(final Object object) {
        return getMapper().convertValue(object, Map.class);
    }

    public static Map<String, ?> jsonToMap(JsonNode json) {
        Map<String, Object> map = new LinkedHashMap<>();
        Iterator<String> fieldNames = json.fieldNames();
        while (fieldNames.hasNext()) {
            String fieldName = fieldNames.next();
            JsonNode jsonNode = json.get(fieldName);
            map.put(fieldName, nodeToObject(jsonNode));
        }
        return map;
    }

    @Nullable
    public static Object nodeToObject(JsonNode json) {
        JsonNodeType type = json.getNodeType();
        switch (type) {
            case ARRAY:
                List<Object> array = new ArrayList<>();
                for (JsonNode node : json) {
                    array.add(nodeToObject(node));
                }
                return array;
            case BOOLEAN:
                return json.booleanValue();
            case BINARY:
                try {
                    return json.binaryValue();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            case NULL:
                return null;
            case NUMBER:
                return json.numberValue();
            case OBJECT:
            case POJO:
                return jsonToMap(json);
            default:
                return json.textValue();
        }
    }

    public static JsonNode objectToJson(Object object) {
        if (object instanceof String) {
            try {
                return getMapper().readTree((String) object);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        }
        if (object instanceof byte[]) {
            try {
                return getMapper().readTree((byte[]) object);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return getMapper().convertValue(object, JsonNode.class);
    }

    public static <T> T jsonToObject(JsonNode json, Class<T> cls) {
        return getMapper().convertValue(json, cls);
    }

    public static <T> String toJsonString(Object object) {
        return objectToJson(object).toString();
    }
}