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
import com.fasterxml.jackson.datatype.jsr310.deser.LocalDateTimeDeserializer;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateTimeSerializer;
import com.fasterxml.jackson.module.paramnames.ParameterNamesModule;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public final class JsonUtils {
    public static final String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";

    public static final ThreadLocal<ObjectMapper> OBJECT_MAPPER_THREAD_LOCAL = ThreadLocal.withInitial(() -> {
        final ObjectMapper mapper = new ObjectMapper();
        JavaTimeModule javaTimeModule = new JavaTimeModule();
        javaTimeModule.addSerializer(LocalDateTime.class, new LocalDateTimeSerializer(DateTimeFormatter.ofPattern(DATETIME_FORMAT)));
        javaTimeModule.addDeserializer(LocalDateTime.class, new LocalDateTimeDeserializer(DateTimeFormatter.ofPattern(DATETIME_FORMAT)));
        mapper.registerModule(javaTimeModule);
        mapper.registerModule(new ParameterNamesModule());
        mapper.registerModule(new Jdk8Module());
        mapper.configure(DeserializationFeature.WRAP_EXCEPTIONS, false);
        mapper.configure(DeserializationFeature.FAIL_ON_MISSING_EXTERNAL_TYPE_ID_PROPERTY, false);
        mapper.configure(DeserializationFeature.FAIL_ON_UNRESOLVED_OBJECT_IDS, false);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(DeserializationFeature.READ_ENUMS_USING_TO_STRING, true);
        mapper.configure(SerializationFeature.WRITE_ENUMS_USING_TO_STRING, true);
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        return mapper;
    });

    public static ObjectMapper getMapper() {
        return OBJECT_MAPPER_THREAD_LOCAL.get();
    }

    public static Map<String, ?> jsonToMap(JsonNode json) {
        if (json.isObject()) {
            Map<String, Object> map = new LinkedHashMap<>();
            Iterator<String> fieldNames = json.fieldNames();
            while (fieldNames.hasNext()) {
                String fieldName = fieldNames.next();
                JsonNode jsonNode = json.get(fieldName);
                map.put(fieldName, nodeToObject(jsonNode));
            }
            return map;
        } else {
            throw new IllegalArgumentException("JsonObject type needed");
        }
    }

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
            case OBJECT:
            case POJO:
                return jsonToMap(json);
            case NUMBER:
                return json.numberValue();
            case NULL:
                return null;
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

    public static <T> Object jsonToObject(JsonNode json, Class<T> cls) {
        return getMapper().convertValue(json, cls);
    }

    public static <T> String toJsonString(Object object) {
        return objectToJson(object).toString();
    }
}
