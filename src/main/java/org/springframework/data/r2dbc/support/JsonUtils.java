package org.springframework.data.r2dbc.support;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
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
    private static final ThreadLocal<ObjectMapper> OBJECT_MAPPER_THREAD_LOCAL = ThreadLocal.withInitial(() -> {
        final var mapper = new ObjectMapper();
        final var javaTimeModule = new JavaTimeModule();
        javaTimeModule.addSerializer(ZonedDateTime.class, new ZonedDateTimeSerializer(DateTimeFormatter.ISO_ZONED_DATE_TIME));
        javaTimeModule.addDeserializer(ZonedDateTime.class, InstantDeserializer.ZONED_DATE_TIME);
        mapper.registerModule(javaTimeModule);
        mapper.registerModule(new Jdk8Module());
        mapper.registerModule(new ParameterNamesModule());
        mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        mapper.setSerializationInclusion(JsonInclude.Include.NON_EMPTY);
        mapper.configure(DeserializationFeature.FAIL_ON_MISSING_EXTERNAL_TYPE_ID_PROPERTY, false);
        mapper.configure(DeserializationFeature.FAIL_ON_UNRESOLVED_OBJECT_IDS, false);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.configure(DeserializationFeature.READ_ENUMS_USING_TO_STRING, true);
        mapper.configure(DeserializationFeature.WRAP_EXCEPTIONS, false);
        mapper.configure(SerializationFeature.WRITE_ENUMS_USING_TO_STRING, true);
        mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        mapper.configure(SerializationFeature.WRITE_ENUMS_USING_TO_STRING, true);
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

    public static Map<String, ?> jsonToMap(final JsonNode json) {
        final var map = new LinkedHashMap<String, Object>();
        final var fieldNames = json.fieldNames();
        while (fieldNames.hasNext()) {
            final var fieldName = fieldNames.next();
            final var jsonNode = json.get(fieldName);
            map.put(fieldName, nodeToObject(jsonNode));
        }
        return map;
    }

    @Nullable
    public static Object nodeToObject(final JsonNode json) {
        final var type = json.getNodeType();
        switch (type) {
            case ARRAY:
                var array = new ArrayList<>();
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
                    throw new RuntimeException(e);
                }
            case NULL:
                return null;
            case NUMBER:
                return json.numberValue();
            case POJO:
            case OBJECT:
                return jsonToMap(json);
            default:
                return json.textValue();
        }
    }

    public static JsonNode objectToJson(final Object object) {
        if (object instanceof String) {
            try {
                return getMapper().readTree((String) object);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
        if (object instanceof byte[]) {
            try {
                return getMapper().readTree((byte[]) object);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return getMapper().convertValue(object, JsonNode.class);
    }

    public static <T> T jsonToObject(final JsonNode json, final Class<T> cls) {
        return getMapper().convertValue(json, cls);
    }

    public static <T> T jsonToObject(final String json, final Class<T> cls) {
        return getMapper().convertValue(json, cls);
    }

    public static <T> ArrayList<T> jsonToList(final JsonNode json, final Class<T> cls) {
        var list = new ArrayList<T>();
        var maps = JsonUtils.jsonToObject(json, ArrayList.class);
        for (Object map : maps) {
            try {
                var constructor = cls.getConstructor();
                var obj = constructor.newInstance();
                FastMethodInvoker.setMap(obj, (LinkedHashMap<String, Object>) map);
                list.add(obj);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return list;
    }

    public static <T> String toJsonString(final Object object) {
        return objectToJson(object).toString();
    }
}
