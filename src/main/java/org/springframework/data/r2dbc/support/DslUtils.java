package org.springframework.data.r2dbc.support;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.codec.binary.Hex;
import org.springframework.data.r2dbc.repository.query.Dsl;

import java.lang.reflect.Field;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Utilities for dsl interaction.
 *
 * @author Lao Tsing
 */
public abstract class DslUtils {
    public static final String UUID_REGEX = "[0-9a-fA-F]{8}\\-[0-9a-fA-F]{4}\\-[0-9a-fA-F]{4}\\-[0-9a-fA-F]{4}\\-[0-9a-fA-F]{12}";
    public static final String NUMBER_REGEX = "^\\d+$";
    public static final String FLOAT_REGEX = "\\d+\\.\\d+";

    public static String toJsonbPath(final String path) {
        if (path.contains(".")) {
            final var paths = path.split(".");
            final var order = new StringBuilder(paths[0]);
            for (int i = 1; i < paths.length; i++) {
                if (i < paths.length - 1)
                    order.append("->'").append(paths[i]).append("'");
                else
                    order.append("->>'").append(paths[i]).append("'");
            }
            return order.toString();
        } else return path;
    }

    public static <T> String toJsonbPath(final String dotter, final Class<T> type) {
        if (dotter.contains(".")) {
            final var fieldName = dotter.split(".")[0];
            final var reflectionStorage = FastMethodInvoker.reflectionStorage(type);
            for (var field : reflectionStorage) {
                if (fieldName.equals(field.getName()) && field.getType() == JsonNode.class) {
                    return toJsonbPath(dotter);
                }
            }
        }
        return dotter;
    }

    public static <T> List<Object> stringToObject(final String[] list, final String fieldName, final Class<T> type) {
        final var result = new ArrayList<>();
        for (var it : list) {
            result.add(stringToObject(it, fieldName, type));
        }
        return result;
    }

    public static <T> Object stringToObject(final String it, final String fieldName, final Class<T> type) {
        final var reflectionStorage = FastMethodInvoker.reflectionStorage(type);
        for (var field : reflectionStorage) {
            if (WordUtils.dotToCamel(fieldName).equals(field.getName())) {
                switch (field.getType().getSimpleName()) {
                    case "UUID":
                        return UUID.fromString(it);
                    case "LocalDateTime":
                        return "'$this'::timestamp";
                    case "LocalDate":
                        return "'$this'::date";
                    case "Short":
                        return Short.valueOf(it);
                    case "Integer":
                    case "int":
                        return Integer.valueOf(it);
                    case "Long":
                        return Long.parseLong(it);
                    case "Double":
                        return Double.valueOf(it);
                    case "BigInteger":
                        return BigInteger.valueOf(Long.parseLong(it));
                    case "Boolean":
                        return Boolean.valueOf(it);
                    case "byte[]":
                        return it.getBytes(StandardCharsets.UTF_8);
                }
                break;
            }
        }
        return it;
    }

    public static String binding(String builder, Object target) {
        var buildQuery = WordUtils.trimInline(builder.toString());
        for (var field : FastMethodInvoker.reflectionStorage(target.getClass())) {
            if (buildQuery.contains(":" + field.getName()) && !field.getName().equals(Dsl.idProperty)) {
                var value = FastMethodInvoker.getValue(target, field.getName());
                var result = "null";
                if (value != null) {
                    if (value instanceof String) result = "'" + value + "'";
                    if (value instanceof UUID) result = "'" + value + "'::uuid";
                    if (value instanceof byte[]) result = "decode('" + Hex.encodeHex((byte[]) value) + "', 'hex')";
                    if (value instanceof Enum) result = "'" + ((Enum) value).name() + "'";
                    if (value instanceof JsonNode) result = "'" + value.toString().replaceAll("'", "") + "'";
                    if (result == null) result = ConvertUtils.convert(value, String.class).toString();
                }
                buildQuery = buildQuery.replaceAll(":" + field.getName(), result);
            }
        }
        return buildQuery;
    }

    public static Object getObject(final String object) {
        if (object.matches(NUMBER_REGEX)) return Long.parseLong(object);
        if (object.matches(FLOAT_REGEX)) return Double.parseDouble(object);
        if (Arrays.asList(Boolean.TRUE.toString(), Boolean.FALSE.toString()).contains(object.toLowerCase()))
            return Boolean.TRUE.toString().equalsIgnoreCase(object);
        if (object.matches(UUID_REGEX)) return UUID.fromString(object);
        return null;
    }
}