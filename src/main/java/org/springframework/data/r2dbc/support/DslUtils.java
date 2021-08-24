package org.springframework.data.r2dbc.support;

import com.fasterxml.jackson.databind.JsonNode;

import java.lang.reflect.Field;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

/**
 * Utilities for dsl interaction.
 *
 * @author Lao Tsing
 */
public abstract class DslUtils {
    public static final String UUID_REGEX = "[0-9a-fA-F]{8}\\-[0-9a-fA-F]{4}\\-[0-9a-fA-F]{4}\\-[0-9a-fA-F]{4}\\-[0-9a-fA-F]{12}";
    public static final String NUMBER_REGEX = "^\\d+$";
    public static final String FLOAT_REGEX = "\\d+\\.\\d+";

    public static String toJsonbPath(String path) {
        if (path.contains(".")) {
            String[] paths = path.split(".");
            StringBuilder order = new StringBuilder(paths[0]);
            for (int i = 1; i < paths.length; i++) {
                if (i < paths.length - 1)
                    order.append("->'").append(paths[i]).append("'");
                else
                    order.append("->>'").append(paths[i]).append("'");
            }
            return order.toString();
        } else return path;
    }

    public static <T> String toJsonbPath(String dotter, Class<T> type) {
        if (dotter.contains(".")) {
            String fieldName = dotter.split(".")[0];
            List<Field> reflectionStorage = FastMethodInvoker.reflectionStorage(type);
            for (Field field : reflectionStorage) {
                if (fieldName.equals(field.getName()) && field.getType() == JsonNode.class) {
                    return toJsonbPath(dotter);
                }
            }
        }
        return dotter;
    }

    public static <T> List<Object> stringToObject(String[] list, String fieldName, Class<T> type) {
        List<Object> result = new ArrayList<>();
        for (String it : list) {
            result.add(stringToObject(it, fieldName, type));
        }
        return result;
    }

    public static <T> Object stringToObject(String it, String fieldName, Class<T> type) {
        List<Field> reflectionStorage = FastMethodInvoker.reflectionStorage(type);
        for (Field field : reflectionStorage) {
            if (WordUtils.dotToCamel(fieldName).equals(field.getName())) {
                switch (field.getType().getSimpleName()) {
                    case "UUID": return UUID.fromString(it);
                    case "LocalDateTime": return "'$this'::timestamp";
                    case "LocalDate": return "'$this'::date";
                    case "Short": return Short.valueOf(it);
                    case "Integer": case "int": return Integer.valueOf(it);
                    case "Long": return Long.parseLong(it);
                    case "Double": return Double.valueOf(it);
                    case "BigInteger": return BigInteger.valueOf(Long.parseLong(it));
                    case "Boolean": return Boolean.valueOf(it);
                    case "byte[]": return it.getBytes(StandardCharsets.UTF_8);
                }
                break;
            }
        }
        return it;
    }

    public static Object getObject(String object) {
       if (object.matches(NUMBER_REGEX)) return Long.parseLong(object);
       if (object.matches(FLOAT_REGEX)) return Double.parseDouble(object);
       if (Arrays.asList(Boolean.TRUE.toString(), Boolean.FALSE.toString()).contains(object.toLowerCase())) return Boolean.TRUE.toString().equalsIgnoreCase(object);
       if (object.matches(UUID_REGEX)) return UUID.fromString(object);
       return null;
    }
}