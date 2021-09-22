package org.springframework.data.r2dbc.support;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.codec.binary.Hex;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.r2dbc.query.Criteria;
import org.springframework.data.r2dbc.repository.query.Dsl;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.commons.lang3.StringUtils.SPACE;
import static org.springframework.data.r2dbc.support.WordUtils.camelToSql;

/**
 * Utilities for dsl interaction.
 *
 * @author Lao Tsing
 */
public abstract class DslUtils {
    public static final String COMMANDS = "(##|!#|==|!=|>>|>=|<<|<=|~~|@@)";
    public static final String PREFIX = "(!|@|!@)";
    public static final String CLEAN = "[^#!=><~@]";
    public static final String dot = ".";
    public static final String jsonb = "->>'";

    public static String toJsonbPath(final String path) {
        if (path.contains(dot)) {
            final var paths = path.split(dot);
            final var order = new StringBuilder(paths[0]);
            for (int i = 1; i < paths.length; i++) {
                if (i < paths.length - 1)
                    order.append("->'").append(paths[i]).append("'");
                else
                    order.append(jsonb).append(paths[i]).append("'");
            }
            return order.toString();
        } else return path;
    }

    public static String getJsonName(final String jsonPath) {
        if (jsonPath.contains(jsonb)) {
            return jsonPath.substring(jsonPath.indexOf(jsonb) + 4, jsonPath.length() - 1);
        }
        return jsonPath;
    }

    public static <T> String toJsonbPath(final String dotter, final Class<T> type) {
        if (dotter.contains(dot)) {
            final var fieldName = dotter.split(dot)[0];
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
            if (fieldName.equals(field.getName())) {
                switch (field.getType().getSimpleName()) {
                    case "UUID":
                        return "'" + UUID.fromString(it) + "'::uuid";
                    case "LocalDateTime":
                    case "ZonedDateTime":
                        return "'" + it + "'::timestamp";
                    case "LocalDate":
                        return "'" + it + "'::date";
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
        var buildQuery = WordUtils.trimInline(builder);
        for (var field : FastMethodInvoker.reflectionStorage(target.getClass())) {
            if (buildQuery.contains(":" + field.getName()) && !field.getName().equals(Dsl.idProperty)) {
                var value = FastMethodInvoker.getValue(target, field.getName());
                String result = null;
                if (value != null) {
                    if (value instanceof String) result = "'" + value + "'";
                    if (value instanceof UUID) result = "'" + value + "'::uuid";
                    if (value instanceof byte[]) result = "decode('" + Hex.encodeHex((byte[]) value) + "', 'hex')";
                    if (value instanceof Enum) result = "'" + ((Enum) value).name() + "'";
                    if (value instanceof JsonNode) result = "'" + value.toString().replaceAll("'", "") + "'";
                    if (result == null) result = ConvertUtils.convert(value, String.class).toString();
                }
                buildQuery = buildQuery.replaceAll(":" + field.getName(), result == null ? "null" : result);
            }
        }
        return buildQuery;
    }

    public static <T> Criteria getCriteriaBy(Dsl dsl, Class<T> type) {
        Criteria criteriaBy = null;
        if (dsl.getQuery() != null && !dsl.getQuery().isEmpty()) {
            String[] criterias = dsl.getQuery().split(Dsl.comma);
            for (String criteria : criterias) {
                String[] parts = criteria.split(COMMANDS);
                String field = parts[0].replaceAll(PREFIX, "");
                Criteria.CriteriaStep step = criteriaBy != null ? criteriaBy.and(camelToSql(field)) : Criteria.where(camelToSql(field));
                String value = parts.length > 1 ? parts[1] : null;
                switch (criteria.replaceAll(CLEAN, "")) {
                    case "##":
                        criteriaBy = step.in(DslUtils.stringToObject(value.split(SPACE), field, type));
                        break;
                    case "!#":
                        criteriaBy = step.notIn(DslUtils.stringToObject(value.split(SPACE), field, type));
                        break;
                    case "==":
                        criteriaBy = step.is(DslUtils.stringToObject(value, field, type));
                        break;
                    case "!=":
                        criteriaBy = step.not(DslUtils.stringToObject(value, field, type));
                        break;
                    case "":
                        criteriaBy = step.is(true);
                        break;
                    case "!":
                        criteriaBy = step.not(true);
                        break;
                    case "@":
                        criteriaBy = step.isNull();
                        break;
                    case "!@":
                        criteriaBy = step.isNotNull();
                        break;
                    case ">>":
                        criteriaBy = step.greaterThan(Long.valueOf(value));
                        break;
                    case ">=":
                        criteriaBy = step.greaterThanOrEquals(Long.valueOf(value));
                        break;
                    case "<<":
                        criteriaBy = step.lessThan(Long.valueOf(value));
                        break;
                    case "<=":
                        criteriaBy = step.lessThanOrEquals(Long.valueOf(value));
                        break;
                    case "~~":
                        criteriaBy = step.like("%" + value + "%");
                        break;
                    default:
                        criteriaBy = null;
                }
            }
        }
        return criteriaBy;
    }

    public static List<String> getCriteriaFields(Dsl dsl) {
        List<String> list = new ArrayList<>();
        if (dsl.getQuery() != null && !dsl.getQuery().isEmpty()) {
            String[] criterias = dsl.getQuery().split(Dsl.comma);
            for (String criteria : criterias) {
                String[] parts = criteria.split(COMMANDS);
                list.add(parts[0].replace(PREFIX, ""));
            }
        }
        return list;
    }

    public static Pageable getPageable(Dsl dsl) {
        if (dsl.isPaged())
            return PageRequest.of(dsl.getPage(), dsl.getSize(), getSorted(dsl));
        else
            return Pageable.unpaged();
    }

    public static Sort getSorted(Dsl dsl) {
        if (dsl.isSorted()) {
            return Sort.by(Stream.of(dsl.getSort().split(Dsl.comma)).map(it -> {
                String[] parts = it.split(":");
                String name;
                if (parts[0].contains(dot)) {
                    name = DslUtils.toJsonbPath(parts[0]);
                } else
                    name = parts[0];
                return new Sort.Order(Sort.Direction.valueOf(parts[1].toUpperCase()), name);
            }).collect(Collectors.toList()));
        } else return Sort.unsorted();
    }
}