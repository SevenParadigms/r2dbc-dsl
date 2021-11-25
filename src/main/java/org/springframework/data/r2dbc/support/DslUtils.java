package org.springframework.data.r2dbc.support;

import com.fasterxml.jackson.databind.JsonNode;
import kotlin.Pair;
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
import static org.springframework.data.r2dbc.support.WordUtils.*;

/**
 * Utilities for dsl interaction.
 *
 * @author Lao Tsing
 */
public abstract class DslUtils {
    public static final String COMMANDS = "(##|!#|==|!=|>>|>=|<<|<=|~~|@@)";
    public static final String PREFIX = "(!|@|!@)";
    public static final String CLEAN = "[^#!=><~@]";
    public static final String DOT = ".";
    public static final String DOT_REGEX = "\\.";
    public static final String JSONB = "->>'";

    public static String toJsonbPath(final String path) {
        if (path.contains(DOT)) {
            final var paths = path.split(DOT);
            final var order = new StringBuilder(paths[0]);
            for (int i = 1; i < paths.length; i++) {
                if (i < paths.length - 1)
                    order.append("->'").append(paths[i]).append("'");
                else
                    order.append(JSONB).append(paths[i]).append("'");
            }
            return order.toString();
        } else return path;
    }

    public static String getJsonName(final String jsonPath) {
        if (jsonPath.contains(JSONB)) {
            return jsonPath.substring(jsonPath.indexOf(JSONB) + 4, jsonPath.length() - 1);
        }
        return jsonPath;
    }

    public static <T> String toJsonbPath(final String dotter, final Class<T> type) {
        if (dotter.contains(DOT)) {
            final var fieldName = dotter.split(DOT)[0];
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
            if (sqlToCamel(lastOctet(fieldName)).equals(field.getName())) {
                switch (field.getType().getSimpleName()) {
                    case "UUID":
                        return UUID.fromString(it);
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
        var buildQuery = trimInline(builder);
        for (var field : FastMethodInvoker.reflectionStorage(target.getClass())) {
            if (buildQuery.contains(Dsl.COLON + field.getName()) && !field.getName().equals(Dsl.idProperty)) {
                var value = FastMethodInvoker.getValue(target, field.getName());
                String result = null;
                if (value != null) {
                    result = objectToSql(value);
                }
                buildQuery = buildQuery.replaceAll(Dsl.COLON + field.getName(), result == null ? "null" : result);
            }
        }
        return buildQuery;
    }

    public static String objectToSql(Object value) {
        if (value instanceof String) return "'" + value + "'";
        if (value instanceof UUID) return "'" + value + "'::uuid";
        if (value instanceof byte[]) return "decode('" + String.valueOf(Hex.encodeHex((byte[]) value)) + "', 'hex')";
        if (value instanceof Enum) return "'" + ((Enum) value).name() + "'";
        if (value instanceof JsonNode) return "'" + value.toString().replaceAll("'", "") + "'";
        return ConvertUtils.convert(value, String.class).toString();
    }

    public static <T> Criteria getCriteriaBy(Dsl dsl, Class<T> type) {
        Criteria criteriaBy = null;
        if (dsl.getQuery() != null && !dsl.getQuery().isEmpty()) {
            String[] criterias = dsl.getQuery().split(Dsl.COMMA);
            for (String criteria : criterias) {
                String[] parts = criteria.split(COMMANDS);
                String field = parts[0].replaceAll(PREFIX, "");
                Criteria.CriteriaStep step = criteriaBy != null ? criteriaBy.and(camelToSql(field)) : Criteria.where(camelToSql(field));
                String value = parts.length > 1 ? parts[1] : null;
                switch (criteria.replaceAll(CLEAN, "")) {
                    case Dsl.in:
                        if (value != null) criteriaBy = step.in(stringToObject(value.split(SPACE), field, type));
                        break;
                    case Dsl.notIn:
                        if (value != null) criteriaBy = step.notIn(stringToObject(value.split(SPACE), field, type));
                        break;
                    case Dsl.equal:
                        if (value != null) criteriaBy = step.is(stringToObject(value, field, type));
                        break;
                    case Dsl.notEqual:
                        if (value != null) criteriaBy = step.not(stringToObject(value, field, type));
                        break;
                    case "":
                        criteriaBy = step.is(true);
                        break;
                    case Dsl.not:
                        criteriaBy = step.not(true);
                        break;
                    case Dsl.isNull:
                        criteriaBy = step.isNull();
                        break;
                    case Dsl.notNull:
                        criteriaBy = step.isNotNull();
                        break;
                    case Dsl.greater:
                        if (value != null) criteriaBy = step.greaterThan(Long.valueOf(value));
                        break;
                    case Dsl.greaterEqual:
                        if (value != null) criteriaBy = step.greaterThanOrEquals(Long.valueOf(value));
                        break;
                    case Dsl.less:
                        if (value != null) criteriaBy = step.lessThan(Long.valueOf(value));
                        break;
                    case Dsl.lessEqual:
                        if (value != null) criteriaBy = step.lessThanOrEquals(Long.valueOf(value));
                        break;
                    case Dsl.like:
                        if (value != null) criteriaBy = step.like("%" + value + "%");
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
            String[] criterias = dsl.getQuery().split(Dsl.COMMA);
            for (String criteria : criterias) {
                if (criteria.contains(Dsl.fts)) continue;
                String[] parts = criteria.split(COMMANDS);
                list.add(parts[0].replace(PREFIX, ""));
            }
        }
        return list;
    }

    public static Pair<String, String> getFtsPair(Dsl dsl) {
        if (dsl.getQuery() != null && !dsl.getQuery().isEmpty()) {
            String[] criterias = dsl.getQuery().split(Dsl.COMMA);
            for (String criteria : criterias) {
                if (criteria.contains(Dsl.fts)) {
                    String[] parts = criteria.split(COMMANDS);
                    return new Pair<>(parts[0], parts[1]);
                }
            }
        }
        return null;
    }

    public static Pageable getPageable(Dsl dsl) {
        if (dsl.isPaged())
            return PageRequest.of(dsl.getPage(), dsl.getSize(), getSorted(dsl));
        else
            return Pageable.unpaged();
    }

    public static Sort getSorted(Dsl dsl) {
        if (dsl.isSorted()) {
            return Sort.by(Stream.of(dsl.getSort().split(Dsl.COMMA)).map(it -> {
                String[] parts = it.split(Dsl.COLON);
                String name;
                if (parts[0].contains(DOT)) {
                    name = toJsonbPath(parts[0]);
                } else
                    name = parts[0];
                return new Sort.Order(Sort.Direction.valueOf(parts[1].toUpperCase()), name);
            }).collect(Collectors.toList()));
        } else return Sort.unsorted();
    }
}