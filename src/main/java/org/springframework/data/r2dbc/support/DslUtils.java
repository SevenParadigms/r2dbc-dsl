package org.springframework.data.r2dbc.support;

import com.fasterxml.jackson.databind.JsonNode;
import kotlin.Pair;
import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.MurmurHash2;
import org.apache.commons.lang3.StringUtils;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.r2dbc.config.Beans;
import org.springframework.data.r2dbc.query.Criteria;
import org.springframework.data.r2dbc.repository.query.Dsl;
import org.springframework.lang.Nullable;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Field;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.*;
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
    public static final String COMMANDS = "(\\^\\^|!\\^|==|!=|>>|>=|<<|<=|~~|@@)";
    public static final String PREFIX = "(!|@|!@)";
    public static final String CLEAN = "[^!=>\\^<~@]";
    public static final String DOT = ".";
    public static final String OR = "(";
    public static final String GROUP_COMBINATOR = "\"";
    public static final String COMBINATORS = "(\\(|\\)|\")";
    public static final String DOT_REGEX = "\\.";
    public static final String JSONB = "->>'";

    public enum Fields {createdAt, updatedAt, version}

    public static String toJsonbPath(final String path) {
        if (path.contains(DOT)) {
            final var paths = path.split(DOT_REGEX);
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
            final var fieldName = dotter.split(DOT_REGEX)[0];
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
                        return LocalDateTime.parse(it);
                    case "ZonedDateTime":
                        return ZonedDateTime.parse(it);
                    case "LocalDate":
                        return LocalDate.parse(it);
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
            if (buildQuery.contains(Dsl.COLON + field.getName() + Dsl.COLON) && !field.getName().equals(SqlField.id)) {
                var value = FastMethodInvoker.getValue(target, field.getName());
                String result = null;
                if (value != null) {
                    result = objectToSql(value);
                }
                buildQuery = buildQuery.replaceAll(Dsl.COLON + field.getName() + Dsl.COLON, result == null ? "null" : result);
            }
        }
        return buildQuery;
    }

    public static String objectToSql(@Nullable Object value) {
        if (value == null) return "null";
        if (value instanceof LocalDateTime || value instanceof ZonedDateTime)  return "'" +
                (value.toString().indexOf("+") > 0 ? value.toString().substring(0, value.toString().indexOf("+")) : value.toString()) + "'::timestamp";
        if (value instanceof LocalDate)  return "'" + value + "'::date";
        if (value instanceof String) return "'" + value + "'";
        if (value instanceof UUID) return "'" + value + "'::uuid";
        if (value instanceof byte[]) return "decode('" + String.valueOf(Hex.encodeHex((byte[]) value)) + "', 'hex')";
        if (value instanceof Enum) return "'" + ((Enum) value).name() + "'";
        if (value instanceof JsonNode) return "'" + value.toString().replaceAll("'", "") + "'";
        return ConvertUtils.convert(value, String.class).toString();
    }

    @Nullable
    public static <T> Criteria getCriteriaBy(Dsl dsl, Class<T> type, @Nullable List<String> jsonNodeFields) {
        Criteria criteriaBy = null;
        if (dsl.getQuery() != null && !dsl.getQuery().isEmpty()) {
            String[] criterias = dsl.getQuery().split(Dsl.COMMA);
            for (String criteria : criterias) {
                String[] parts = criteria.split(COMMANDS);
                String field = parts[0].replaceAll(PREFIX, "").replaceAll(COMBINATORS, "");
                if (jsonNodeFields != null && jsonNodeFields.contains(field)) {
                    field = toJsonbPath(field, type);
                }
                Criteria.CriteriaStep step = getCriteriaByCombinators(criteriaBy, field, parts[0]);
                String value = parts.length > 1 ? parts[1].replaceAll(COMBINATORS, "") : null;
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
                        if (value != null) criteriaBy = step.greaterThan(stringToObject(value, field, type));
                        break;
                    case Dsl.greaterEqual:
                        if (value != null) criteriaBy = step.greaterThanOrEquals(stringToObject(value, field, type));
                        break;
                    case Dsl.less:
                        if (value != null) criteriaBy = step.lessThan(stringToObject(value, field, type));
                        break;
                    case Dsl.lessEqual:
                        if (value != null) criteriaBy = step.lessThanOrEquals(stringToObject(value, field, type));
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

    @Nullable
    public static Pair<String, String> getFtsPair(Dsl dsl, Class<?> type) {
        if (dsl.getQuery() != null && !dsl.getQuery().isEmpty()) {
            String[] criterias = dsl.getQuery().split(Dsl.COMMA);
            for (String criteria : criterias) {
                if (criteria.contains(Dsl.fts)) {
                    String[] parts = criteria.split(COMMANDS);
                    if (parts[0].contains(DOT)) {
                        Field entityField = ReflectionUtils.findField(type, parts[0].split(DOT_REGEX)[0]);
                        if (entityField != null && entityField.getType() == JsonNode.class) {
                            parts[0] = toJsonbPath(parts[0], type);
                        }
                    }
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
                return new Sort.Order(Sort.Direction.valueOf(parts[1].toUpperCase()), camelToSql(name));
            }).collect(Collectors.toList()));
        } else return Sort.unsorted();
    }

    public static void setVersion(Object objectToSave, Field version, Object versionValue) {
        if (versionValue instanceof Number) {
            var longValue = (Long) ConvertUtils.convert(versionValue, Long.class);
            if (version.getType() == Long.class) {
                FastMethodInvoker.setValue(objectToSave, version.getName(), longValue + 1);
            }
            if (version.getType() == Integer.class) {
                var value = (Integer) ConvertUtils.convert(longValue + 1, Integer.class);
                FastMethodInvoker.setValue(objectToSave, version.getName(), value);
            }
        }
        setNowStamp(objectToSave, version);
    }

    public static void setNowStamp(Object objectToSave, Field field) {
        if (field.getType() == LocalDateTime.class) {
            FastMethodInvoker.setValue(objectToSave, field.getName(), LocalDateTime.now());
        }
        if (field.getType() == ZonedDateTime.class) {
            FastMethodInvoker.setValue(objectToSave, field.getName(), ZonedDateTime.now());
        }
        if (field.getType() == OffsetDateTime.class) {
            FastMethodInvoker.setValue(objectToSave, field.getName(), OffsetDateTime.now());
        }
    }

    public static Set<Field> getFields(Object objectToSave, Enum<?> name, Class<?>... cls) {
        return FastMethodInvoker.getFields(objectToSave.getClass(), name.name(), cls);
    }

    public static Set<Field> getPropertyFields(Object objectToSave, String property) {
        var result = new HashSet<Field>();
        var propertyString = Beans.getProperty(property, StringUtils.EMPTY);
        if (!propertyString.isEmpty()) {
            result.addAll(Arrays.stream(propertyString.split(Dsl.COMMA))
                    .map(name -> FastMethodInvoker.getField(objectToSave.getClass(), name)).filter(Objects::nonNull)
                    .collect(Collectors.toSet()));
        }
        return result;
    }

    public static int generateHash(Dsl dsl) {
        return MurmurHash2.hash32(dsl.getQuery() + " " + dsl.getPage() + " " + dsl.getSize() + " " + dsl.getSort() + " " + Arrays.toString(dsl.getFields()));
    }

    private static Criteria.CriteriaStep getCriteriaByCombinators(@Nullable Criteria criteriaBy,
                                                                  String field,
                                                                  String part) {
        boolean hasOrCombinator = part.contains(OR);
        if (criteriaBy != null) {
            if (hasOrCombinator) {
                return criteriaBy.or(camelToSql(field));
            } else {
                return criteriaBy.and(camelToSql(field));
            }
        }
        return Criteria.where(camelToSql(field));
    }

    @Nullable
    private static Criteria getCriteriaByGroupCombinators(@Nullable Criteria criteriaBy, String part) {
        boolean hasOrCombinator = part.contains(OR);
        boolean hasGroupCombinator = part.contains(GROUP_COMBINATOR);
        List<Criteria> groupCriterias = new ArrayList<>();
        if (criteriaBy != null && hasGroupCombinator) {
            Criteria previousCriteria = criteriaBy.getPrevious();
            while (previousCriteria != null) {
                groupCriterias.add(previousCriteria);
                previousCriteria = previousCriteria.getPrevious();
            }
            return hasOrCombinator ? criteriaBy.or(groupCriterias) : criteriaBy.and(groupCriterias);
        }
        return criteriaBy;
    }
}