package org.springframework.data.r2dbc.repository.query;

import io.netty.util.internal.StringUtil;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.r2dbc.query.Criteria;
import org.springframework.data.r2dbc.support.DslUtils;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.springframework.data.r2dbc.support.WordUtils.camelToSql;


public class Dsl {
    public static Dsl create() {
        return new Dsl(StringUtil.EMPTY_STRING);
    }

    public static final String COMMANDS = "(##|!#|==|!=|>>|>=|<<|<=|~~)";
    public static final String PREFIX = "(!|@|!@)";
    public static final String CLEAN = "[^#!=><~@]";
    public static final String delimiter = ";";
    public static final String defaultId = "id";

    public String query = StringUtil.EMPTY_STRING;
    public String getQuery() {
        String decodedQuery = null;
        try {
            decodedQuery = URLDecoder.decode(query, UTF_8.displayName()).trim();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return decodedQuery;
    }

    public Dsl(final String query) {
        this.query = query;
    }

    public String lang = "english";
    public String[] fields = new String[0];
    public Integer page = -1;
    public Integer size = -1;
    public String sort = StringUtil.EMPTY_STRING;

    public boolean isPaged() {
        return page != -1 && size != -1;
    }
    public Pageable getPageable() {
        if (isPaged())
            return PageRequest.of(page, size, getSorted());
        else
            return Pageable.unpaged();
    }
    public Dsl pageable(int page, int size) {
        this.page = page;
        this.size = size;
        return this;
    }

    public boolean isSorted() {
        return !sort.isEmpty() && sort.contains(":");
    }
    public Sort getSorted() {
        if (isSorted()) {
            return Sort.by(Stream.of(sort.split(",")).map(it -> {
                String[] parts = it.split(":");
                String name;
                if (parts[0].contains(".")) {
                    name = DslUtils.toJsonbPath(parts[0]);
                } else
                    name = parts[0];
                return new Sort.Order(Sort.Direction.valueOf(parts[1].toUpperCase()), name);
            }).collect(Collectors.toList()));
        }
        else return Sort.unsorted();
    }
    public Dsl sorting(String field, Sort.Direction direction) {
        if (!sort.isEmpty()) {
            sort += ",";
        }
        sort += (field + ":" + direction);
        return this;
    }

    public Dsl fields(List<String> fields) {
        if (fields != null) {
            this.fields = fields.toArray(new String[0]);
        }
        return this;
    }

    public Dsl in(String field, Set<Long> ids) {
        if (field != null && !ids.isEmpty()) {
            query = start(query) + field + "##" + ids.stream().map(Object::toString).collect(Collectors.joining(" "));
        }
        return this;
    }

    public Dsl notIn(String field, Set<Long> ids) {
        if (field != null && !ids.isEmpty()) {
            query = start(query) + field + "!#" + ids.stream().map(Object::toString).collect(Collectors.joining(" "));
        }
        return this;
    }

    public Dsl id(String id) {
        Object object = DslUtils.getObject(id);
        if (object instanceof UUID) return id((UUID) object);
        if (object instanceof Long) return id((Long) object);
        if (object instanceof Integer) return id((Integer) object);
        return this;
    }

    public Dsl id(UUID id) {
        if (id != null) {
            query = start(query) + defaultId + "=='" + id + "'::uuid";
        }
        return this;
    }

    public Dsl id(Long id) {
        if (id != null) {
            query = start(query) + defaultId + "==" + id;
        }
        return this;
    }

    public Dsl id(Integer id) {
        if (id != null) {
            query = start(query) + defaultId + "==" + id;
        }
        return this;
    }

    public Dsl isTrue(String field) {
        if (field != null) {
            query = start(query) + field;
        }
        return this;
    }

    public Dsl filter(String field, Object value) {
        if (field != null) {
            query = start(query) + field + "==";
            if (value instanceof UUID) query += "'" + value + "':uuid";
            else if (value instanceof Number) query += value;
            else query += "'" + value + "'";
        }
        return this;
    }

    public Dsl isFalse(String field) {
        if (field != null) {
            query = start(query) + "!" + field;
        }
        return this;
    }

    public Dsl equals(String field, Object value) {
        if (field != null) {
            query = start(query) + field + "==" + value;
        }
        return this;
    }

    public Dsl notEquals(String field, Object value) {
        if (field != null) {
            query = start(query) + field + "!=" + value;
        }
        return this;
    }

    public Dsl greaterThan(String field, Long value) {
        if (field != null) {
            query = start(query) + field + ">>" + value;
        }
        return this;
    }

    public Dsl greaterThanOrEquals(String field, Long value) {
        if (field != null) {
            query = start(query) + field + ">=" + value;
        }
        return this;
    }

    public Dsl lessThan(String field, Long value) {
        if (field != null) {
            query = start(query) + field + "<<" + value;
        }
        return this;
    }

    public Dsl lessThanOrEquals(String field, Long value) {
        if (field != null) {
            query = start(query) + field + "<=" + value;
        }
        return this;
    }

    public Dsl isNull(String field) {
        query = start(query) + "@" + field;
        return this;
    }

    public Dsl isNotNull(String field) {
        query = start(query) + "!@" + field;
        return this;
    }

    public Dsl like(String field, String filter) {
        if (field != null && filter != null) {
            query = start(query) + field + "~~" + filter.trim();
        }
        return this;
    }

    private String start(String string) {
        if (string.trim().isEmpty())
            return StringUtil.EMPTY_STRING;
        else
            return string + ",";
    }

    public <T> Criteria getCriteriaBy(Class<T> type) {
        Criteria criteriaBy = null;
        if (!query.isEmpty()) {
            String[] criterias = getQuery().split(delimiter);
            for (String criteria : criterias) {
                String[] parts = criteria.split(COMMANDS);
                String field = parts[0].replaceAll(PREFIX, "");
                Criteria.CriteriaStep step = criteriaBy != null ? criteriaBy.and(camelToSql(field)) : Criteria.where(camelToSql(field));
                String value = parts.length > 1 ? parts[1] : null;
                switch (criteria.replaceAll(CLEAN, "")) {
                    case "##": criteriaBy = step.in(DslUtils.stringToObject(value.split(" "), field, type)); break;
                    case "!#": criteriaBy = step.notIn(DslUtils.stringToObject(value.split(" "), field, type)); break;
                    case "==": criteriaBy = step.is(DslUtils.stringToObject(value, field, type)); break;
                    case "!=": criteriaBy = step.not(DslUtils.stringToObject(value, field, type)); break;
                    case "": criteriaBy = step.is(true); break;
                    case "!": criteriaBy = step.not(true); break;
                    case "@": criteriaBy = step.isNull(); break;
                    case "!@": criteriaBy = step.isNotNull(); break;
                    case ">>": criteriaBy = step.greaterThan(Long.valueOf(value)); break;
                    case ">=": criteriaBy = step.greaterThanOrEquals(Long.valueOf(value)); break;
                    case "<<": criteriaBy = step.lessThan(Long.valueOf(value)); break;
                    case "<=": criteriaBy = step.lessThanOrEquals(Long.valueOf(value)); break;
                    case "~~": criteriaBy = step.like("%" + value + "%"); break;
                    default: criteriaBy = null;
                }
            }
        }
        return criteriaBy;
    }

    public List<String> getCriteriaFields() {
        List<String> list = new ArrayList<>();
        if (!query.isEmpty()) {
            String[] criterias = getQuery().split(delimiter);
            for (String criteria : criterias) {
                String[] parts = criteria.split(COMMANDS);
                list.add(parts[0].replace(PREFIX, ""));
            }
        }
        return list;
    }

    public List<String> getResultFields() {
        if (fields.length > 0) {
            return Arrays.asList(fields);
        }
        return new ArrayList<>();
    }

    public void setResultFields(List<String> fields) {
        this.fields = fields.toArray(new String[0]);
    }
}