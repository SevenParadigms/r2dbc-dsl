/*
 * Copyright 2019-2020 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.r2dbc.query;

import io.netty.util.internal.StringUtil;
import org.springframework.data.r2dbc.convert.R2dbcConverter;
import org.springframework.data.r2dbc.dialect.R2dbcDialect;
import org.springframework.data.r2dbc.mapping.SettableValue;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.sql.*;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;
import org.springframework.r2dbc.core.binding.BindMarker;
import org.springframework.r2dbc.core.binding.BindMarkers;
import org.springframework.r2dbc.core.binding.MutableBindings;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A subclass of {@link CustomQueryMapper} that maps {@link Update} to update assignments.
 *
 * @author Mark Paluch
 */
public class CustomUpdateMapper extends CustomQueryMapper {

	/**
	 * Creates a new {@link CustomQueryMapper} with the given {@link R2dbcConverter}.
	 *
	 * @param dialect must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 */
	public CustomUpdateMapper(R2dbcDialect dialect, R2dbcConverter converter) {
		super(dialect, converter);
	}

	/**
	 * Map a {@link Update} object to {@link BoundAssignments} and consider value/{@code NULL} {Bindings}.
	 *
	 * @param markers bind markers object, must not be {@literal null}.
	 * @param update update definition to map, must not be {@literal null}.
	 * @param table must not be {@literal null}.
	 * @param entity related {@link RelationalPersistentEntity}, can be {@literal null}.
	 * @return the mapped {@link BoundAssignments}.
	 */
	public BoundAssignments getMappedObject(BindMarkers markers, Update update, Table table,
			@Nullable RelationalPersistentEntity<?> entity) {
		return getMappedObject(markers, update.getAssignments(), table, entity);
	}

	/**
	 * Map a {@code assignments} object to {@link BoundAssignments} and consider value/{@code NULL} {Bindings}.
	 *
	 * @param markers bind markers object, must not be {@literal null}.
	 * @param assignments update/insert definition to map, must not be {@literal null}.
	 * @param table must not be {@literal null}.
	 * @param entity related {@link RelationalPersistentEntity}, can be {@literal null}.
	 * @return the mapped {@link BoundAssignments}.
	 */
	public BoundAssignments getMappedObject(BindMarkers markers, Map<SqlIdentifier, ? extends Object> assignments, Table table,
			@Nullable RelationalPersistentEntity<?> entity) {

		Assert.notNull(markers, "BindMarkers must not be null!");
		Assert.notNull(assignments, "Assignments must not be null!");
		Assert.notNull(table, "Table must not be null!");

		MutableBindings bindings = new MutableBindings(markers);
		List<Assignment> result = new ArrayList<>();

		assignments.forEach((column, value) -> {
			Assignment assignment = getAssignment(column.getReference(), value, bindings, table, entity);
			result.add(assignment);
		});

		return new BoundAssignments(bindings, result);
	}

	/**
	 * Map a {@link Criteria} object into {@link Condition} and consider value/{@code NULL} {Bindings}.
	 *
	 * @param markers bind markers object, must not be {@literal null}.
	 * @param criteria criteria definition to map, must not be {@literal null}.
	 * @param tables must not be {@literal null}.
	 * @return the mapped {@link BoundCondition}.
	 */
	public BoundCondition getMappedObject(BindMarkers markers, Criteria criteria, Map<String, Table> tables) {

		Assert.notNull(markers, "BindMarkers must not be null!");
		Assert.notNull(criteria, "Criteria must not be null!");
		Assert.notNull(tables, "Table must not be null!");

		Criteria current = criteria;
		MutableBindings bindings = new MutableBindings(markers);

		// reverse unroll criteria chain
		Map<Criteria, Criteria> forwardChain = new HashMap<>();

		while (current.hasPrevious()) {
			forwardChain.put(current.getPrevious(), current);
			current = current.getPrevious();
		}

		// perform the actual mapping

		Condition mapped = getCondition(current, bindings, tables, null);
		while (forwardChain.containsKey(current)) {

			Criteria nextCriteria = forwardChain.get(current);

			if (nextCriteria.getCombinator() == Criteria.Combinator.AND) {
				mapped = mapped.and(getCondition(nextCriteria, bindings, tables, null));
			}

			if (nextCriteria.getCombinator() == Criteria.Combinator.OR) {
				mapped = mapped.or(getCondition(nextCriteria, bindings, tables, null));
			}

			current = nextCriteria;
		}

		return new BoundCondition(bindings, mapped);
	}

	private Condition getCondition(Criteria criteria, MutableBindings bindings, Map<String, Table> tables,
								   @Nullable RelationalPersistentEntity<?> entity) {
		Table table = tables.get(StringUtil.EMPTY_STRING);
		String columnName = criteria.getColumn().getReference();
		if (criteria.getColumn().getReference().indexOf('.') > -1) {
			table = tables.get(columnName.substring(0, columnName.indexOf('.')));
			columnName = columnName.substring(columnName.indexOf('.') + 1);
		}

		Field propertyField = createPropertyField(entity, columnName, getMappingContext());
		TypeInformation<?> actualType = propertyField.getTypeHint().getRequiredActualType();

		Object mappedValue;
		Class<?> typeHint;

		if (criteria.getValue() instanceof SettableValue) {

			SettableValue settableValue = (SettableValue) criteria.getValue();

			mappedValue = convertValue(settableValue.getValue(), propertyField.getTypeHint());
			typeHint = getTypeHint(mappedValue, actualType.getType(), settableValue);

		} else {

			mappedValue = convertValue(criteria.getValue(), propertyField.getTypeHint());
			typeHint = actualType.getType();
		}
		Column column = table.column(columnName);
		return createCondition(column, mappedValue, typeHint, bindings, criteria.getComparator());
	}

	private Assignment getAssignment(String columnName, Object value, MutableBindings bindings, Table table,
			@Nullable RelationalPersistentEntity<?> entity) {

		Field propertyField = createPropertyField(entity, columnName, getMappingContext());
		Column column = table.column(toSql(propertyField.getMappedColumnName()));
		TypeInformation<?> actualType = propertyField.getTypeHint().getRequiredActualType();

		Object mappedValue;
		Class<?> typeHint;

		if (value instanceof SettableValue) {

			SettableValue settableValue = (SettableValue) value;

			mappedValue = convertValue(settableValue.getValue(), propertyField.getTypeHint());
			typeHint = getTypeHint(mappedValue, actualType.getType(), settableValue);

		} else {

			mappedValue = convertValue(value, propertyField.getTypeHint());

			if (mappedValue == null) {
				return Assignments.value(column, SQL.nullLiteral());
			}

			typeHint = actualType.getType();
		}

		return createAssignment(column, mappedValue, typeHint, bindings);
	}

	private Assignment createAssignment(Column column, Object value, Class<?> type, MutableBindings bindings) {

		BindMarker bindMarker = bindings.nextMarker(column.getName().getReference());
		AssignValue assignValue = Assignments.value(column, SQL.bindMarker(bindMarker.getPlaceholder()));

		if (value == null) {
			bindings.bindNull(bindMarker, type);
		} else {
			bindings.bind(bindMarker, value);
		}

		return assignValue;
	}
}
