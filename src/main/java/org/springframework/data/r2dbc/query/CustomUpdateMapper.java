/*
 * Copyright 2019 the original author or authors.
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

import org.springframework.data.r2dbc.convert.R2dbcConverter;
import org.springframework.data.r2dbc.dialect.BindMarker;
import org.springframework.data.r2dbc.dialect.BindMarkers;
import org.springframework.data.r2dbc.dialect.Bindings;
import org.springframework.data.r2dbc.dialect.MutableBindings;
import org.springframework.data.r2dbc.mapping.SettableValue;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.sql.*;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import java.util.ArrayList;
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
	 * @param converter must not be {@literal null}.
	 */
	public CustomUpdateMapper(R2dbcConverter converter) {
		super(converter);
	}

	/**
	 * Map a {@link Update} object to {@link BoundAssignments} and consider value/{@code NULL} {@link Bindings}.
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
	 * Map a {@code assignments} object to {@link BoundAssignments} and consider value/{@code NULL} {@link Bindings}.
	 *
	 * @param markers bind markers object, must not be {@literal null}.
	 * @param assignments update/insert definition to map, must not be {@literal null}.
	 * @param table must not be {@literal null}.
	 * @param entity related {@link RelationalPersistentEntity}, can be {@literal null}.
	 * @return the mapped {@link BoundAssignments}.
	 */
	public BoundAssignments getMappedObject(BindMarkers markers, Map<String, ? extends Object> assignments, Table table,
			@Nullable RelationalPersistentEntity<?> entity) {

		Assert.notNull(markers, "BindMarkers must not be null!");
		Assert.notNull(assignments, "Assignments must not be null!");
		Assert.notNull(table, "Table must not be null!");

		MutableBindings bindings = new MutableBindings(markers);
		List<Assignment> result = new ArrayList<>();

		assignments.forEach((column, value) -> {
			Assignment assignment = getAssignment(column, value, bindings, table, entity);
			result.add(assignment);
		});

		return new BoundAssignments(bindings, result);
	}

	private Assignment getAssignment(String columnName, Object value, MutableBindings bindings, Table table,
			@Nullable RelationalPersistentEntity<?> entity) {

		Field propertyField = createPropertyField(entity, columnName, getMappingContext());
		Column column = table.column(propertyField.getMappedColumnName());
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

		BindMarker bindMarker = bindings.nextMarker(column.getName());
		AssignValue assignValue = Assignments.value(column, SQL.bindMarker(bindMarker.getPlaceholder()));

		if (value == null) {
			bindings.bindNull(bindMarker, type);
		} else {
			bindings.bind(bindMarker, value);
		}

		return assignValue;
	}
}
