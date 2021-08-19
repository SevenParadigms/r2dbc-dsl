/*
 * Copyright 2019-2021 the original author or authors.
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
package org.springframework.data.r2dbc.core;

import java.util.function.Supplier;

import org.springframework.data.r2dbc.dialect.BindTarget;

/**
 * Extension to {@link QueryOperation} for a prepared SQL query {@link Supplier} with bound parameters. Contains
 * parameter bindings that can be {@link #bindTo bound} bound to a {@link BindTarget}.
 * <p>
 * Can be executed with {@link org.springframework.data.r2dbc.core.DatabaseClient}.
 * </p>
 *
 * @param <T> underlying operation source.
 * @author Mark Paluch
 * @see org.springframework.data.r2dbc.core.DatabaseClient#execute(Supplier)
 * @deprecated since 1.2, use Spring R2DBC's {@link org.springframework.r2dbc.core.PreparedOperation} support instead.
 */
@Deprecated
public interface PreparedOperation<T> extends QueryOperation, org.springframework.r2dbc.core.PreparedOperation<T> {

	/**
	 * @return the query source, such as a statement/criteria object.
	 */
	T getSource();

	/**
	 * Apply bindings to {@link BindTarget}.
	 *
	 * @param target the target to apply bindings to.
	 */
	void bindTo(BindTarget target);

	@Override
	default String get() {
		return toQuery();
	}

}
