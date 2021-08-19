/*
 * Copyright 2018-2021 the original author or authors.
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
package org.springframework.data.r2dbc;

import io.r2dbc.spi.R2dbcException;

/**
 * Exception thrown when SQL specified is invalid. Such exceptions always have a {@link io.r2dbc.spi.R2dbcException}
 * root cause.
 * <p>
 * It would be possible to have subclasses for no such table, no such column etc. A custom
 * {@link org.springframework.data.r2dbc.support.R2dbcExceptionTranslator} could create such more specific exceptions,
 * without affecting code using this class.
 *
 * @author Mark Paluch
 * @deprecated since 1.2, use directly Spring R2DBC's {@link org.springframework.r2dbc.BadSqlGrammarException} instead.
 */
@Deprecated
public class BadSqlGrammarException extends org.springframework.r2dbc.BadSqlGrammarException {

	private static final long serialVersionUID = 3814579246913482054L;

	/**
	 * Creates a new {@link BadSqlGrammarException}.
	 *
	 * @param task name of current task.
	 * @param sql the offending SQL statement.
	 * @param ex the root cause.
	 */
	public BadSqlGrammarException(String task, String sql, R2dbcException ex) {
		super(task, sql, ex);
	}

	/**
	 * Return the wrapped {@link R2dbcException}.
	 */
	public R2dbcException getR2dbcException() {
		return (R2dbcException) getCause();
	}

	/**
	 * Return the SQL that caused the problem.
	 */
	public String getSql() {
		return super.getSql();
	}
}
