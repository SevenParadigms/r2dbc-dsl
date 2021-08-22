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
package org.springframework.data.r2dbc.connectionfactory.lookup;

import io.r2dbc.spi.ConnectionFactory;

import org.springframework.util.Assert;

/**
 * An implementation of {@link ConnectionFactoryLookup} that simply wraps a single given {@link ConnectionFactory},
 * returned for any connection factory name.
 *
 * @author Mark Paluch
 * @deprecated since 1.2 in favor of Spring R2DBC. Use
 *             {@link org.springframework.r2dbc.connection.lookup.SingleConnectionFactoryLookup} instead.
 */
@Deprecated
public class SingleConnectionFactoryLookup implements ConnectionFactoryLookup {

	private final ConnectionFactory connectionFactory;

	/**
	 * Create a new instance of the {@link SingleConnectionFactoryLookup} class.
	 *
	 * @param connectionFactory the single {@link ConnectionFactory} to wrap.
	 */
	public SingleConnectionFactoryLookup(ConnectionFactory connectionFactory) {

		Assert.notNull(connectionFactory, "ConnectionFactory must not be null!");

		this.connectionFactory = connectionFactory;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.r2dbc.connectionfactory.lookup.ConnectionFactoryLookup#getConnectionFactory(java.lang.String)
	 */
	@Override
	public ConnectionFactory getConnectionFactory(String connectionFactoryName)
			throws ConnectionFactoryLookupFailureException {
		return this.connectionFactory;
	}
}