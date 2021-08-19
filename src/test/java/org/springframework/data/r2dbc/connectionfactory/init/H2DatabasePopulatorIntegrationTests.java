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
package org.springframework.data.r2dbc.connectionfactory.init;

import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import reactor.test.StepVerifier;

import java.util.UUID;

import org.junit.jupiter.api.Test;

/**
 * Integration tests for {@link DatabasePopulator} using H2.
 *
 * @author Mark Paluch
 */
class H2DatabasePopulatorIntegrationTests extends AbstractDatabaseInitializationTests {

	private final UUID databaseName = UUID.randomUUID();

	private final ConnectionFactory connectionFactory = ConnectionFactories
			.get("r2dbc:h2:mem:///" + databaseName + "?options=DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE");

	@Override
	ConnectionFactory getConnectionFactory() {
		return this.connectionFactory;
	}

	@Test
	void shouldRunScript() {

		databasePopulator.addScript(usersSchema());
		databasePopulator.addScript(resource("db-test-data-h2.sql"));
		// Set statement separator to double newline so that ";" is not
		// considered a statement separator within the source code of the
		// aliased function 'REVERSE'.
		databasePopulator.setSeparator("\n\n");

		DatabasePopulatorUtils.execute(databasePopulator, connectionFactory).as(StepVerifier::create).verifyComplete();

		assertUsersDatabaseCreated(connectionFactory, "White");
	}
}
