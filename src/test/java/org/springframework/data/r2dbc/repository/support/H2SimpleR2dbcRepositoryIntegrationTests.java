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
package org.springframework.data.r2dbc.repository.support;

import io.r2dbc.spi.ConnectionFactory;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataAccessException;
import org.springframework.data.domain.Persistable;
import org.springframework.data.r2dbc.config.AbstractR2dbcConfiguration;
import org.springframework.data.r2dbc.config.R2dbcDslProperties;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.r2dbc.testing.H2TestSupport;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.repository.query.RelationalEntityInformation;
import org.springframework.data.relational.repository.support.MappingRelationalEntityInformation;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import reactor.test.StepVerifier;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link SimpleR2dbcRepository} against H2.
 *
 * @author Mark Paluch
 * @author Greg Turnquist
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration
public class H2SimpleR2dbcRepositoryIntegrationTests extends AbstractSimpleR2dbcRepositoryIntegrationTests {

	@Autowired private R2dbcEntityTemplate entityTemplate;

	@Autowired private RelationalMappingContext mappingContext;

	@Autowired private ApplicationContext applicationContext;

	@Configuration
	@EnableConfigurationProperties(R2dbcDslProperties.class)
	static class IntegrationTestConfiguration extends AbstractR2dbcConfiguration {

		@Override
		public ConnectionFactory connectionFactory() {
			return H2TestSupport.createConnectionFactory();
		}
	}

	@Override
	protected DataSource createDataSource() {
		return H2TestSupport.createDataSource();
	}

	@Override
	protected String getCreateTableStatement() {
		return H2TestSupport.CREATE_TABLE_LEGOSET_WITH_ID_GENERATION;
	}

	@Test // gh-90
	void shouldInsertNewObjectWithGivenId() {

		try {
			this.jdbc.execute("DROP TABLE always_new");
		} catch (DataAccessException e) {}

		this.jdbc.execute("CREATE TABLE always_new (\n" //
				+ "    id          integer PRIMARY KEY,\n" //
				+ "    name        varchar(255) NOT NULL\n" //
				+ ");");

		RelationalEntityInformation<AlwaysNew, Long> entityInformation = new MappingRelationalEntityInformation<>(
				(RelationalPersistentEntity<AlwaysNew>) mappingContext.getRequiredPersistentEntity(AlwaysNew.class));

		SimpleR2dbcRepository<AlwaysNew, Long> repository = new SimpleR2dbcRepository<>(entityInformation, entityTemplate,
				entityTemplate.getConverter(), applicationContext);

		AlwaysNew alwaysNew = new AlwaysNew(9999L, "SCHAUFELRADBAGGER");

		repository.save(alwaysNew) //
				.as(StepVerifier::create) //
				.consumeNextWith( //
						actual -> assertThat(actual.getId()).isEqualTo(9999) //
				).expectComplete();

		int count = jdbc.queryForObject("SELECT count(*) FROM always_new", Integer.class);
		Assertions.assertEquals(0, count);
	}

	@Test // gh-232
	void updateShouldFailIfRowDoesNotExist() {

		LegoSet legoSet = new LegoSet(9999, "SCHAUFELRADBAGGER", 12);

		repository.save(legoSet) //
				.as(StepVerifier::create) //
				.verifyErrorSatisfies(actual -> {
					assertThat(actual).isInstanceOf(DataAccessException.class)
							.hasMessage("Incorrect result size: expected 1, actual 0");
				});
	}

	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	static class AlwaysNew implements Persistable<Long> {

		/*@Id */Long id;
		String name;

		@Override
		public boolean isNew() {
			return true;
		}
	}
}
