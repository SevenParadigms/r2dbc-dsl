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
package org.springframework.data.r2dbc.repository;

import com.fasterxml.jackson.databind.JsonNode;
import io.r2dbc.spi.ConnectionFactory;
import lombok.AllArgsConstructor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan.Filter;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.data.r2dbc.config.AbstractR2dbcConfiguration;
import org.springframework.data.r2dbc.mapping.event.BeforeConvertCallback;
import org.springframework.data.r2dbc.repository.config.EnableR2dbcRepositories;
import org.springframework.data.r2dbc.repository.query.Dsl;
import org.springframework.data.r2dbc.support.JsonUtils;
import org.springframework.data.r2dbc.testing.ExternalDatabase;
import org.springframework.data.r2dbc.testing.PostgresTestSupport;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import javax.sql.DataSource;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link LegoSetRepository} using {@linkRepositoryFactorySupport} against Postgres.
 *
 * @author Mark Paluch
 * @author Jose Luis Leon
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration
public class PostgresR2dbcRepositoryIntegrationTests extends AbstractR2dbcRepositoryIntegrationTests {

	@RegisterExtension public static final ExternalDatabase database = PostgresTestSupport.database();

	@Autowired WithJsonRepository withJsonRepository;

	@Autowired WithHStoreRepository hstoreRepositoryWith;

	@Configuration
	@EnableR2dbcRepositories(considerNestedRepositories = true,
			includeFilters = @Filter(
					classes = { PostgresLegoSetRepository.class, WithJsonRepository.class, WithHStoreRepository.class },
					type = FilterType.ASSIGNABLE_TYPE))
	static class IntegrationTestConfiguration extends AbstractR2dbcConfiguration {

		@Bean
		@Override
		public ConnectionFactory connectionFactory() {
			return PostgresTestSupport.createConnectionFactory(database);
		}

		@Bean
		public BeforeConvertCallback<LegoSet> autogeneratedId(DatabaseClient client) {

			return (entity, table) -> {

				if (entity.getId() == null) {
					return client.sql("SELECT nextval('person_seq');") //
							.map(row -> row.get(0, Integer.class)) //
							.first() //
							.doOnNext(entity::setId) //
							.thenReturn(entity);
				}

				return Mono.just(entity);
			};
		}
	}

	@Override
	protected DataSource createDataSource() {
		return PostgresTestSupport.createDataSource(database);
	}

	@Override
	protected ConnectionFactory createConnectionFactory() {
		return PostgresTestSupport.createConnectionFactory(database);
	}

	@Override
	protected String getCreateTableStatement() {
		return PostgresTestSupport.CREATE_TABLE_LEGOSET + ";CREATE SEQUENCE IF NOT EXISTS person_seq;";
	}

	@Override
	protected Class<? extends LegoSetRepository> getRepositoryInterfaceType() {
		return PostgresLegoSetRepository.class;
	}

	interface PostgresLegoSetRepository extends LegoSetRepository {

		@Override
		@Query("SELECT name from lego_set")
		Flux<Named> findAsProjection();

		@Override
		@Query("SELECT * from lego_set WHERE manual = :manual")
		Mono<LegoSet> findByManual(int manual);

		@Override
		@Query("SELECT id from lego_set")
		Flux<Integer> findAllIds();
	}

	@Test
	void shouldSaveAndLoadJson() {

		JdbcTemplate template = new JdbcTemplate(createDataSource());

		template.execute("DROP TABLE IF EXISTS with_json");
		template.execute("CREATE TABLE with_json (\n" //
				+ "    id          SERIAL PRIMARY KEY,\n" //
				+ "    json_value  JSONB NOT NULL" //
				+ ");");

		WithJson person = new WithJson(null, JsonUtils.getMapper().createObjectNode().put("hello", "world"));
		withJsonRepository.save(person).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		withJsonRepository.findAll().as(StepVerifier::create).consumeNextWith(actual -> {

			assertThat(actual.jsonValue).isNotNull();
			assertThat(actual.jsonValue.toString()).isEqualTo("{\"hello\":\"world\"}");
		}).verifyComplete();
	}

	@Test // gh-492
	void shouldSaveAndLoadHStore() {

		JdbcTemplate template = new JdbcTemplate(createDataSource());

		template.execute("DROP TABLE IF EXISTS with_hstore");
		template.execute("CREATE EXTENSION IF NOT EXISTS hstore;");
		template.execute("CREATE TABLE with_hstore (" //
				+ "    id            SERIAL PRIMARY KEY," //
				+ "    hstore_value  HSTORE NOT NULL);");

		WithHstore person = new WithHstore(null, Collections.singletonMap("hello", "world"));
		hstoreRepositoryWith.save(person).as(StepVerifier::create).expectNextCount(1).verifyComplete();

		hstoreRepositoryWith.findAll().as(StepVerifier::create).consumeNextWith(actual -> {

			assertThat(actual.hstoreValue).isNotNull().containsEntry("hello", "world");
		}).verifyComplete();
	}

	@AllArgsConstructor
	static class WithJson {

		/*@Id */Long id;

		JsonNode jsonValue;

		public Long getId() {
			return id;
		}

		public JsonNode getJsonValue() {
			return jsonValue;
		}
	}

	interface WithJsonRepository extends R2dbcRepository<WithJson, Long> {

	}

	@AllArgsConstructor
//	@Table("with_hstore")
	static class WithHstore {

		/*@Id */Long id;

		Map<String, String> hstoreValue;

		public Long getId() {
			return id;
		}

		public Map<String, String> getHstoreValue() {
			return hstoreValue;
		}
	}

	interface WithHStoreRepository extends R2dbcRepository<WithHstore, Long> {

	}

	@Test
	void shouldSimpleDsl() {

		List<LegoSet> legoSets = shouldInsertNewItems();

		repository.findOne(Dsl.create().equals("manual", 13))
				.as(StepVerifier::create)
				.consumeNextWith(actual -> {
					assertThat(actual.getName()).isEqualTo("FORSCHUNGSSCHIFF");
				})
				.verifyComplete();

		repository.findOne(Dsl.create().notEquals("manual", 13))
				.as(StepVerifier::create)
				.consumeNextWith(actual -> {
					assertThat(actual.getName()).isEqualTo("SCHAUFELRADBAGGER");
				})
				.verifyComplete();

		repository.findOne(Dsl.create().equals("name", "FORSCHUNGSSCHIFF"))
				.as(StepVerifier::create)
				.consumeNextWith(actual -> {
					assertThat(actual.getManual()).isEqualTo(13);
				})
				.verifyComplete();

		repository.findOne(Dsl.create().like("name", "UNGSS"))
				.as(StepVerifier::create)
				.consumeNextWith(actual -> {
					assertThat(actual.getManual()).isEqualTo(13);
				})
				.verifyComplete();

		repository.findOne(Dsl.create().id(1L))
				.as(StepVerifier::create)
				.consumeNextWith(actual -> {
					assertThat(actual.getName()).isEqualTo("SCHAUFELRADBAGGER");
				})
				.verifyComplete();

		repository.findOne(Dsl.create().in("id", new HashSet<>(List.of(1L))))
				.as(StepVerifier::create)
				.consumeNextWith(actual -> {
					assertThat(actual.getName()).isEqualTo("SCHAUFELRADBAGGER");
				})
				.verifyComplete();

		repository.findAll(Dsl.create())
				.as(StepVerifier::create)
				.expectNextCount(2) //
				.verifyComplete();

		repository.findOne(Dsl.create().sorting("id", "desc"))
				.as(StepVerifier::create)
				.consumeNextWith(actual -> {
					assertThat(actual.getId().longValue() == 2);
				})
				.verifyComplete();

		repository.findOne(Dsl.create().sorting("id", "desc").pageable(1, 1))
				.as(StepVerifier::create)
				.consumeNextWith(actual -> {
					assertThat(actual.getId().longValue() == 1);
				})
				.verifyComplete();
	}

	@Test
	void shouldBatch() {
		LegoSet legoSet1 = new LegoSet(null, "SCHAUFELRADBAGGER", 12);
		LegoSet legoSet2 = new LegoSet(null, "FORSCHUNGSSCHIFF", 13);

		repository.saveBatch(List.of(legoSet1, legoSet2))
				.as(StepVerifier::create)
				.expectNextCount(2) //
				.verifyComplete();
	}
}
