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
import org.springframework.data.r2dbc.config.Beans;
import org.springframework.data.r2dbc.mapping.event.BeforeConvertCallback;
import org.springframework.data.r2dbc.repository.cache.AbstractRepositoryCache;
import org.springframework.data.r2dbc.repository.config.EnableR2dbcRepositories;
import org.springframework.data.r2dbc.repository.query.Dsl;
import org.springframework.data.r2dbc.support.JsonUtils;
import org.springframework.data.r2dbc.testing.ExternalDatabase;
import org.springframework.data.r2dbc.testing.PostgresTestSupport;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.util.Assert;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import javax.sql.DataSource;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link LegoSetRepository} using {linkRepositoryFactorySupport} against Postgres.
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
	void shouldAnnotationsVerify() {
		List<LegoSet> legoSets = shouldInsertNewItems();

		repository.findById(2)
				.as(StepVerifier::create)
				.consumeNextWith(actual -> {
					assertThat(actual.getName()).isEqualTo("FORSCHUNGSSCHIFF");
					assertThat(actual.getManual()).isEqualTo(13);
					assertThat(actual.getVersion()).isEqualTo(1);
					assertThat(actual.getNow()).isNotNull();
				})
				.verifyComplete();

		// try to change equality attr name and get exception
		var legoSet = legoSets.get(legoSets.size() - 1);
		legoSet.setName("123");

		repository.save(legoSet) //
				.as(StepVerifier::create) //
				.expectError(IllegalStateException.class);

		// try to change readonly attr manual and get old value
		legoSet.setName("FORSCHUNGSSCHIFF");
		legoSet.setManual(123);

		repository.save(legoSet) //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {
					assertThat(actual.getManual()).isEqualTo(13);
				})
				.verifyComplete();

	}

	@Test
	void shouldSimpleDsl() {
		List<LegoSet> legoSets = shouldInsertNewItems();
		assert legoSets.size() == 2;

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

		repository.findOne(Dsl.create().in("id", 1L))
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

		repository.findAll(Dsl.create().greaterThanOrEquals("id", 1L).limit(2))
				.as(StepVerifier::create)
				.expectNextCount(2) //
				.verifyComplete();

		repository.findAll(Dsl.create().greaterThanOrEquals("id", 2L).limit(1))
				.as(StepVerifier::create)
				.expectNextCount(1) //
				.verifyComplete();

		repository.findAll(Dsl.create().greaterThanOrEquals("id", 2L).fields(" id", "name").limit(1))
				.as(StepVerifier::create)
				.expectNextCount(1) //
				.verifyComplete();

		repository.findAll(Dsl.create().notIn("id", 2L).fields(" id", "name").limit(1))
				.as(StepVerifier::create)
				.expectNextCount(1) //
				.verifyComplete();

		repository.findAll(Dsl.create().in("id", 2L).fields(" id", "name").limit(1))
				.as(StepVerifier::create)
				.expectNextCount(1) //
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

	@Test
	void shouldBeans() {
		Assert.notNull(Beans.getApplicationContext(), "Application context must not be null!");
		Assert.notNull(Beans.of(DatabaseClient.class), "DatabaseClient must not be null!");
	}

	static class CacheTest extends AbstractRepositoryCache<String, UUID> {
		public CacheTest() {
			super(null, Beans.getApplicationContext());
			var id = UUID.fromString("00000000-0000-0000-0000-000000000000");
			put(String.class, Dsl.create().id(id), "test1");
			var hash = getHash(String.class, Dsl.create().id(id));
			var contains = contains(String.class, Dsl.create().id(id));
			var value = Objects.requireNonNull(get(String.class, Dsl.create().id(id)));
			Assert.isTrue(hash.equals("null.String.CacheTest.-569643752"), "must equals");
			Assert.isTrue(contains, "should be contain");
			Assert.isTrue(value.equals("test1"), "value is equal");
			evict(String.class, Dsl.create().id(id));
			contains = contains(String.class, Dsl.create().id(id));
			Assert.isTrue(!contains, "should not contain");
		}
	}

	@Test
	void shouldMonoCacheWorking() {
		var cacheLayer = new CacheTest();
		var id = UUID.randomUUID();
		cacheLayer.putMono(Dsl.create().id(id), "one");
		Assert.isTrue(cacheLayer.containsMono(Dsl.create().id(id)), "hash is contains");

		cacheLayer.getMono(Dsl.create().id(id), Mono.just("two"))
				.as(StepVerifier::create)
				.consumeNextWith(actual -> Assert.isTrue(actual.equals("one"), "must equals"))
				.verifyComplete();

		cacheLayer.getMono(Dsl.create().id(UUID.randomUUID()), Mono.just("two"))
				.as(StepVerifier::create)
				.consumeNextWith(actual -> Assert.isTrue(actual.equals("two"), "must equals"))
				.verifyComplete();
	}

	@Test
	void shouldFluxCacheWorking() {
		var cacheLayer = new CacheTest();
		var id = UUID.randomUUID();
		cacheLayer.putFlux(Dsl.create().id(id), List.of("many"));
		Assert.isTrue(cacheLayer.containsFlux(Dsl.create().id(id)), "hash is contains");

		cacheLayer.getFlux(Dsl.create().id(id), Flux.fromIterable(List.of("many", "mickey"))).collectList()
				.as(StepVerifier::create)
				.consumeNextWith(list -> Assert.isTrue(list.get(0).equals("many"), "must equals"))
				.verifyComplete();

		cacheLayer.getFlux(Dsl.create().id(UUID.randomUUID()), Flux.fromIterable(List.of("many", "mickey"))).collectList()
				.as(StepVerifier::create)
				.consumeNextWith(list -> Assert.isTrue(list.get(1).equals("mickey"), "must equals"))
				.verifyComplete();
	}

	@Test
	void shouldCountByDsl() {
		LegoSet legoSet1 = new LegoSet(null, "SCHAUFELRADBAGGER", 12);
		LegoSet legoSet2 = new LegoSet(null, "FORSCHUNGSSCHIFF", 13);

		repository.saveBatch(List.of(legoSet1, legoSet2))
				.as(StepVerifier::create)
				.expectNextCount(2) //
				.verifyComplete();

		repository.count(Dsl.create())
				.as(StepVerifier::create)
				.consumeNextWith(count -> Assert.isTrue(count == 2, "must equals"))
				.verifyComplete();

		repository.count(Dsl.create().equals("manual", 12))
				.as(StepVerifier::create)
				.consumeNextWith(count -> Assert.isTrue(count == 1, "must equals"))
				.verifyComplete();
	}
}
