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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.r2dbc.spi.ConnectionFactory;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan.Filter;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.FilterType;
import org.springframework.data.annotation.CreatedBy;
import org.springframework.data.domain.Sort;
import org.springframework.data.r2dbc.config.AbstractR2dbcConfiguration;
import org.springframework.data.r2dbc.expression.ExpressionParserCache;
import org.springframework.data.r2dbc.mapping.event.BeforeConvertCallback;
import org.springframework.data.r2dbc.repository.cache.AbstractRepositoryCache;
import org.springframework.data.r2dbc.repository.config.EnableR2dbcRepositories;
import org.springframework.data.r2dbc.repository.query.Dsl;
import org.springframework.data.r2dbc.repository.query.UpdatedBy;
import org.springframework.data.r2dbc.repository.security.AuthenticationIdentifierResolver;
import org.springframework.data.r2dbc.support.Beans;
import org.springframework.data.r2dbc.support.JsonUtils;
import org.springframework.data.r2dbc.support.R2dbcUtils;
import org.springframework.data.r2dbc.testing.ExternalDatabase;
import org.springframework.data.r2dbc.testing.PostgresTestSupport;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.util.Assert;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import javax.sql.DataSource;
import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for {@link LegoSetRepository} using {linkRepositoryFactorySupport} against Postgres.
 *
 * @author Mark Paluch
 * @author Jose Luis Leon
 */
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = { PostgresR2dbcRepositoryIntegrationTests.IntegrationTestConfiguration.class })
@TestPropertySource(properties = {
		"spring.r2dbc.dsl.cacheManager=true",
		"spring.r2dbc.dsl.equality=nameEquality",
		"spring.r2dbc.dsl.readOnly=manualReadOnly",
		"spring.r2dbc.dsl.version=counterVersion"})
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class PostgresR2dbcRepositoryIntegrationTests extends AbstractR2dbcRepositoryIntegrationTests {

	@RegisterExtension public static final ExternalDatabase database = PostgresTestSupport.database();

	@Autowired WithJsonRepository withJsonRepository;

	@Autowired WithHStoreRepository hstoreRepositoryWith;

	@Autowired LegoJoinRepository legoJoinRepository;

	static class UserIdResolver implements AuthenticationIdentifierResolver {
		@Override
		public Mono<Object> resolve() {
			return Mono.just(UUID.randomUUID());
		}
	}

	@Configuration
	@EnableR2dbcRepositories(considerNestedRepositories = true,
			includeFilters = @Filter(
					classes = { PostgresLegoSetRepository.class, WithJsonRepository.class, WithHStoreRepository.class, LegoJoinRepository.class },
					type = FilterType.ASSIGNABLE_TYPE))
	static class IntegrationTestConfiguration extends AbstractR2dbcConfiguration {

		@Bean
		public AuthenticationIdentifierResolver userIdResolver() {
			return new UserIdResolver();
		}

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

		hstoreRepositoryWith.findAll().as(StepVerifier::create).consumeNextWith(actual -> assertThat(actual.hstoreValue).isNotNull().containsEntry("hello", "world")).verifyComplete();
	}

	@Setter
	@NoArgsConstructor
	@AllArgsConstructor
	static class WithJson implements Serializable {

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

	interface LegoJoinRepository extends R2dbcRepository<LegoJoin, Long> {

	}

	@Setter
	@NoArgsConstructor
	@AllArgsConstructor
//	@Table("with_hstore")
	static class WithHstore implements Serializable {

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
					assertThat(actual.getId()).isEqualTo(2);
					assertThat(actual.getName()).isEqualTo("FORSCHUNGSSCHIFF");
					assertThat(actual.getManual()).isEqualTo(13);
					assertThat(actual.getVersion()).isEqualTo(1);
					assertThat(actual.getGroup()).isNotNull();
					assertThat(actual.getManualReadOnly()).isEqualTo(22);
				})
				.verifyComplete();

		var legoSet = legoSets.get(1);

		// try to change readonly attr from properties manual and get old value
		legoSet.setNameEquality("equality");
		legoSet.setManualReadOnly(123);

		repository.save(legoSet) //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> assertThat(actual.getManual()).isEqualTo(13))
				.verifyComplete();

		// check versions
		repository.findById(2)
				.as(StepVerifier::create)
				.consumeNextWith(actual -> assertThat(actual.getVersion()).isEqualTo(2))
				.verifyComplete();

		// try to change equality attr name and get exception
		legoSet.setName("123");

		repository.save(legoSet) //
				.as(StepVerifier::create) //
				.expectError(IllegalStateException.class);

		// try to change equality attr from properties name and get exception
		legoSet.setNameEquality("123");

		repository.save(legoSet) //
				.as(StepVerifier::create) //
				.expectError(IllegalStateException.class);
	}

	@Test
	void shouldSimpleDsl() {
		List<LegoSet> legoSets = shouldInsertNewItems();
		assert legoSets.size() == 2;

		repository.findOne(Dsl.create().equals("manual", 13))
				.as(StepVerifier::create)
				.consumeNextWith(actual -> assertThat(actual.getName()).isEqualTo("FORSCHUNGSSCHIFF"))
				.verifyComplete();

		repository.findOne(Dsl.create().notEquals("manual", 13))
				.as(StepVerifier::create)
				.consumeNextWith(actual -> assertThat(actual.getName()).isEqualTo("SCHAUFELRADBAGGER"))
				.verifyComplete();

		repository.findOne(Dsl.create().notEquals("name", "SCHAUFELRADBAGGER"))
				.as(StepVerifier::create)
				.consumeNextWith(actual -> assertThat(actual.getName()).isEqualTo("FORSCHUNGSSCHIFF"))
				.verifyComplete();

		repository.findOne(Dsl.create().equals("name", "FORSCHUNGSSCHIFF"))
				.as(StepVerifier::create)
				.consumeNextWith(actual -> assertThat(actual.getManual()).isEqualTo(13))
				.verifyComplete();

		repository.findOne(Dsl.create().like("name", "chung")) // in PostgreSQL LIKE replaced by ILIKE
				.as(StepVerifier::create)
				.consumeNextWith(actual -> assertThat(actual.getManual()).isEqualTo(13))
				.verifyComplete();

		repository.findOne(Dsl.create().id(1L))
				.as(StepVerifier::create)
				.consumeNextWith(actual -> assertThat(actual.getName()).isEqualTo("SCHAUFELRADBAGGER"))
				.verifyComplete();

		repository.findOne(Dsl.create().in("id", 1L))
				.as(StepVerifier::create)
				.consumeNextWith(actual -> assertThat(actual.getName()).isEqualTo("SCHAUFELRADBAGGER"))
				.verifyComplete();

		repository.findAll(Dsl.create())
				.as(StepVerifier::create)
				.expectNextCount(2) //
				.verifyComplete();

		repository.findOne(Dsl.create().sorting("id", "desc"))
				.as(StepVerifier::create)
				.consumeNextWith(actual -> Assert.isTrue(actual.getId().longValue() == 2, "must 2"))
				.verifyComplete();

		repository.findOne(Dsl.create().sorting("id", "desc").pageable(1, 1))
				.as(StepVerifier::create)
				.consumeNextWith(actual -> Assert.isTrue(actual.getId().longValue() == 1, "must 1"))
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

		repository.findAll(Dsl.create().in("id", 1L).in("name", "SCHAUFELRADBAGGER").fields(" id", "name").limit(1))
				.as(StepVerifier::create)
				.expectNextCount(1) //
				.verifyComplete();

		repository.findAll(Dsl.create().top(1).order("group", Sort.Direction.DESC))
				.as(StepVerifier::create)
				.consumeNextWith(actual -> Assert.isTrue(actual.getId().longValue() == 2, "must 2"))
				.verifyComplete();

		repository.findAll(Dsl.create().distinct())
				.as(StepVerifier::create)
				.expectNextCount(2) //
				.verifyComplete();
	}

	@Test
	void shouldCompareDatesAndPagedResult() {
		var now = LocalDate.now();
		var nowTime = LocalDateTime.now();
		var zonedTime = ZonedDateTime.now();
		var offsetTime = OffsetDateTime.now();

		LegoSet legoSet1 = new LegoSet(null, "SCHAUFELRADBAGGER", 12);
		legoSet1.setData(now.minus(1, ChronoUnit.DAYS));
		legoSet1.setDataTime(nowTime.minus(1, ChronoUnit.DAYS));
		legoSet1.setZonedTime(zonedTime.minus(1, ChronoUnit.DAYS));
		legoSet1.setOffsetTime(offsetTime.minus(1, ChronoUnit.DAYS));

		LegoSet legoSet2 = new LegoSet(null, "FORSCHUNGSSCHIFF_2", 13);
		legoSet2.setData(now);
		legoSet2.setDataTime(nowTime);
		legoSet2.setZonedTime(zonedTime);
		legoSet2.setOffsetTime(offsetTime);

		LegoSet legoSet3 = new LegoSet(null, "FORSCHUNGSSCHIFF_3", 14);
		legoSet3.setData(now.plus(1, ChronoUnit.DAYS));
		legoSet3.setDataTime(nowTime.plus(1, ChronoUnit.DAYS));
		legoSet3.setZonedTime(zonedTime.plus(1, ChronoUnit.DAYS));
		legoSet3.setOffsetTime(offsetTime.plus(1, ChronoUnit.DAYS));

		R2dbcUtils.batchSave(List.of(legoSet1, legoSet2, legoSet3))
				.as(StepVerifier::create)
				.expectNextCount(3) //
				.verifyComplete();

		repository.findAll(Dsl.create().greaterThan("data", now)).collectList()
				.as(StepVerifier::create)
				.consumeNextWith(actual -> Assert.isTrue(actual.get(0).id == 3, "must equals"))
				.verifyComplete();
		repository.findAll(Dsl.create().lessThan("data", now)).collectList()
				.as(StepVerifier::create)
				.consumeNextWith(actual -> Assert.isTrue(actual.get(0).id == 1, "must equals"))
				.verifyComplete();
		repository.findAll(Dsl.create().lessThanOrEquals("data", now))
				.as(StepVerifier::create)
				.expectNextCount(2) //
				.verifyComplete();
		repository.findAll(Dsl.create().equals("data", now)).collectList()
				.as(StepVerifier::create)
				.consumeNextWith(actual -> Assert.isTrue(actual.get(0).id == 2, "must equals"))
				.verifyComplete();

		repository.findAll(Dsl.create().greaterThan("dataTime", nowTime)).collectList()
				.as(StepVerifier::create)
				.consumeNextWith(actual -> Assert.isTrue(actual.get(0).id == 3, "must equals"))
				.verifyComplete();
		repository.findAll(Dsl.create().lessThan("dataTime", nowTime)).collectList()
				.as(StepVerifier::create)
				.consumeNextWith(actual -> Assert.isTrue(actual.get(0).id == 1, "must equals"))
				.verifyComplete();
		repository.findAll(Dsl.create().greaterThanOrEquals("dataTime", nowTime))
				.as(StepVerifier::create)
				.expectNextCount(2) //
				.verifyComplete();
		repository.findAll(Dsl.create().equals("dataTime", nowTime)).collectList()
				.as(StepVerifier::create)
				.consumeNextWith(actual -> Assert.isTrue(actual.get(0).id == 2, "must equals"))
				.verifyComplete();

		repository.findAll(Dsl.create().greaterThan("zonedTime", zonedTime)).collectList()
				.as(StepVerifier::create)
				.consumeNextWith(actual -> Assert.isTrue(actual.get(0).id == 3, "must equals"))
				.verifyComplete();
		repository.findAll(Dsl.create().lessThan("zonedTime", zonedTime)).collectList()
				.as(StepVerifier::create)
				.consumeNextWith(actual -> Assert.isTrue(actual.get(0).id == 1, "must equals"))
				.verifyComplete();
		repository.findAll(Dsl.create().greaterThanOrEquals("zonedTime", zonedTime))
				.as(StepVerifier::create)
				.expectNextCount(2) //
				.verifyComplete();
		repository.findAll(Dsl.create().equals("zonedTime", zonedTime)).collectList()
				.as(StepVerifier::create)
				.consumeNextWith(actual -> Assert.isTrue(actual.get(0).id == 2, "must equals"))
				.verifyComplete();

		repository.findAll(Dsl.create().greaterThan("offsetTime", offsetTime)).collectList()
				.as(StepVerifier::create)
				.consumeNextWith(actual -> Assert.isTrue(actual.get(0).id == 3, "must equals"))
				.verifyComplete();
		repository.findAll(Dsl.create().lessThan("offsetTime", offsetTime)).collectList()
				.as(StepVerifier::create)
				.consumeNextWith(actual -> Assert.isTrue(actual.get(0).id == 1, "must equals"))
				.verifyComplete();
		repository.findAll(Dsl.create().greaterThanOrEquals("offsetTime", offsetTime))
				.as(StepVerifier::create)
				.expectNextCount(2) //
				.verifyComplete();
		repository.findAll(Dsl.create().equals("offsetTime", offsetTime)).collectList()
				.as(StepVerifier::create)
				.consumeNextWith(actual -> Assert.isTrue(actual.get(0).id == 2, "must equals"))
				.verifyComplete();

		var monsterDsl = Dsl.create()
				.notEquals("name", "FORSCHUNGSSCHIFF_2")
				.in("name", "FORSCHUNGSSCHIFF_3")
				.greaterThanOrEquals("dataTime", legoSet2.dataTime)
				.greaterThanOrEquals("zonedTime", legoSet2.zonedTime)
				.greaterThanOrEquals("offsetTime", legoSet2.offsetTime)
				.lessThanOrEquals("dataTime", legoSet3.dataTime)
				.lessThanOrEquals("zonedTime", legoSet3.zonedTime)
				.lessThanOrEquals("offsetTime", legoSet3.offsetTime)
				.in("manual", 14)
				.sorting("dataTime", "desc");

		Assert.isTrue(monsterDsl.getQuery().startsWith("name!=FORSCHUNGSSCHIFF_2,name^^FORSCHUNGSSCHIFF_3")
				&& monsterDsl.getQuery().endsWith("manual^^14"), "Dsl criteria must is ordered");

		repository.findAll(monsterDsl).collectList()
				.as(StepVerifier::create)
				.consumeNextWith(actual -> Assert.isTrue(actual.get(0).id == 3, "must equals"))
				.verifyComplete();

		repository.findAllPaged(Dsl.create().pageable(0, 3))
				.as(StepVerifier::create)
				.consumeNextWith(actual -> Assert.isTrue(
						actual.isLastPage()
								&& actual.getPage().getTotalPages() == 1
								&& actual.getPage().getOffset() == 0
								&& actual.getPage().getTotalElements() == 3,
						"must equals"))
				.verifyComplete();
	}

	@Test
	void shouldBeans() throws JsonProcessingException {
		Assert.notNull(Beans.getApplicationContext(), "Application context must not be null!");
		Assert.notNull(Beans.of(DatabaseClient.class), "DatabaseClient must not be null!");
		Assert.isTrue(Beans.of(ObjectMapper.class).readTree("{'name':'value'}").toString().equals("{\"name\":\"value\"}"), "is not true");
	}

	static class CacheTest extends AbstractRepositoryCache<String, UUID> {
		public CacheTest() {
			super(null, Beans.getApplicationContext());
			var id = UUID.fromString("00000000-0000-0000-0000-000000000000");
			put(String.class, Dsl.create().id(id), "test1");
			var hash = getHash(String.class, Dsl.create().id(id));
			var contains = contains(String.class, Dsl.create().id(id));
			var value = Objects.requireNonNull(get(String.class, Dsl.create().id(id)));
			Assert.isTrue(hash.equals("null->>'String->>'id==00000000-0000-0000-0000-000000000000<->-1<->-1<->"), "must equals");
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

		R2dbcUtils.batchSave(List.of(legoSet1, legoSet2))
				.as(StepVerifier::create)
				.expectNextCount(2) //
				.verifyComplete();

		repository.count(Dsl.create())
				.as(StepVerifier::create)
				.consumeNextWith(count -> Assert.isTrue(count == 2, "must equals"))
				.verifyComplete();

		repository.count(Dsl.create().equals("manual", 12).in("name", "SCHAUFELRADBAGGER", "SCHAUFELRADBAGGER"))
				.as(StepVerifier::create)
				.consumeNextWith(count -> Assert.isTrue(count == 1, "must equals"))
				.verifyComplete();

		repository.count(Dsl.create().equals("manual", 12))
				.as(StepVerifier::create)
				.consumeNextWith(count -> Assert.isTrue(count == 1, "must equals"))
				.verifyComplete();
	}

	@Test
	void shouldExpressionWork() {
		LegoSet legoSet1 = new LegoSet(null, "SCHAUFELRADBAGGER", 12);
		legoSet1.having = Objects.requireNonNull(ExpressionParserCache.INSTANCE.parseExpression("a==5"));

		repository.save(legoSet1)
				.as(StepVerifier::create)
				.consumeNextWith(l -> Assert.isTrue(l.id == 1, "must equals"))
				.verifyComplete();

		repository.findById(1)
				.as(StepVerifier::create)
				.consumeNextWith(actual -> {
					Assert.isTrue(actual.getHaving().getExpressionString().equals("a==5"), "must equals");
					Assert.isTrue(actual.getName().equals("SCHAUFELRADBAGGER"), "must equals");
					Assert.isTrue(actual.getManual() == 12, "must equals");
				})
				.verifyComplete();
	}

	@Test
	void shouldCacheManage() {
		LegoSet legoSet1 = new LegoSet(null, "SCHAUFELRADBAGGER", 12);

		repository.save(legoSet1)
				.as(StepVerifier::create)
				.expectNextCount(1)
				.verifyComplete();

		repository.findOne(Dsl.create().id(1))
				.as(StepVerifier::create)
				.consumeNextWith(actual -> {
					Assert.isTrue(actual.getName().equals("SCHAUFELRADBAGGER"), "must true");
					Assert.isTrue(Objects.requireNonNull(repository.cache().get(1)).getName().equals("SCHAUFELRADBAGGER"), "must equals");
				})
				.verifyComplete();

		Assert.isTrue(Objects.requireNonNull(repository.cache().get(1)).getName().equals("SCHAUFELRADBAGGER"), "must equals");

		Assert.isTrue(repository.cache().evict(1).cache().get(1) == null, "must equals");

		legoSet1.setId(1);
		Assert.isTrue(Objects.requireNonNull(repository.cache().put(legoSet1).cache().get(1)).getName().equals("SCHAUFELRADBAGGER"), "must equals");

		repository.cache().evictAll();
		Assert.isTrue(repository.cache().get(1) == null, "must equals");
	}

	@Test
	void shouldCacheMonoFromFlux() {
		repository.deleteAll().block();

		LegoSet legoSet1 = new LegoSet(null, "SCHAUFELRADBAGGER", 12);
		repository.save(legoSet1)
				.as(StepVerifier::create)
				.expectNextCount(1)
				.verifyComplete();

		Assert.isTrue(Objects.requireNonNull(repository.cache().get(1)).getName().equals("SCHAUFELRADBAGGER"), "must equals");

		repository.cache().evictAll().findAll(Dsl.create().id(1))
				.as(StepVerifier::create)
				.consumeNextWith(actual -> {
					Assert.isTrue(actual.getName().equals("SCHAUFELRADBAGGER"), "must true");
					Assert.isTrue(Objects.requireNonNull(repository.cache().get(1)).getName().equals("SCHAUFELRADBAGGER"), "must equals");
				})
				.verifyComplete();
	}

	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	@Accessors(chain = true)
	static class LegoJoin implements Serializable {
		Long id;
		Integer version;
		String name;
		JsonNode data;
		UUID createdBy;
		@CreatedBy
		UUID createdUser;
		UUID updatedBy;
		@UpdatedBy
		UUID updatedUser;
	}

	void createLegoJoin() {
		JdbcTemplate template = new JdbcTemplate(createDataSource());
		template.execute("DROP TABLE IF EXISTS lego_join");
		template.execute("CREATE TABLE lego_join (\n" //
				+ "    id          SERIAL PRIMARY KEY,\n" //
				+ "    version     integer NULL,\n" //
				+ "    data        jsonb NULL,\n" //
				+ "    name        text NULL,\n" //
				+ "    tsv         tsvector NULL,\n" //
				+ "    created_by   uuid NULL,\n" //
				+ "    created_user uuid NULL,\n" //
				+ "    updated_by   uuid NULL,\n" //
				+ "    updated_user uuid NULL\n" //
				+ ");");
	}

	@Test
	void shouldJoinTableAndGroupingByOr() {
		shouldInsertNewItems();
		createLegoJoin();
		LegoJoin legoJoin1 = new LegoJoin().setName("join1").setData(JsonUtils.objectNode().put("key", "value1"));
		legoJoinRepository.save(legoJoin1).as(StepVerifier::create).expectNextCount(1).verifyComplete();
		LegoJoin legoJoin2 = new LegoJoin().setName("join2").setData(JsonUtils.objectNode().put("key", "value2"));
		legoJoinRepository.save(legoJoin2).as(StepVerifier::create).expectNextCount(1).verifyComplete();
		legoJoinRepository.save(legoJoin2).as(StepVerifier::create).expectNextCount(1).verifyComplete();
		Assert.isTrue(legoJoin2.getVersion() == 2, "must 2");

		repository.findAll(Dsl.create().equals("legoJoin.name", "join1").limit(1))
				.as(StepVerifier::create)
				.consumeNextWith(actual -> Assert.isTrue(actual.getId().longValue() == 1, "must 1"))
				.verifyComplete();

		repository.findAll(Dsl.create().equals("legoJoin.data.key", "value2").limit(1))
				.as(StepVerifier::create)
				.consumeNextWith(actual -> Assert.isTrue(actual.getId().longValue() == 2, "must 2"))
				.verifyComplete();

		repository.findAll(Dsl.create().equals("legoJoin.name", "join2").equals("legoJoin.version", 2))
				.as(StepVerifier::create)
				.consumeNextWith(actual -> Assert.isTrue(actual.getId().longValue() == 2, "must 2"))
				.verifyComplete();

		repository.findAll(Dsl.create("version==2,name==abc,()legoJoin.name==bbc,manual==10,()legoJoin.name==join2,legoJoin.version==2"))
				.as(StepVerifier::create)
				.consumeNextWith(actual -> Assert.isTrue(actual.getId().longValue() == 2, "must 2"))
				.verifyComplete();

		repository.findAll(Dsl.create().equals("version", 2).equals("name", "abc").or()
						.equals("legoJoin.name", "bbc").equals("manual", 10).or()
						.equals("legoJoin.name", "join2").equals("legoJoin.version", 2))
				.as(StepVerifier::create)
				.consumeNextWith(actual -> Assert.isTrue(actual.getId().longValue() == 2, "must 2"))
				.verifyComplete();
	}

	@Test
	void shouldFullTextSearchAndCountIt() {
		shouldInsertNewItems();
		JdbcTemplate template = new JdbcTemplate(createDataSource());
		createLegoJoin();
		template.execute("INSERT INTO lego_join (version,name,tsv,data) " +
				"values(1,'join1',to_tsvector('pg_catalog.english','The best from the west'),'{\"key\":\"value1\"}'::jsonb);");
		template.execute("INSERT INTO lego_join (version,name,tsv,data) " +
				"values(1,'join2',to_tsvector('pg_catalog.english','The best from the wild west'),'{\"key\":\"value2\"}'::jsonb);");

		legoJoinRepository.findAll(Dsl.create().fts("Wild").isNotNull("data.key").equals("data.key", "value2"))
				.as(StepVerifier::create)
				.consumeNextWith(actual -> Assert.isTrue(actual.getId() == 2, "must 2"))
				.verifyComplete();

		legoJoinRepository.count(Dsl.create().fts("West").isNotNull("data.key"))
				.as(StepVerifier::create)
				.consumeNextWith(actual -> Assert.isTrue(actual == 2, "must 2"))
				.verifyComplete();

		legoJoinRepository.findAllPaged(Dsl.create().fts("West").isNotNull("data.key"))
				.as(StepVerifier::create)
				.consumeNextWith(actual -> Assert.isTrue(actual.getCount() == 2 && actual.getContent().size() == 2, "must 2"))
				.verifyComplete();
	}

	@Test
	void shouldSecurityUserIdResolver() {
		createLegoJoin();

		var one = new LegoJoin().setName("one");
		legoJoinRepository.save(one)
				.as(StepVerifier::create)
				.consumeNextWith(actual -> {
					Assert.isTrue(actual.getName().equals("one"), "must true");
					Assert.isTrue(actual.getCreatedBy() != null, "must true");
					Assert.isTrue(actual.getCreatedUser() != null, "must true");
					Assert.isTrue(actual.getUpdatedBy() == null, "must true");
					Assert.isTrue(actual.getUpdatedUser() == null, "must true");
					Assert.isTrue(actual.getVersion() == 1, "must true");
				})
				.verifyComplete();

		legoJoinRepository.save(one.setName("two"))
				.as(StepVerifier::create)
				.consumeNextWith(actual -> {
					Assert.isTrue(actual.getName().equals("two"), "must true");
					Assert.isTrue(actual.getCreatedBy() != null, "must true");
					Assert.isTrue(actual.getCreatedUser() != null, "must true");
					Assert.isTrue(actual.getUpdatedBy() != null, "must true");
					Assert.isTrue(actual.getUpdatedUser() != null, "must true");
					Assert.isTrue(actual.getVersion() == 2, "must true");
				})
				.verifyComplete();
	}
}
