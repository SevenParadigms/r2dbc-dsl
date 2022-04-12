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

import io.r2dbc.spi.ConnectionFactory;
import lombok.*;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.sevenparadigms.cache.hazelcast.AnySerializable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.PersistenceConstructor;
import org.springframework.data.annotation.Version;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.r2dbc.connectionfactory.R2dbcTransactionManager;
import org.springframework.data.r2dbc.repository.query.Equality;
import org.springframework.data.r2dbc.repository.query.ReadOnly;
import org.springframework.data.r2dbc.testing.PostgresTestSupport;
import org.springframework.data.r2dbc.testing.R2dbcIntegrationTestSupport;
import org.springframework.data.repository.NoRepositoryBean;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.expression.Expression;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.lang.Nullable;
import org.springframework.transaction.reactive.TransactionalOperator;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import javax.sql.DataSource;
import java.io.Serializable;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Abstract base class for integration tests for {@link LegoSetRepository} using {@link RepositoryFactorySupport}.
 *
 * @author Mark Paluch
 */
public abstract class AbstractR2dbcRepositoryIntegrationTests extends R2dbcIntegrationTestSupport {

	@Autowired protected LegoSetRepository repository;
	@Autowired protected ConnectionFactory connectionFactory;
	protected JdbcTemplate jdbc;

	@BeforeEach
	void before() {

		this.jdbc = createJdbcTemplate(createDataSource());

		try {
			this.jdbc.execute(PostgresTestSupport.DROP_TABLE_LEGOSET);
		} catch (DataAccessException e) {}

		this.jdbc.execute(getCreateTableStatement());
	}

	/**
	 * Creates a {@link DataSource} to be used in this test.
	 *
	 * @return the {@link DataSource} to be used in this test.
	 */
	protected abstract DataSource createDataSource();

	/**
	 * Creates a {@link ConnectionFactory} to be used in this test.
	 *
	 * @return the {@link ConnectionFactory} to be used in this test.
	 */
	protected abstract ConnectionFactory createConnectionFactory();

	/**
	 * Returns the the CREATE TABLE statement for table {@code lego_set} with the following three columns:
	 * <ul>
	 * <li>id integer (primary key), not null, auto-increment</li>
	 * <li>name varchar(255), nullable</li>
	 * <li>manual integer, nullable</li>
	 * </ul>
	 *
	 * @return the CREATE TABLE statement for table {@code lego_set} with three columns.
	 */
	protected abstract String getCreateTableStatement();

	protected abstract Class<? extends LegoSetRepository> getRepositoryInterfaceType();

	@Test
	List<LegoSet> shouldInsertNewItems() {

		LegoSet legoSet1 = new LegoSet(null, "SCHAUFELRADBAGGER", 12);
		LegoSet legoSet2 = new LegoSet(null, "FORSCHUNGSSCHIFF", 13);
		legoSet2.setManualReadOnly(22);
		legoSet2.setNameEquality("equality");

		repository.saveAll(Arrays.asList(legoSet1, legoSet2)) //
				.as(StepVerifier::create) //
				.expectNextCount(2) //
				.verifyComplete();

		legoSet1.setId(1);
		legoSet2.setId(2);

		return List.of(legoSet1, legoSet2);
	}

	@Test
	void shouldFindItemsByManual() {

		shouldInsertNewItems();

		repository.findByManual(13) //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {
					assertThat(actual.getName()).isEqualTo("FORSCHUNGSSCHIFF");
				}) //
				.verifyComplete();
	}

	@Test
	void shouldFindItemsByNameContains() {

		shouldInsertNewItems();

//		repository.findByNameContains("F") //
//				.map(LegoSet::getName) //
//				.collectList() //
//				.as(StepVerifier::create) //
//				.consumeNextWith(actual -> {
//					assertThat(actual).contains("SCHAUFELRADBAGGER", "FORSCHUNGSSCHIFF");
//				}).verifyComplete();
	}

	@Test // gh-475, gh-607
	void shouldFindApplyingInterfaceProjection() {

		shouldInsertNewItems();

		repository.findAsProjection() //
				.map(Named::getName) //
				.collectList() //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {
					assertThat(actual).contains("SCHAUFELRADBAGGER", "FORSCHUNGSSCHIFF");
				}).verifyComplete();

		repository.findBy(WithName.class) //
				.map(WithName::getName) //
				.collectList() //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {
					assertThat(actual).contains("SCHAUFELRADBAGGER", "FORSCHUNGSSCHIFF");
				}).verifyComplete();
	}

	@Test // gh-475
	void shouldByStringQueryApplyingDtoProjection() {

		shouldInsertNewItems();

		repository.findAsDtoProjection() //
				.map(LegoDto::getName) //
				.collectList() //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {
					assertThat(actual).contains("SCHAUFELRADBAGGER", "FORSCHUNGSSCHIFF");
				}).verifyComplete();
	}

	@Test // gh-344
	void shouldFindApplyingDistinctProjection() {

		LegoSet legoSet1 = new LegoSet(null, "SCHAUFELRADBAGGER", 12);
		LegoSet legoSet2 = new LegoSet(null, "SCHAUFELRADBAGGER", 13);

		repository.saveAll(Arrays.asList(legoSet1, legoSet2)) //
				.as(StepVerifier::create) //
				.expectNextCount(2) //
				.verifyComplete();

		repository.findDistinctBy() //
				.map(Named::getName) //
				.collectList() //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {
					assertThat(actual).hasSize(1).contains("SCHAUFELRADBAGGER");
				}).verifyComplete();
	}

	@Test // gh-41
	void shouldFindApplyingSimpleTypeProjection() {

		shouldInsertNewItems();

		repository.findAllIds() //
				.collectList() //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {
					assertThat(actual).hasSize(2).allMatch(Integer.class::isInstance);
				}).verifyComplete();
	}

	@Test
	void shouldDeleteUsingQueryMethod() {

		shouldInsertNewItems();

		repository.deleteAllByManual(12) //
				.then().as(StepVerifier::create) //
				.verifyComplete();

		Map<String, Object> count = jdbc.queryForMap("SELECT count(*) AS count from lego_set");
		assertThat(getCount(count)).satisfies(numberOf(1));
	}

//	@Test // gh-335
	void shouldFindByPageable() {

		Flux<LegoSet> sets = Flux.fromStream(IntStream.range(0, 100).mapToObj(value -> {
			return new LegoSet(null, "Set " + value, value);
		}));

		repository.saveAll(sets) //
				.as(StepVerifier::create) //
				.expectNextCount(100) //
				.verifyComplete();

		repository.findAllByOrderByManual(PageRequest.of(0, 10)) //
				.collectList() //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {

					assertThat(actual).hasSize(10).extracting(LegoSet::getManual).containsSequence(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
				}).verifyComplete();

		repository.findAllByOrderByManual(PageRequest.of(19, 5)) //
				.collectList() //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {

					assertThat(actual).hasSize(5).extracting(LegoSet::getManual).containsSequence(95, 96, 97, 98, 99);
				}).verifyComplete();
	}

//	@Test // gh-335
	void shouldFindTop10() {

		Flux<LegoSet> sets = Flux.fromStream(IntStream.range(0, 100).mapToObj(value -> {
			return new LegoSet(null, "Set " + value, value);
		}));

		repository.saveAll(sets) //
				.as(StepVerifier::create) //
				.expectNextCount(100) //
				.verifyComplete();

		repository.findFirst10By() //
				.as(StepVerifier::create) //
				.expectNextCount(10) //
				.verifyComplete();
	}

	@Test // gh-341
	void shouldDeleteAll() {

		shouldInsertNewItems();

		repository.deleteAllBy() //
				.as(StepVerifier::create) //
				.verifyComplete();

		repository.findAll() //
				.as(StepVerifier::create) //
				.verifyComplete();
	}

	@Test
	public void shouldInsertItemsTransactional() {

		R2dbcTransactionManager r2dbcTransactionManager = new R2dbcTransactionManager(connectionFactory);
		TransactionalOperator rxtx = TransactionalOperator.create(r2dbcTransactionManager);

		LegoSet legoSet1 = new LegoSet(null, "SCHAUFELRADBAGGER", 12);
		LegoSet legoSet2 = new LegoSet(null, "FORSCHUNGSSCHIFF", 13);

		Mono<Map<String, Object>> transactional = repository.save(legoSet1) //
				.map(it -> jdbc.queryForMap("SELECT count(*) AS count from lego_set")).as(rxtx::transactional);

		Mono<Map<String, Object>> nonTransactional = repository.save(legoSet2) //
				.map(it -> jdbc.queryForMap("SELECT count(*) AS count from lego_set"));

		transactional.as(StepVerifier::create).assertNext(actual -> assertThat(getCount(actual)).satisfies(numberOf(0)))
				.verifyComplete();
		nonTransactional.as(StepVerifier::create).assertNext(actual -> assertThat(getCount(actual)).satisfies(numberOf(2)))
				.verifyComplete();

		Map<String, Object> map = jdbc.queryForMap("SELECT count(*) AS count from lego_set");
		assertThat(getCount(map)).satisfies(numberOf(2));
	}

	@Test // gh-363
	void derivedQueryWithCountProjection() {

		shouldInsertNewItems();

		repository.countByNameContains("SCH") //
				.as(StepVerifier::create) //
				.assertNext(i -> assertThat(i).isEqualTo(2)) //
				.verifyComplete();
	}

	@Test // gh-363
	void derivedQueryWithCount() {

		shouldInsertNewItems();

		repository.countByNameContains("SCH") //
				.as(StepVerifier::create) //
				.assertNext(i -> assertThat(i).isEqualTo(2)) //
				.verifyComplete();
	}

//	@Test // gh-468
	void derivedQueryWithExists() {

		shouldInsertNewItems();

		repository.existsByName("ABS") //
				.as(StepVerifier::create) //
				.expectNext(Boolean.FALSE) //
				.verifyComplete();

		repository.existsByName("SCHAUFELRADBAGGER") //
				.as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();
	}

	@Test // gh-421
	void shouldDeleteAllAndReturnCount() {

		shouldInsertNewItems();

		repository.deleteAllAndReturnCount() //
				.as(StepVerifier::create) //
				.expectNext(2) //
				.verifyComplete();

		repository.findAll() //
				.as(StepVerifier::create) //
				.verifyComplete();
	}

	private static Object getCount(Map<String, Object> map) {
		return map.getOrDefault("count", map.get("COUNT"));
	}

	private Condition<? super Object> numberOf(int expected) {
		return new Condition<>(it -> {
			return it instanceof Number && ((Number) it).intValue() == expected;
		}, "Number  %d", expected);
	}

	@NoRepositoryBean
	interface LegoSetRepository extends R2dbcRepository<LegoSet, Integer> {

		Flux<LegoSet> findByNameContains(String name);

		Flux<LegoSet> findFirst10By();

		Flux<LegoSet> findAllByOrderByManual(Pageable pageable);

		Flux<Named> findAsProjection();

		<T> Flux<T> findBy(Class<T> theClass);

		@Query("SELECT name from lego_set")
		Flux<LegoDto> findAsDtoProjection();

		Flux<Named> findDistinctBy();

		Mono<LegoSet> findByManual(int manual);

		Flux<Integer> findAllIds();

		Mono<Void> deleteAllBy();

		@Modifying
		@Query("DELETE from lego_set where manual = :manual")
		Mono<Void> deleteAllByManual(int manual);

		@Modifying
		@Query("DELETE from lego_set")
		Mono<Integer> deleteAllAndReturnCount();

		Mono<Integer> countByNameContains(String namePart);

		Mono<Boolean> existsByName(String name);
	}

	public interface Buildable {

		String getName();
	}

	@Getter
	@Setter
//	@Table("lego_set")
	@NoArgsConstructor
	public static class LegoSet extends Lego implements Buildable, AnySerializable {
		@Equality
		String name;
		@ReadOnly
		Integer manual;
		@Version
		Integer version;
		@CreatedDate
		LocalDateTime group;	// reserved word

		Expression having;

		String nameEquality;
		Integer manualReadOnly;
		Long counterVersion;

		LocalDate data;
		LocalDateTime dataTime;
		ZonedDateTime zonedTime;
		OffsetDateTime offsetTime;

		@PersistenceConstructor
		LegoSet(@Nullable Integer id, String name, Integer manual) {
			super(id);
			this.name = name;
			this.manual = manual;
		}
	}

	@AllArgsConstructor
	@NoArgsConstructor
	@Getter
	@Setter
	static class Lego {
		/*@Id */Integer id;
	}

	@Value
	static class LegoDto implements Serializable {
		String name;
		String unknown;

		public LegoDto(String name, String unknown) {
			this.name = name;
			this.unknown = unknown;
		}
	}

	interface Named {
		String getName();
	}

	interface WithName {
		String getName();
	}
}
