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
package org.springframework.data.r2dbc.repository.support;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.domain.ExampleMatcher.*;
import static org.springframework.data.domain.ExampleMatcher.GenericPropertyMatchers.*;
import static org.springframework.data.domain.ExampleMatcher.StringMatcher.*;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.context.ApplicationContext;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.domain.Example;
import org.springframework.data.domain.Sort;
import org.springframework.data.r2dbc.convert.MappingR2dbcConverter;
import org.springframework.data.r2dbc.core.ReactiveDataAccessStrategy;
import org.springframework.data.r2dbc.testing.R2dbcIntegrationTestSupport;
import org.springframework.data.relational.core.mapping.RelationalMappingContext;
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity;
import org.springframework.data.relational.core.mapping.Table;
import org.springframework.data.relational.repository.query.RelationalEntityInformation;
import org.springframework.data.relational.repository.support.MappingRelationalEntityInformation;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.r2dbc.core.DatabaseClient;

/**
 * Abstract integration tests for {@link SimpleR2dbcRepository} to be ran against various databases.
 *
 * @author Mark Paluch
 * @author Bogdan Ilchyshyn
 * @author Stephen Cohen
 * @author Jens Schauder
 * @author Greg Turnquist
 */
public abstract class AbstractSimpleR2dbcRepositoryIntegrationTests extends R2dbcIntegrationTestSupport {

	@Autowired private DatabaseClient databaseClient;

	@Autowired private RelationalMappingContext mappingContext;

	@Autowired private ReactiveDataAccessStrategy strategy;

	@Autowired private ApplicationContext applicationContext;

	SimpleR2dbcRepository<LegoSet, Integer> repository;
	SimpleR2dbcRepository<LegoSetWithNonScalarId, Integer> repositoryWithNonScalarId;
	JdbcTemplate jdbc;

	@BeforeEach
	void before() {

		MappingR2dbcConverter converter = new MappingR2dbcConverter(mappingContext);

		RelationalEntityInformation<LegoSet, Integer> entityInformation = new MappingRelationalEntityInformation<>(
				(RelationalPersistentEntity<LegoSet>) mappingContext.getRequiredPersistentEntity(LegoSet.class));

		this.repository = new SimpleR2dbcRepository<>(entityInformation, databaseClient, converter, strategy, applicationContext);

		RelationalEntityInformation<LegoSetWithNonScalarId, Integer> boxedEntityInformation = new MappingRelationalEntityInformation<>(
				(RelationalPersistentEntity<LegoSetWithNonScalarId>) mappingContext
						.getRequiredPersistentEntity(LegoSetWithNonScalarId.class));

		this.repositoryWithNonScalarId = new SimpleR2dbcRepository<>(boxedEntityInformation, databaseClient, converter,
				strategy, applicationContext);

		this.jdbc = createJdbcTemplate(createDataSource());
		try {
			this.jdbc.execute("DROP TABLE lego_set");
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
	 * Returns the the CREATE TABLE statement for table {@code legoset} with the following three columns:
	 * <ul>
	 * <li>id integer (primary key), not null, auto-increment</li>
	 * <li>name varchar(255), nullable</li>
	 * <li>manual integer, nullable</li>
	 * </ul>
	 *
	 * @return the CREATE TABLE statement for table {@code legoset} with three columns.
	 */
	protected abstract String getCreateTableStatement();

	@Test // gh-444
	void shouldSaveNewObject() {

		repository.save(new LegoSet(0, "SCHAUFELRADBAGGER", 12)) //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {

				}).verifyComplete();

		repository.save(new LegoSet(0, "SCHAUFELRADBAGGER", 12)) //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> assertThat(actual.getId()).isGreaterThan(0)) //
				.verifyComplete();
	}

	@Test // gh-93
	void shouldSaveNewObjectAndSetVersionIfWrapperVersionPropertyExists() {

		LegoSetVersionable legoSet = new LegoSetVersionable(0, "SCHAUFELRADBAGGER", 12, null);

		repository.save(legoSet) //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> assertThat(actual.getVersion()).isEqualTo(0)) //
				.verifyComplete();

		Map<String, Object> map = jdbc.queryForMap("SELECT * from lego_set");
		assertThat(map) //
				.containsEntry("name", "SCHAUFELRADBAGGER") //
				.containsEntry("manual", 12) //
				.containsEntry("version", 0) //
				.containsKey("id");
	}

	@Test // gh-93
	void shouldSaveNewObjectAndSetVersionIfPrimitiveVersionPropertyExists() {

		LegoSetPrimitiveVersionable legoSet = new LegoSetPrimitiveVersionable(0, "SCHAUFELRADBAGGER", 12, 0);

		repository.save(legoSet) //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> assertThat(actual.getVersion()).isEqualTo(1)) //
				.verifyComplete();

		Map<String, Object> map = jdbc.queryForMap("SELECT * from lego_set");
		assertThat(map) //
				.containsEntry("name", "SCHAUFELRADBAGGER") //
				.containsEntry("manual", 12) //
				.containsEntry("version", 1) //
				.containsKey("id");
	}

	@Test
	void shouldUpdateObject() {

		jdbc.execute("INSERT INTO lego_set (name, manual) VALUES('SCHAUFELRADBAGGER', 12)");
		Integer id = jdbc.queryForObject("SELECT id from lego_set", Integer.class);

		LegoSet legoSet = new LegoSet(id, "SCHAUFELRADBAGGER", 12);
		legoSet.setManual(14);

		repository.save(legoSet) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		Map<String, Object> map = jdbc.queryForMap("SELECT * from lego_set");
		assertThat(map) //
				.containsEntry("name", "SCHAUFELRADBAGGER") //
				.containsEntry("manual", 14) //
				.containsKey("id");
	}

	@Test // gh-93
	void shouldUpdateVersionableObjectAndIncreaseVersion() {

		jdbc.execute("INSERT INTO lego_set (name, manual, version) VALUES('SCHAUFELRADBAGGER', 12, 42)");
		Integer id = jdbc.queryForObject("SELECT id from lego_set", Integer.class);

		LegoSetVersionable legoSet = new LegoSetVersionable(id, "SCHAUFELRADBAGGER", 12, 42);
		legoSet.setManual(14);

		repository.save(legoSet) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();

		assertThat(legoSet.getVersion()).isEqualTo(43);

		Map<String, Object> map = jdbc.queryForMap("SELECT * from lego_set");
		assertThat(map) //
				.containsEntry("name", "SCHAUFELRADBAGGER") //
				.containsEntry("manual", 14) //
				.containsEntry("version", 43) //
				.containsKey("id");
	}

	@Test // gh-93
	void shouldFailWithOptimistickLockingWhenVersionDoesNotMatchOnUpdate() {

		jdbc.execute("INSERT INTO lego_set (name, manual, version) VALUES('SCHAUFELRADBAGGER', 12, 42)");
		Integer id = jdbc.queryForObject("SELECT id from lego_set", Integer.class);

		LegoSetVersionable legoSet = new LegoSetVersionable(id, "SCHAUFELRADBAGGER", 12, 0);

		repository.save(legoSet) //
				.as(StepVerifier::create) //
				.expectError(OptimisticLockingFailureException.class) //
				.verify();
	}

	@Test
	void shouldSaveObjectsUsingIterable() {

		LegoSet legoSet1 = new LegoSet(0, "SCHAUFELRADBAGGER", 12);
		LegoSet legoSet2 = new LegoSet(0, "FORSCHUNGSSCHIFF", 13);
		LegoSet legoSet3 = new LegoSet(0, "RALLYEAUTO", 14);
		LegoSet legoSet4 = new LegoSet(0, "VOLTRON", 15);

		repository.saveAll(Arrays.asList(legoSet1, legoSet2, legoSet3, legoSet4)) //
				.map(LegoSet::getManual) //
				.as(StepVerifier::create) //
				.expectNext(12) //
				.expectNext(13) //
				.expectNext(14) //
				.expectNext(15) //
				.verifyComplete();

		Integer count = jdbc.queryForObject("SELECT COUNT(*) from lego_set", Integer.class);
		assertThat(count).isEqualTo(4);
	}

	@Test
	void shouldSaveObjectsUsingPublisher() {

		LegoSet legoSet1 = new LegoSet(0, "SCHAUFELRADBAGGER", 12);
		LegoSet legoSet2 = new LegoSet(0, "FORSCHUNGSSCHIFF", 13);

		repository.saveAll(Flux.just(legoSet1, legoSet2)) //
				.as(StepVerifier::create) //
				.expectNextCount(2) //
				.verifyComplete();

		Integer count = jdbc.queryForObject("SELECT COUNT(*) from lego_set", Integer.class);
		assertThat(count).isEqualTo(2);
	}

	@Test
	void shouldFindById() {

		jdbc.execute("INSERT INTO lego_set (name, manual) VALUES('SCHAUFELRADBAGGER', 12)");
		Integer id = jdbc.queryForObject("SELECT id from lego_set", Integer.class);

		repository.findById(id) //
				.as(StepVerifier::create) //
				.assertNext(actual -> {

					assertThat(actual.getId()).isEqualTo(id);
					assertThat(actual.getName()).isEqualTo("SCHAUFELRADBAGGER");
					assertThat(actual.getManual()).isEqualTo(12);
				}).verifyComplete();
	}

	@Test
	void shouldExistsById() {

		jdbc.execute("INSERT INTO lego_set (name, manual) VALUES('SCHAUFELRADBAGGER', 12)");
		Integer id = jdbc.queryForObject("SELECT id from lego_set", Integer.class);

		repository.existsById(id) //
				.as(StepVerifier::create) //
				.expectNext(true)//
				.verifyComplete();

		repository.existsById(42) //
				.as(StepVerifier::create) //
				.expectNext(false)//
				.verifyComplete();
	}

	@Test
	void shouldExistsByIdPublisher() {

		jdbc.execute("INSERT INTO lego_set (name, manual) VALUES('SCHAUFELRADBAGGER', 12)");
		Integer id = jdbc.queryForObject("SELECT id from lego_set", Integer.class);

		repository.existsById(Mono.just(id)) //
				.as(StepVerifier::create) //
				.expectNext(true)//
				.verifyComplete();

		repository.existsById(Mono.just(42)) //
				.as(StepVerifier::create) //
				.expectNext(false)//
				.verifyComplete();
	}

	@Test
	void shouldFindByAll() {

		jdbc.execute("INSERT INTO lego_set (name, manual) VALUES('SCHAUFELRADBAGGER', 12)");
		jdbc.execute("INSERT INTO lego_set (name, manual) VALUES('FORSCHUNGSSCHIFF', 13)");

		repository.findAll() //
				.map(LegoSet::getName) //
				.collectList() //
				.as(StepVerifier::create) //
				.assertNext(actual -> assertThat(actual).containsExactly("SCHAUFELRADBAGGER", "FORSCHUNGSSCHIFF"))
				.verifyComplete();
	}

	@Test // gh-407
	void shouldFindAllWithSort() {

		jdbc.execute("INSERT INTO lego_set (name, manual) VALUES('FORSCHUNGSSCHIFF', 13)");
		jdbc.execute("INSERT INTO lego_set (name, manual) VALUES('SCHAUFELRADBAGGER', 12)");
		jdbc.execute("INSERT INTO lego_set (name, manual) VALUES('VOLTRON', 15)");
		jdbc.execute("INSERT INTO lego_set (name, manual) VALUES('RALLYEAUTO', 14)");

		repository.findAll(Sort.by("manual").ascending()) //
				.map(LegoSet::getName) //
				.collectList() //
				.as(StepVerifier::create) //
				.assertNext(actual -> assertThat(actual).containsExactly( //
						"SCHAUFELRADBAGGER", //
						"FORSCHUNGSSCHIFF", //
						"RALLYEAUTO", //
						"VOLTRON"))
				.verifyComplete();
	}

	@Test
	void shouldFindAllByIdUsingIterable() {

		jdbc.execute("INSERT INTO lego_set (name, manual) VALUES('SCHAUFELRADBAGGER', 12)");
		jdbc.execute("INSERT INTO lego_set (name, manual) VALUES('FORSCHUNGSSCHIFF', 13)");

		List<Integer> ids = jdbc.queryForList("SELECT id from lego_set", Integer.class);

		repository.findAllById(ids) //
				.map(LegoSet::getName) //
				.collectList() //
				.as(StepVerifier::create) //
				.assertNext(actual -> {

					assertThat(actual).hasSize(2).contains("SCHAUFELRADBAGGER", "FORSCHUNGSSCHIFF");
				}).verifyComplete();
	}

	@Test
	void shouldFindAllByIdUsingPublisher() {

		jdbc.execute("INSERT INTO lego_set (name, manual) VALUES('SCHAUFELRADBAGGER', 12)");
		jdbc.execute("INSERT INTO lego_set (name, manual) VALUES('FORSCHUNGSSCHIFF', 13)");

		List<Integer> ids = jdbc.queryForList("SELECT id from lego_set", Integer.class);

		repository.findAllById(Flux.fromIterable(ids)) //
				.map(LegoSet::getName) //
				.collectList() //
				.as(StepVerifier::create) //
				.assertNext(actual -> {

					assertThat(actual).hasSize(2).contains("SCHAUFELRADBAGGER", "FORSCHUNGSSCHIFF");
				}).verifyComplete();
	}

	@Test
	void shouldCount() {

		repository.count() //
				.as(StepVerifier::create) //
				.expectNext(0L) //
				.verifyComplete();

		jdbc.execute("INSERT INTO lego_set (name, manual) VALUES('SCHAUFELRADBAGGER', 12)");
		jdbc.execute("INSERT INTO lego_set (name, manual) VALUES('FORSCHUNGSSCHIFF', 13)");

		repository.count() //
				.as(StepVerifier::create) //
				.expectNext(2L) //
				.verifyComplete();
	}

	@Test
	void shouldDeleteById() {

		jdbc.execute("INSERT INTO lego_set (name, manual) VALUES('SCHAUFELRADBAGGER', 12)");
		Integer id = jdbc.queryForObject("SELECT id from lego_set", Integer.class);

		repository.deleteById(id) //
				.as(StepVerifier::create) //
				.verifyComplete();

		Integer count = jdbc.queryForObject("SELECT COUNT(*) from lego_set", Integer.class);
		assertThat(count).isEqualTo(0);
	}

	@Test
	void shouldDeleteByIdPublisher() {

		jdbc.execute("INSERT INTO lego_set (name, manual) VALUES('SCHAUFELRADBAGGER', 12)");
		Integer id = jdbc.queryForObject("SELECT id from lego_set", Integer.class);

		repository.deleteById(Mono.just(id)) //
				.as(StepVerifier::create) //
				.verifyComplete();

		Integer count = jdbc.queryForObject("SELECT COUNT(*) from lego_set", Integer.class);
		assertThat(count).isEqualTo(0);
	}

	@Test
	void shouldDelete() {

		jdbc.execute("INSERT INTO lego_set (name, manual) VALUES('SCHAUFELRADBAGGER', 12)");
		Integer id = jdbc.queryForObject("SELECT id from lego_set", Integer.class);

		LegoSet legoSet = new LegoSet(id, "SCHAUFELRADBAGGER", 12);

		repository.delete(legoSet) //
				.as(StepVerifier::create) //
				.verifyComplete();

		Integer count = jdbc.queryForObject("SELECT COUNT(*) from lego_set", Integer.class);
		assertThat(count).isEqualTo(0);
	}

	@Test
	void shouldDeleteAllUsingIterable() {

		jdbc.execute("INSERT INTO lego_set (name, manual) VALUES('SCHAUFELRADBAGGER', 12)");
		Integer id = jdbc.queryForObject("SELECT id from lego_set", Integer.class);

		LegoSet legoSet = new LegoSet(id, "SCHAUFELRADBAGGER", 12);

		repository.deleteAll(Collections.singletonList(legoSet)) //
				.as(StepVerifier::create) //
				.verifyComplete();

		Integer count = jdbc.queryForObject("SELECT COUNT(*) from lego_set", Integer.class);
		assertThat(count).isEqualTo(0);
	}

	@Test
	void shouldDeleteAllUsingPublisher() {

		jdbc.execute("INSERT INTO lego_set (name, manual) VALUES('SCHAUFELRADBAGGER', 12)");
		Integer id = jdbc.queryForObject("SELECT id from lego_set", Integer.class);

		LegoSet legoSet = new LegoSet(id, "SCHAUFELRADBAGGER", 12);

		repository.deleteAll(Mono.just(legoSet)) //
				.as(StepVerifier::create) //
				.verifyComplete();

		Integer count = jdbc.queryForObject("SELECT COUNT(*) from lego_set", Integer.class);
		assertThat(count).isEqualTo(0);
	}

	@Test // gh-498
	void shouldDeleteAllById() {

		jdbc.execute("INSERT INTO lego_set (name, manual) VALUES('SCHAUFELRADBAGGER', 12)");
		Integer id = jdbc.queryForObject("SELECT id from lego_set", Integer.class);

		repository.deleteAllById(Collections.singletonList(id)) //
				.as(StepVerifier::create) //
				.verifyComplete();

		Integer count = jdbc.queryForObject("SELECT COUNT(*) from lego_set", Integer.class);
		assertThat(count).isEqualTo(0);
	}

	@Test // gh-538
	void shouldSelectByExampleUsingId() {

		jdbc.execute("INSERT INTO lego_set (name, manual) VALUES('SCHAUFELRADBAGGER', 12)");
		Integer id = jdbc.queryForObject("SELECT id from lego_set", Integer.class);

		LegoSetWithNonScalarId legoSet = new LegoSetWithNonScalarId();
		legoSet.setId(id);

		Example<LegoSetWithNonScalarId> example = Example.of(legoSet);

		repositoryWithNonScalarId.findOne(example) //
				.as(StepVerifier::create) //
				.expectNext(new LegoSetWithNonScalarId(id, "SCHAUFELRADBAGGER", 12, null)) //
				.verifyComplete();
	}

	@Test // gh-538
	void shouldSelectByExampleUsingName() {

		jdbc.execute("INSERT INTO lego_set (id, name, manual) VALUES(0, 'SCHAUFELRADBAGGER', 12)");
		Integer id = jdbc.queryForObject("SELECT id from lego_set", Integer.class);

		LegoSetWithNonScalarId legoSet = new LegoSetWithNonScalarId();
		legoSet.setName("SCHAUFELRADBAGGER");

		Example<LegoSetWithNonScalarId> example = Example.of(legoSet);

		repositoryWithNonScalarId.findOne(example) //
				.as(StepVerifier::create) //
				.expectNext(new LegoSetWithNonScalarId(id, "SCHAUFELRADBAGGER", 12, null)) //
				.verifyComplete();
	}

	@Test // gh-538
	void shouldSelectByExampleUsingManual() {

		jdbc.execute("INSERT INTO lego_set (name, manual) VALUES('SCHAUFELRADBAGGER', 12)");
		Integer id = jdbc.queryForObject("SELECT id from lego_set", Integer.class);

		LegoSetWithNonScalarId legoSet = new LegoSetWithNonScalarId();
		legoSet.setManual(12);

		Example<LegoSetWithNonScalarId> example = Example.of(legoSet);

		repositoryWithNonScalarId.findOne(example) //
				.as(StepVerifier::create) //
				.expectNext(new LegoSetWithNonScalarId(id, "SCHAUFELRADBAGGER", 12, null)) //
				.verifyComplete();
	}

	@Test // gh-538
	void shouldSelectByExampleUsingGlobalStringMatcher() {

		jdbc.execute("INSERT INTO lego_set (id, name, manual) VALUES(1, 'Moon space base', 12)");
		jdbc.execute("INSERT INTO lego_set (id, name, manual) VALUES(2, 'Mars space base', 13)");
		jdbc.execute("INSERT INTO lego_set (id, name, manual) VALUES(3, 'Moon construction kit', 14)");
		jdbc.execute("INSERT INTO lego_set (id, name, manual) VALUES(4, 'Mars construction kit', 15)");

		LegoSetWithNonScalarId legoSet = new LegoSetWithNonScalarId();

		legoSet.setName("Moon");
		Example<LegoSetWithNonScalarId> exampleByStarting = Example.of(legoSet, matching().withStringMatcher(STARTING));

		repositoryWithNonScalarId.findAll(exampleByStarting) //
				.map(LegoSetWithNonScalarId::getName) //
				.as(StepVerifier::create) //
				.expectNext("Moon space base") //
				.expectNext("Moon construction kit") //
				.verifyComplete();

		legoSet.setName("base");
		Example<LegoSetWithNonScalarId> exampleByEnding = Example.of(legoSet, matching().withStringMatcher(ENDING));

		repositoryWithNonScalarId.findAll(exampleByEnding) //
				.map(LegoSetWithNonScalarId::getName) //
				.as(StepVerifier::create) //
				.expectNext("Moon space base") //
				.expectNext("Mars space base") //
				.verifyComplete();

		legoSet.setName("construction");
		Example<LegoSetWithNonScalarId> exampleByContaining = Example.of(legoSet, matching().withStringMatcher(CONTAINING));

		repositoryWithNonScalarId.findAll(exampleByContaining) //
				.map(LegoSetWithNonScalarId::getName) //
				.as(StepVerifier::create) //
				.expectNext("Moon construction kit") //
				.expectNext("Mars construction kit") //
				.verifyComplete();
	}

	@Test // gh-538
	void shouldSelectByExampleUsingFieldLevelStringMatcher() {

		jdbc.execute("INSERT INTO lego_set (name, manual) VALUES('Moon space base', 12)");
		jdbc.execute("INSERT INTO lego_set (name, manual) VALUES('Mars space base', 13)");
		jdbc.execute("INSERT INTO lego_set (name, manual) VALUES('Moon construction kit', 14)");
		jdbc.execute("INSERT INTO lego_set (name, manual) VALUES('Mars construction kit', 15)");

		LegoSetWithNonScalarId legoSet = new LegoSetWithNonScalarId();

		legoSet.setName("Moon");
		Example<LegoSetWithNonScalarId> exampleByFieldBasedStartsWith = Example.of(legoSet,
				matching().withMatcher("name", startsWith()));

		repositoryWithNonScalarId.findAll(exampleByFieldBasedStartsWith) //
				.map(LegoSetWithNonScalarId::getName) //
				.as(StepVerifier::create) //
				.expectNext("Moon space base") //
				.expectNext("Moon construction kit") //
				.verifyComplete();

		legoSet.setName("base");
		Example<LegoSetWithNonScalarId> exampleByFieldBasedEndsWith = Example.of(legoSet,
				matching().withMatcher("name", endsWith()));

		repositoryWithNonScalarId.findAll(exampleByFieldBasedEndsWith) //
				.map(LegoSetWithNonScalarId::getName) //
				.as(StepVerifier::create) //
				.expectNext("Moon space base") //
				.expectNext("Mars space base") //
				.verifyComplete();

		legoSet.setName("construction");
		Example<LegoSetWithNonScalarId> exampleByFieldBasedConstruction = Example.of(legoSet,
				matching().withMatcher("name", contains()));

		repositoryWithNonScalarId.findAll(exampleByFieldBasedConstruction) //
				.map(LegoSetWithNonScalarId::getName) //
				.as(StepVerifier::create) //
				.expectNext("Moon construction kit") //
				.expectNext("Mars construction kit") //
				.verifyComplete();
	}

	@Test // gh-538
	void shouldSelectByExampleIgnoringCase() {

		jdbc.execute("INSERT INTO lego_set (id, name, manual) VALUES(1, 'Moon space base', 12)");
		jdbc.execute("INSERT INTO lego_set (id, name, manual) VALUES(2, 'Mars space base', 13)");
		jdbc.execute("INSERT INTO lego_set (id, name, manual) VALUES(3, 'Moon construction kit', 14)");
		jdbc.execute("INSERT INTO lego_set (id, name, manual) VALUES(4, 'Mars construction kit', 15)");

		LegoSetWithNonScalarId legoSet = new LegoSetWithNonScalarId();

		legoSet.setName("moon SPACE bAsE");
		Example<LegoSetWithNonScalarId> exampleIgnoreCase = Example.of(legoSet, matching().withIgnoreCase());

		repositoryWithNonScalarId.findAll(exampleIgnoreCase) //
				.map(LegoSetWithNonScalarId::getName) //
				.as(StepVerifier::create) //
				.expectNext("Moon space base") //
				.verifyComplete();

		legoSet.setName("moon SPACE bAsE");
		Example<LegoSetWithNonScalarId> exampleByFieldBasedStartsWith = Example.of(legoSet,
				matching().withMatcher("name", ignoreCase()));

		repositoryWithNonScalarId.findAll(exampleByFieldBasedStartsWith) //
				.map(LegoSetWithNonScalarId::getName) //
				.as(StepVerifier::create) //
				.expectNext("Moon space base") //
				.verifyComplete();

	}

	@Test // gh-538
	void shouldFailSelectByExampleWhenUsingRegEx() {

		jdbc.execute("INSERT INTO lego_set (id, name, manual) VALUES(1, 'Moon space base', 12)");
		jdbc.execute("INSERT INTO lego_set (id, name, manual) VALUES(2, 'Mars space base', 13)");
		jdbc.execute("INSERT INTO lego_set (id, name, manual) VALUES(3, 'Moon construction kit', 14)");
		jdbc.execute("INSERT INTO lego_set (id, name, manual) VALUES(4, 'Mars construction kit', 15)");

		LegoSetWithNonScalarId legoSet = new LegoSetWithNonScalarId();

		legoSet.setName("moon");

		Example<LegoSetWithNonScalarId> exampleWithRegExGlobal = Example.of(legoSet, matching().withStringMatcher(REGEX));

		assertThatIllegalStateException().isThrownBy(() -> {

			repositoryWithNonScalarId.findAll(exampleWithRegExGlobal) //
					.map(LegoSetWithNonScalarId::getName) //
					.as(StepVerifier::create) //
					.expectNext("Moon space base") //
					.verifyComplete();
		});

		Example<LegoSetWithNonScalarId> exampleWithFieldRegEx = Example.of(legoSet,
				matching().withMatcher("name", regex()));

		assertThatIllegalStateException().isThrownBy(() -> {

			repositoryWithNonScalarId.findAll(exampleWithFieldRegEx) //
					.map(LegoSetWithNonScalarId::getName) //
					.as(StepVerifier::create) //
					.expectNext("Moon space base") //
					.verifyComplete();
		});

	}

	@Test // gh-538
	void shouldSelectByExampleIncludingNull() {

		jdbc.execute("INSERT INTO lego_set (id, name, extra, manual) VALUES(1, 'Moon space base', 'base', 12)");
		jdbc.execute("INSERT INTO lego_set (id, name, extra, manual) VALUES(2, 'Mars space base', 'base', 13)");
		jdbc.execute("INSERT INTO lego_set (id, name, manual) VALUES(3, 'Moon construction kit', 14)");
		jdbc.execute("INSERT INTO lego_set (id, name, manual) VALUES(4, 'Mars construction kit', 15)");

		LegoSetWithNonScalarId legoSet = new LegoSetWithNonScalarId();
		legoSet.setExtra("base");

		Example<LegoSetWithNonScalarId> exampleIncludingNull = Example.of(legoSet, matching().withIncludeNullValues());

		repositoryWithNonScalarId.findAll(exampleIncludingNull) //
				.map(LegoSetWithNonScalarId::getName) //
				.as(StepVerifier::create) //
				.expectNext("Moon space base", "Mars space base", "Moon construction kit", "Mars construction kit") //
				.verifyComplete();
	}

	@Test // gh-538
	void shouldSelectByExampleWithAnyMatching() {

		jdbc.execute("INSERT INTO lego_set (id, name, manual) VALUES(1, 'Moon space base', 12)");
		jdbc.execute("INSERT INTO lego_set (id, name, manual) VALUES(2, 'Mars space base', 13)");
		jdbc.execute("INSERT INTO lego_set (id, name, manual) VALUES(3, 'Moon construction kit', 14)");
		jdbc.execute("INSERT INTO lego_set (id, name, manual) VALUES(4, 'Mars construction kit', 15)");

		LegoSet legoSet = new LegoSet();
		legoSet.setName("Moon space base");
		legoSet.setManual(15);

		Example<LegoSet> exampleIncludingNull = Example.of(legoSet, matchingAny());

		repository.findAll(exampleIncludingNull) //
				.map(LegoSet::getName) //
				.as(StepVerifier::create) //
				.expectNext("Moon space base", "Mars construction kit") //
				.verifyComplete();
	}

	@Test // gh-538
	void shouldCountByExampleUsingId() {

		jdbc.execute("INSERT INTO lego_set (name, manual) VALUES('SCHAUFELRADBAGGER', 12)");
		Integer id = jdbc.queryForObject("SELECT id from lego_set", Integer.class);

		LegoSetWithNonScalarId legoSet = new LegoSetWithNonScalarId();
		legoSet.setId(id);

		Example<LegoSetWithNonScalarId> example = Example.of(legoSet);

		repositoryWithNonScalarId.count(example) //
				.as(StepVerifier::create) //
				.expectNextCount(1) //
				.verifyComplete();
	}

	@Test // gh-538
	void shouldCheckExistenceByExampleUsingId() {

		jdbc.execute("INSERT INTO lego_set (name, manual) VALUES('SCHAUFELRADBAGGER', 12)");
		Integer id = jdbc.queryForObject("SELECT id from lego_set", Integer.class);

		LegoSetWithNonScalarId legoSet = new LegoSetWithNonScalarId();
		legoSet.setId(id);

		Example<LegoSetWithNonScalarId> example = Example.of(legoSet);

		repositoryWithNonScalarId.exists(example) //
				.as(StepVerifier::create) //
				.expectNext(true) //
				.verifyComplete();
	}

	@Data
	@Table("legoset")
	@AllArgsConstructor
	@NoArgsConstructor
	static class LegoSet {

		@Id int id;
		String name;
		Integer manual;
	}

	@Data
	@Table("legoset")
	@AllArgsConstructor
	@NoArgsConstructor
	static class LegoSetWithNonScalarId {

		@Id Integer id;
		String name;
		Integer manual;
		String extra;
	}

	@Data
	@Table("legoset")
	@NoArgsConstructor
	static class LegoSetVersionable extends LegoSet {

		@Version Integer version;

		LegoSetVersionable(int id, String name, Integer manual, Integer version) {
			super(id, name, manual);
			this.version = version;
		}
	}

	@Data
	@Table("legoset")
	@NoArgsConstructor
	static class LegoSetPrimitiveVersionable extends LegoSet {

		@Version int version;

		LegoSetPrimitiveVersionable(int id, String name, Integer manual, int version) {
			super(id, name, manual);
			this.version = version;
		}
	}
}
