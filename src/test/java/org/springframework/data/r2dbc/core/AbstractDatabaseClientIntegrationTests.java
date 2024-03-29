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
package org.springframework.data.r2dbc.core;

import io.r2dbc.spi.ConnectionFactory;
import lombok.Data;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.r2dbc.testing.R2dbcIntegrationTestSupport;
import org.springframework.data.relational.core.query.Criteria;
import org.springframework.data.relational.core.query.Update;
import org.springframework.jdbc.core.JdbcTemplate;
import reactor.test.StepVerifier;

import javax.sql.DataSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.data.domain.Sort.Order.desc;
import static org.springframework.data.r2dbc.query.Criteria.where;

/**
 * Integration tests for {@link DatabaseClient}.
 *
 * @author Mark Paluch
 * @author Mingyuan Wu
 */
public abstract class AbstractDatabaseClientIntegrationTests extends R2dbcIntegrationTestSupport {

	private ConnectionFactory connectionFactory;

	private JdbcTemplate jdbc;

	@BeforeEach
	public void before() {

		connectionFactory = createConnectionFactory();

		jdbc = createJdbcTemplate(createDataSource());

		try {
			jdbc.execute("DROP TABLE lego_set");
		} catch (DataAccessException e) {}
		jdbc.execute(getCreateTableStatement());
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
	 * <li>id integer (primary key), not null</li>
	 * <li>name varchar(255), nullable</li>
	 * <li>manual integer, nullable</li>
	 * </ul>
	 *
	 * @return the CREATE TABLE statement for table {@code lego_set} with three columns.
	 */
	protected abstract String getCreateTableStatement();

	/**
	 * Get a parameterized {@code INSERT INTO lego_set} statement setting id, name, and manual values.
	 */
	protected String getInsertIntoLegosetStatement() {
		return "INSERT INTO lego_set (id, name, manual) VALUES(:id, :name, :manual)";
	}

	@Test // gh-2
	public void executeInsert() {

		DatabaseClient databaseClient = DatabaseClient.create(connectionFactory);

		databaseClient.execute(getInsertIntoLegosetStatement()) //
				.bind("id", 42055) //
				.bind("name", "SCHAUFELRADBAGGER") //
				.bindNull("manual", Integer.class) //
				.fetch().rowsUpdated() //
				.as(StepVerifier::create) //
				.expectNext(1) //
				.verifyComplete();

		assertThat(jdbc.queryForMap("SELECT id, name, manual FROM lego_set")).hasEntrySatisfying("id", numberOf(42055));
	}

	@Test // gh-2
	public void shouldTranslateDuplicateKeyException() {

		DatabaseClient databaseClient = DatabaseClient.create(connectionFactory);

		executeInsert();

		databaseClient.execute(getInsertIntoLegosetStatement()) //
				.bind("id", 42055) //
				.bind("name", "SCHAUFELRADBAGGER") //
				.bindNull("manual", Integer.class) //
				.fetch().rowsUpdated() //
				.as(StepVerifier::create) //
				.expectErrorSatisfies(exception -> assertThat(exception) //
						.isInstanceOf(DataIntegrityViolationException.class) //
						.hasMessageContaining("execute; SQL [INSERT INTO lego_set")) //
				.verify();
	}

	@Test // gh-2
	public void executeSelect() {

		jdbc.execute("INSERT INTO lego_set (id, name, manual) VALUES(42055, 'SCHAUFELRADBAGGER', 12)");

		DatabaseClient databaseClient = DatabaseClient.create(connectionFactory);

		databaseClient.execute("SELECT id, name, manual FROM lego_set") //
				.as(LegoSet.class) //
				.fetch().all() //
				.as(StepVerifier::create) //
				.consumeNextWith(actual -> {

					assertThat(actual.getId()).isEqualTo(42055);
					assertThat(actual.getName()).isEqualTo("SCHAUFELRADBAGGER");
					assertThat(actual.getManual()).isEqualTo(12);
				}).verifyComplete();
	}

	@Test // gh-2
	public void executeSelectNamedParameters() {

		DatabaseClient databaseClient = DatabaseClient.create(connectionFactory);

		databaseClient.execute("SELECT id, name, manual FROM lego_set WHERE name = :name or name = :name") //
				.bind("name", "unknown").as(LegoSet.class) //
				.fetch().all() //
				.as(StepVerifier::create) //
				.verifyComplete();
	}

//	@Test // gh-2 // TODO: bug
	public void insert() {

		DatabaseClient databaseClient = DatabaseClient.create(connectionFactory);

		databaseClient.insert().into("lego_set")//
				.value("id", 42055) //
				.value("name", "SCHAUFELRADBAGGER") //
				.nullValue("manual", Integer.class) //
				.fetch() //
				.rowsUpdated() //
				.as(StepVerifier::create) //
				.expectNext(1) //
				.verifyComplete();

		assertThat(jdbc.queryForMap("SELECT id, name, manual FROM lego_set")).hasEntrySatisfying("id", numberOf(42055));
	}

//	@Test // gh-2 // TODO: bug
	public void insertWithoutResult() {

		DatabaseClient databaseClient = DatabaseClient.create(connectionFactory);

		databaseClient.insert().into("lego_set")//
				.value("id", 42055) //
				.value("name", "SCHAUFELRADBAGGER") //
				.nullValue("manual", Integer.class) //
				.then() //
				.as(StepVerifier::create) //
				.verifyComplete();

		assertThat(jdbc.queryForMap("SELECT id, name, manual FROM lego_set")).hasEntrySatisfying("id", numberOf(42055));
	}

//	@Test // gh-2 // TODO: bug
	public void insertTypedObject() {

		LegoSet legoSet = new LegoSet();
		legoSet.setId(42055);
		legoSet.setName("SCHAUFELRADBAGGER");
		legoSet.setManual(12);

		DatabaseClient databaseClient = DatabaseClient.create(connectionFactory);

		databaseClient.insert().into(LegoSet.class)//
				.using(legoSet) //
				.fetch() //
				.rowsUpdated() //
				.as(StepVerifier::create) //
				.expectNext(1) //
				.verifyComplete();

		assertThat(jdbc.queryForMap("SELECT id, name, manual FROM lego_set")).hasEntrySatisfying("id", numberOf(42055));
	}

//	@Test // gh-2 // TODO: bug
	public void insertTypedObjectWithBinary() {

		LegoSet legoSet = new LegoSet();
		legoSet.setId(42055);
		legoSet.setName("SCHAUFELRADBAGGER");
		legoSet.setManual(12);
		legoSet.setCert(new byte[] { 1, 2, 3, 4, 5 });

		DatabaseClient databaseClient = DatabaseClient.create(connectionFactory);

		databaseClient.insert().into(LegoSet.class)//
				.using(legoSet) //
				.fetch() //
				.rowsUpdated() //
				.then() //
				.as(StepVerifier::create) //
				.verifyComplete();

		databaseClient.select().from(LegoSet.class) //
				.matching(where("name").is("SCHAUFELRADBAGGER")) //
				.fetch() //
				.first() //
				.as(StepVerifier::create) //
				.assertNext(actual -> {

					assertThat(actual.getCert()).isEqualTo(new byte[] { 1, 2, 3, 4, 5 });
				}).verifyComplete();
	}

	@Test // gh-64
	public void update() {

		jdbc.execute("INSERT INTO lego_set (id, name, manual) VALUES(42055, 'SCHAUFELRADBAGGER', 12)");

		DatabaseClient databaseClient = DatabaseClient.create(connectionFactory);

		databaseClient.update().table("lego_set")//
				.using(Update.update("name", "Lego")) //
				.matching(Criteria.where("id").is(42055)) //
				.fetch() //
				.rowsUpdated() //
				.as(StepVerifier::create) //
				.expectNext(1) //
				.verifyComplete();

		assertThat(jdbc.queryForMap("SELECT name, manual FROM lego_set")).containsEntry("name", "Lego");
	}

	@Test // gh-64
	public void updateWithoutResult() {

		jdbc.execute("INSERT INTO lego_set (id, name, manual) VALUES(42055, 'SCHAUFELRADBAGGER', 12)");

		DatabaseClient databaseClient = DatabaseClient.create(connectionFactory);

		databaseClient.update().table("lego_set")//
				.using(Update.update("name", "Lego")) //
				.then() //
				.as(StepVerifier::create) //
				.verifyComplete();

		assertThat(jdbc.queryForMap("SELECT name, manual FROM lego_set")).containsEntry("name", "Lego");
	}

	@Test // gh-64
	public void updateTypedObject() {

		jdbc.execute("INSERT INTO lego_set (id, name, manual) VALUES(42055, 'SCHAUFELRADBAGGER', 12)");

		LegoSet legoSet = new LegoSet();
		legoSet.setId(42055);
		legoSet.setName("Lego");
		legoSet.setManual(null);

		DatabaseClient databaseClient = DatabaseClient.create(connectionFactory);

		databaseClient.update() //
				.table(LegoSet.class) //
				.using(legoSet) //
				.fetch() //
				.rowsUpdated() //
				.as(StepVerifier::create) //
				.expectNext(1) //
				.verifyComplete();

		assertThat(jdbc.queryForMap("SELECT name, manual FROM lego_set")).containsEntry("name", "Lego");
	}

	@Test // gh-64
	public void deleteUntyped() {

		jdbc.execute("INSERT INTO lego_set (id, name, manual) VALUES(42055, 'SCHAUFELRADBAGGER', 12)");
		jdbc.execute("INSERT INTO lego_set (id, name, manual) VALUES(42064, 'FORSCHUNGSSCHIFF', 13)");

		DatabaseClient databaseClient = DatabaseClient.create(connectionFactory);

		databaseClient.delete() //
				.from("lego_set") //
				.matching(where("id").is(42055)) //
				.fetch() //
				.rowsUpdated() //
				.as(StepVerifier::create) //
				.expectNext(1).verifyComplete();

		assertThat(jdbc.queryForList("SELECT id AS count FROM lego_set")).hasSize(1);
	}

	@Test // gh-64
	public void deleteTyped() {

		jdbc.execute("INSERT INTO lego_set (id, name, manual) VALUES(42055, 'SCHAUFELRADBAGGER', 12)");
		jdbc.execute("INSERT INTO lego_set (id, name, manual) VALUES(42064, 'FORSCHUNGSSCHIFF', 13)");

		DatabaseClient databaseClient = DatabaseClient.create(connectionFactory);

		databaseClient.delete() //
				.from(LegoSet.class) //
				.matching(where("id").is(42055)) //
				.then() //
				.as(StepVerifier::create) //
				.verifyComplete();

		assertThat(jdbc.queryForList("SELECT id AS count FROM lego_set")).hasSize(1);
	}

	@Test // gh-2
	public void selectAsMap() {

		jdbc.execute("INSERT INTO lego_set (id, name, manual) VALUES(42055, 'SCHAUFELRADBAGGER', 12)");

		DatabaseClient databaseClient = DatabaseClient.create(connectionFactory);

		databaseClient.select().from(LegoSet.class) //
				.project("id", "name", "manual") //
				.orderBy(Sort.by("id")) //
				.fetch() //
				.all() //
				.as(StepVerifier::create) //
				.assertNext(actual -> {
					assertThat(actual.getId()).isEqualTo(42055);
					assertThat(actual.getName()).isEqualTo("SCHAUFELRADBAGGER");
					assertThat(actual.getManual()).isEqualTo(12);
				}).verifyComplete();
	}

	@Test // gh-8
	public void selectExtracting() {

		jdbc.execute("INSERT INTO lego_set (id, name, manual) VALUES(42055, 'SCHAUFELRADBAGGER', 12)");

		DatabaseClient databaseClient = DatabaseClient.create(connectionFactory);

		databaseClient.select().from("lego_set") //
				.project("id", "name", "manual") //
				.orderBy(Sort.by("id")) //
				.map((r) -> r.get("id", Integer.class)) //
				.all() //
				.as(StepVerifier::create) //
				.expectNext(42055) //
				.verifyComplete();
	}

	@Test // gh-109
	public void selectSimpleTypeProjection() {

		jdbc.execute("INSERT INTO lego_set (id, name, manual) VALUES(42055, 'SCHAUFELRADBAGGER', 12)");

		DatabaseClient databaseClient = DatabaseClient.create(connectionFactory);

		databaseClient.execute("SELECT COUNT(*) FROM lego_set") //
				.as(Long.class) //
				.fetch() //
				.all() //
				.as(StepVerifier::create) //
				.expectNext(1L) //
				.verifyComplete();

		databaseClient.execute("SELECT name FROM lego_set") //
				.as(String.class) //
				.fetch() //
				.one() //
				.as(StepVerifier::create) //
				.expectNext("SCHAUFELRADBAGGER") //
				.verifyComplete();
	}

	@Test // gh-8
	public void selectWithCriteria() {

		jdbc.execute("INSERT INTO lego_set (id, name, manual) VALUES(42055, 'SCHAUFELRADBAGGER', 12)");

		DatabaseClient databaseClient = DatabaseClient.create(connectionFactory);

		databaseClient.select().from("lego_set") //
				.project("id", "name", "manual") //
				.orderBy(Sort.by("id")) //
				.matching(where("id").greaterThanOrEquals(42055).and("id").lessThanOrEquals(42055))
				.map((r) -> r.get("id", Integer.class)) //
				.all() //
				.as(StepVerifier::create) //
				.expectNext(42055) //
				.verifyComplete();
	}

	@Test // gh-64
	public void selectWithCriteriaIn() {

		jdbc.execute("INSERT INTO lego_set (id, name, manual) VALUES(42055, 'SCHAUFELRADBAGGER', 12)");
		jdbc.execute("INSERT INTO lego_set (id, name, manual) VALUES(42064, 'FORSCHUNGSSCHIFF', 13)");
		jdbc.execute("INSERT INTO lego_set (id, name, manual) VALUES(42068, 'FLUGHAFEN-LÖSCHFAHRZEUG', 13)");

		DatabaseClient databaseClient = DatabaseClient.create(connectionFactory);

		databaseClient.select().from(LegoSet.class) //
				.orderBy(Sort.by("id")) //
				.matching(where("id").in(42055, 42064)) //
				.map((r, md) -> r.get("id", Integer.class)) //
				.all() //
				.as(StepVerifier::create) //
				.expectNext(42055) //
				.expectNext(42064) //
				.verifyComplete();
	}

	@Test // gh-2
	public void selectOrderByIdDesc() {

		jdbc.execute("INSERT INTO lego_set (id, name, manual) VALUES(42055, 'SCHAUFELRADBAGGER', 12)");
		jdbc.execute("INSERT INTO lego_set (id, name, manual) VALUES(42064, 'FORSCHUNGSSCHIFF', 13)");
		jdbc.execute("INSERT INTO lego_set (id, name, manual) VALUES(42068, 'FLUGHAFEN-LÖSCHFAHRZEUG', 13)");

		DatabaseClient databaseClient = DatabaseClient.create(connectionFactory);

		databaseClient.select().from(LegoSet.class) //
				.orderBy(Sort.by(desc("id"))) //
				.fetch().all() //
				.map(LegoSet::getId) //
				.as(StepVerifier::create) //
				.expectNext(42068, 42064, 42055) //
				.verifyComplete();
	}

	@Test // gh-2
	public void selectOrderPaged() {

		jdbc.execute("INSERT INTO lego_set (id, name, manual) VALUES(42055, 'SCHAUFELRADBAGGER', 12)");
		jdbc.execute("INSERT INTO lego_set (id, name, manual) VALUES(42064, 'FORSCHUNGSSCHIFF', 13)");
		jdbc.execute("INSERT INTO lego_set (id, name, manual) VALUES(42068, 'FLUGHAFEN-LÖSCHFAHRZEUG', 13)");

		DatabaseClient databaseClient = DatabaseClient.create(connectionFactory);

		databaseClient.select().from(LegoSet.class) //
				.orderBy(Sort.by(desc("id"))) //
				.page(PageRequest.of(2, 1)) //
				.fetch().all() //
				.map(LegoSet::getId) //
				.as(StepVerifier::create) //
				.expectNext(42055) //
				.verifyComplete();

		databaseClient.select().from(LegoSet.class) //
				.page(PageRequest.of(2, 1, Sort.by(Sort.Direction.ASC, "id"))) //
				.fetch().all() //
				.map(LegoSet::getId) //
				.as(StepVerifier::create) //
				.expectNext(42068) //
				.verifyComplete();
	}

	@Test // gh-2
	public void selectTypedLater() {

		jdbc.execute("INSERT INTO lego_set (id, name, manual) VALUES(42055, 'SCHAUFELRADBAGGER', 12)");
		jdbc.execute("INSERT INTO lego_set (id, name, manual) VALUES(42064, 'FORSCHUNGSSCHIFF', 13)");
		jdbc.execute("INSERT INTO lego_set (id, name, manual) VALUES(42068, 'FLUGHAFEN-LÖSCHFAHRZEUG', 13)");

		DatabaseClient databaseClient = DatabaseClient.create(connectionFactory);

		databaseClient.select().from("lego_set") //
				.orderBy(Sort.by(desc("id"))) //
				.as(LegoSet.class) //
				.fetch().all() //
				.map(LegoSet::getId) //
				.as(StepVerifier::create) //
				.expectNext(42068, 42064, 42055) //
				.verifyComplete();
	}

	private Condition<? super Object> numberOf(int expected) {
		return new Condition<>(it -> {
			return it instanceof Number && ((Number) it).intValue() == expected;
		}, "Number  %d", expected);
	}

	@Data
//	@Table("lego_set")
	static class LegoSet {

		/*@Id */int id;
		String name;
		Integer manual;
		byte[] cert;
	}
}
