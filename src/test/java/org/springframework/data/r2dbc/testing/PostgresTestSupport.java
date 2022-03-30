package org.springframework.data.r2dbc.testing;

import io.r2dbc.spi.ConnectionFactory;
import org.postgresql.ds.PGSimpleDataSource;
import org.springframework.data.r2dbc.testing.ExternalDatabase.ProvidedDatabase;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.sql.DataSource;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Utility class for testing against Postgres.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @author Bogdan Ilchyshyn
 */
public class PostgresTestSupport {

	private static ExternalDatabase testContainerDatabase;

	public static String CREATE_TABLE_LEGOSET = "CREATE TABLE lego_set (\n" //
			+ "    id          serial PRIMARY KEY,\n" //
			+ "    version     integer NULL,\n" //
			+ "    name        varchar(255) NOT NULL,\n" //
			+ "    manual      integer NULL\n," //
			+ "    now         timestamp NULL\n," //
			+ "    exp         text NULL\n," //
			+ "    name_equality         text NULL\n," //
			+ "    manual_read_only         integer NULL\n," //
			+ "    counter_version         integer NULL\n," //
			+ "    data         date NULL\n," //
			+ "    cert        bytea NULL\n" //
			+ ");";

	public static final String DROP_TABLE_LEGOSET = "DROP TABLE lego_set";
	/**
	 * Returns a database either hosted locally at {@code postgres:@localhost:5432/postgres} or running inside Docker.
	 *
	 * @return information about the database. Guaranteed to be not {@literal null}.
	 */
	public static ExternalDatabase database() {

		if (Boolean.getBoolean("spring.data.r2dbc.test.preferLocalDatabase")) {

			return getFirstWorkingDatabase( //
					PostgresTestSupport::local, //
					PostgresTestSupport::testContainer //
			);
		} else {

			return getFirstWorkingDatabase( //
					PostgresTestSupport::testContainer, //
					PostgresTestSupport::local //
			);
		}
	}

	@SafeVarargs
	private static ExternalDatabase getFirstWorkingDatabase(Supplier<ExternalDatabase>... suppliers) {

		return Stream.of(suppliers).map(Supplier::get) //
				.filter(ExternalDatabase::checkValidity) //
				.findFirst() //
				.orElse(ExternalDatabase.unavailable());
	}

	/**
	 * Returns a locally provided database at {@code postgres:@localhost:5432/postgres}.
	 */
	private static ExternalDatabase local() {

		return ProvidedDatabase.builder() //
				.hostname("localhost") //
				.port(5432) //
				.database("postgres") //
				.username("postgres") //
				.password("postgres") //
				.jdbcUrl("jdbc:postgresql://localhost:5432/postgres") //
				.build();
	}

	/**
	 * Returns a database provided via Testcontainers.
	 */
	private static ExternalDatabase testContainer() {

		if (testContainerDatabase == null) {

			try {
				PostgreSQLContainer container = new PostgreSQLContainer(
						PostgreSQLContainer.IMAGE + ":" + PostgreSQLContainer.DEFAULT_TAG);
				container.start();

				testContainerDatabase = ProvidedDatabase.builder(container).database(container.getDatabaseName()).build();

			} catch (IllegalStateException ise) {
				// docker not available.
				testContainerDatabase = ExternalDatabase.unavailable();
			}

		}

		return testContainerDatabase;
	}

	/**
	 * Creates a new {@link ConnectionFactory} configured from the {@link ExternalDatabase}..
	 */
	public static ConnectionFactory createConnectionFactory(ExternalDatabase database) {
		return ConnectionUtils.getConnectionFactory("postgresql", database);
	}

	/**
	 * Creates a new {@link DataSource} configured from the {@link ExternalDatabase}.
	 */
	public static DataSource createDataSource(ExternalDatabase database) {

		PGSimpleDataSource dataSource = new PGSimpleDataSource();

		dataSource.setUser(database.getUsername());
		dataSource.setPassword(database.getPassword());
		dataSource.setURL(database.getJdbcUrl());

		return dataSource;
	}
}
