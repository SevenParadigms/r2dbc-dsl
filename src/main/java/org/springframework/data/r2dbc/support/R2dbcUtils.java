package org.springframework.data.r2dbc.support;

import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.Result;
import org.springframework.data.annotation.Id;
import org.springframework.data.r2dbc.config.Beans;
import org.springframework.data.r2dbc.core.DefaultReactiveDataAccessStrategy;
import org.springframework.data.r2dbc.dialect.DialectResolver;
import org.springframework.data.r2dbc.repository.query.Dsl;
import org.springframework.data.r2dbc.repository.support.R2dbcRepositoryFactory;
import org.springframework.r2dbc.core.DatabaseClient;
import reactor.core.publisher.Flux;

import java.util.ArrayList;

/**
 * Utilities for r2dbc compliant.
 *
 * @author Lao Tsing
 */
public abstract class R2dbcUtils {
    public static <T> T getRepository(String r2dbcUrl, Class<T> cls) {
        var connectionFactory = ConnectionFactories.get(r2dbcUrl);
        return new R2dbcRepositoryFactory(
                DatabaseClient.builder().connectionFactory(connectionFactory).build(),
                new DefaultReactiveDataAccessStrategy(DialectResolver.getDialect(connectionFactory))
        ).getRepository(cls);
    }

    public static <T> T getRepository(Class<T> cls) {
        var databaseClient = Beans.of(DatabaseClient.class);
        var dialect = DialectResolver.getDialect(databaseClient.getConnectionFactory());
        return new R2dbcRepositoryFactory(
                databaseClient,
                new DefaultReactiveDataAccessStrategy(dialect)
        ).getRepository(cls);
    }

    public <S> Flux<Result> batchSave(Iterable<S> models) {
        var databaseClient = Beans.of(DatabaseClient.class);
        var connectionFactory = (PostgresqlConnectionFactory) databaseClient.getConnectionFactory();
        try {
            var query = new StringBuilder();
            for (S target : models) {
                var fields = new ArrayList<String>();
                var reflectionStorage = FastMethodInvoker.reflectionStorage(target.getClass());
                for (var field : reflectionStorage) {
                    if (!field.isAnnotationPresent(Id.class) && !field.getName().equals(SqlField.id)) {
                        fields.add(Dsl.COLON.concat(field.getName()).concat(Dsl.COLON));
                    }
                }
                var buildFields = String.join(Dsl.COMMA, fields);
                var template = "INSERT INTO " + WordUtils.camelToSql(target.getClass().getSimpleName()) +
                        "(" + WordUtils.camelToSql(buildFields.replaceAll(":", "")) + ") VALUES(" + buildFields + ");";
                query.append(DslUtils.binding(template, target));
            }
            return connectionFactory.create().flatMap(c -> c.createBatch().add(query.toString()).execute().collectList()).flatMapMany(Flux::fromIterable);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}