package org.springframework.data.r2dbc.support;

import io.r2dbc.spi.ConnectionFactories;
import org.springframework.data.r2dbc.core.DefaultReactiveDataAccessStrategy;
import org.springframework.data.r2dbc.dialect.DialectResolver;
import org.springframework.data.r2dbc.repository.support.R2dbcRepositoryFactory;
import org.springframework.r2dbc.core.DatabaseClient;

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
}