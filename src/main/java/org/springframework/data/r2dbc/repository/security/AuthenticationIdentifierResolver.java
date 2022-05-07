package org.springframework.data.r2dbc.repository.security;

import reactor.core.publisher.Mono;

@FunctionalInterface
public interface AuthenticationIdentifierResolver {
    Mono<Object> resolve();
}
