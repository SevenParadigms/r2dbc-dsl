package org.springframework.data.r2dbc.repository.security;

@FunctionalInterface
public interface AuthenticationIdentifierResolver {
    Object resolve();
}
