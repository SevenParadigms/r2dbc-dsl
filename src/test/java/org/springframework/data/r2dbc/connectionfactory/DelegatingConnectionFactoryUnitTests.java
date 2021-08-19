/*
 * Copyright 2019-2021 the original author or authors.
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
package org.springframework.data.r2dbc.connectionfactory;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import reactor.core.publisher.Mono;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link DelegatingConnectionFactory}.
 *
 * @author Mark Paluch
 */
public class DelegatingConnectionFactoryUnitTests {

	ConnectionFactory delegate = mock(ConnectionFactory.class);
	Connection connectionMock = mock(Connection.class);

	DelegatingConnectionFactory connectionFactory = new ExampleConnectionFactory(delegate);

	@Test // gh-107
	public void shouldDelegateGetConnection() {

		Mono<Connection> connectionMono = Mono.just(connectionMock);
		when(delegate.create()).thenReturn((Mono) connectionMono);

		assertThat(connectionFactory.create()).isSameAs(connectionMono);
	}

	@Test // gh-107
	public void shouldDelegateUnwrapWithoutImplementing() {
		assertThat(connectionFactory.unwrap()).isSameAs(delegate);
	}

	static class ExampleConnectionFactory extends DelegatingConnectionFactory {

		ExampleConnectionFactory(ConnectionFactory targetConnectionFactory) {
			super(targetConnectionFactory);
		}
	}

}
