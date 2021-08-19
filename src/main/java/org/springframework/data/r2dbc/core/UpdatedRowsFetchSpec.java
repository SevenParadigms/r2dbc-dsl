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

import reactor.core.publisher.Mono;

/**
 * Contract for fetching the number of affected rows.
 *
 * @author Mark Paluch
 * @deprecated since 1.2, use Spring's {@link org.springframework.r2dbc.core.UpdatedRowsFetchSpec} support instead.
 */
@Deprecated
public interface UpdatedRowsFetchSpec {

	/**
	 * Get the number of updated rows.
	 *
	 * @return {@link Mono} emitting the number of updated rows. Never {@literal null}.
	 */
	Mono<Integer> rowsUpdated();
}
