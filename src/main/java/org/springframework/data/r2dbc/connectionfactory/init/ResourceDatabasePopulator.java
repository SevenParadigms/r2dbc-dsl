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
package org.springframework.data.r2dbc.connectionfactory.init;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.core.io.Resource;
import org.springframework.core.io.buffer.DataBufferFactory;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import org.springframework.core.io.support.EncodedResource;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * Populates, initializes, or cleans up a database using SQL scripts defined in external resources.
 * <ul>
 * <li>Call {@link #addScript} to add a single SQL script location.
 * <li>Call {@link #addScripts} to add multiple SQL script locations.
 * <li>Consult the setter methods in this class for further configuration options.
 * <li>Call {@link #populate} or {@link #execute} to initialize or clean up the database using the configured scripts.
 * </ul>
 *
 * @author Mark Paluch
 * @see DatabasePopulatorUtils
 * @see ScriptUtils
 * @deprecated since 1.2 in favor of Spring R2DBC. Use
 *             {@link org.springframework.r2dbc.connection.init.ResourceDatabasePopulator} instead.
 */
@Deprecated
public class ResourceDatabasePopulator implements DatabasePopulator {

	List<Resource> scripts = new ArrayList<>();

	private @Nullable Charset sqlScriptEncoding;

	private String separator = ScriptUtils.DEFAULT_STATEMENT_SEPARATOR;

	private String commentPrefix = ScriptUtils.DEFAULT_COMMENT_PREFIX;

	private String blockCommentStartDelimiter = ScriptUtils.DEFAULT_BLOCK_COMMENT_START_DELIMITER;

	private String blockCommentEndDelimiter = ScriptUtils.DEFAULT_BLOCK_COMMENT_END_DELIMITER;

	private boolean continueOnError = false;

	private boolean ignoreFailedDrops = false;

	private DataBufferFactory dataBufferFactory = new DefaultDataBufferFactory();

	/**
	 * Creates a new {@link ResourceDatabasePopulator} with default settings.
	 */
	public ResourceDatabasePopulator() {}

	/**
	 * Creates a new {@link ResourceDatabasePopulator} with default settings for the supplied scripts.
	 *
	 * @param scripts the scripts to execute to initialize or clean up the database (never {@literal null})
	 */
	public ResourceDatabasePopulator(Resource... scripts) {
		setScripts(scripts);
	}

	/**
	 * Creates a new {@link ResourceDatabasePopulator} with the supplied values.
	 *
	 * @param continueOnError flag to indicate that all failures in SQL should be logged but not cause a failure
	 * @param ignoreFailedDrops flag to indicate that a failed SQL {@code DROP} statement can be ignored
	 * @param sqlScriptEncoding the encoding for the supplied SQL scripts (may be {@literal null} or <em>empty</em> to
	 *          indicate platform encoding)
	 * @param scripts the scripts to execute to initialize or clean up the database, must not be {@literal null}.
	 */
	public ResourceDatabasePopulator(boolean continueOnError, boolean ignoreFailedDrops,
			@Nullable String sqlScriptEncoding, Resource... scripts) {

		this.continueOnError = continueOnError;
		this.ignoreFailedDrops = ignoreFailedDrops;
		setSqlScriptEncoding(sqlScriptEncoding);
		setScripts(scripts);
	}

	/**
	 * Add a script to execute to initialize or clean up the database.
	 *
	 * @param script the path to an SQL script, must not be {@literal null}.
	 */
	public void addScript(Resource script) {
		Assert.notNull(script, "Script must not be null");
		this.scripts.add(script);
	}

	/**
	 * Add multiple scripts to execute to initialize or clean up the database.
	 *
	 * @param scripts the scripts to execute, must not be {@literal null}.
	 */
	public void addScripts(Resource... scripts) {
		assertContentsOfScriptArray(scripts);
		this.scripts.addAll(Arrays.asList(scripts));
	}

	/**
	 * Set the scripts to execute to initialize or clean up the database, replacing any previously added scripts.
	 *
	 * @param scripts the scripts to execute, must not be {@literal null}.
	 */
	public void setScripts(Resource... scripts) {
		assertContentsOfScriptArray(scripts);
		// Ensure that the list is modifiable
		this.scripts = new ArrayList<>(Arrays.asList(scripts));
	}

	private void assertContentsOfScriptArray(Resource... scripts) {
		Assert.notNull(scripts, "Scripts array must not be null");
		Assert.noNullElements(scripts, "Scripts array must not contain null elements");
	}

	/**
	 * Specify the encoding for the configured SQL scripts, if different from the platform encoding.
	 *
	 * @param sqlScriptEncoding the encoding used in scripts (may be {@literal null} or empty to indicate platform
	 *          encoding).
	 * @see #addScript(Resource)
	 */
	public void setSqlScriptEncoding(@Nullable String sqlScriptEncoding) {
		setSqlScriptEncoding(StringUtils.hasText(sqlScriptEncoding) ? Charset.forName(sqlScriptEncoding) : null);
	}

	/**
	 * Specify the encoding for the configured SQL scripts, if different from the platform encoding.
	 *
	 * @param sqlScriptEncoding the encoding used in scripts (may be {@literal null} to indicate platform encoding).
	 * @see #addScript(Resource)
	 */
	public void setSqlScriptEncoding(@Nullable Charset sqlScriptEncoding) {
		this.sqlScriptEncoding = sqlScriptEncoding;
	}

	/**
	 * Specify the statement separator, if a custom one.
	 * <p/>
	 * Defaults to {@code ";"} if not specified and falls back to {@code "\n"} as a last resort; may be set to
	 * {@link ScriptUtils#EOF_STATEMENT_SEPARATOR} to signal that each script contains a single statement without a
	 * separator.
	 *
	 * @param separator the script statement separator.
	 */
	public void setSeparator(String separator) {
		this.separator = separator;
	}

	/**
	 * Set the prefix that identifies single-line comments within the SQL scripts.
	 * <p/>
	 * Defaults to {@code "--"}.
	 *
	 * @param commentPrefix the prefix for single-line comments
	 */
	public void setCommentPrefix(String commentPrefix) {
		this.commentPrefix = commentPrefix;
	}

	/**
	 * Set the start delimiter that identifies block comments within the SQL scripts.
	 * <p/>
	 * Defaults to {@code "/*"}.
	 *
	 * @param blockCommentStartDelimiter the start delimiter for block comments (never {@literal null} or empty).
	 * @see #setBlockCommentEndDelimiter
	 */
	public void setBlockCommentStartDelimiter(String blockCommentStartDelimiter) {

		Assert.hasText(blockCommentStartDelimiter, "BlockCommentStartDelimiter must not be null or empty");

		this.blockCommentStartDelimiter = blockCommentStartDelimiter;
	}

	/**
	 * Set the end delimiter that identifies block comments within the SQL scripts.
	 * <p/>
	 * Defaults to {@code "*&#47;"}.
	 *
	 * @param blockCommentEndDelimiter the end delimiter for block comments (never {@literal null} or empty)
	 * @see #setBlockCommentStartDelimiter
	 */
	public void setBlockCommentEndDelimiter(String blockCommentEndDelimiter) {

		Assert.hasText(blockCommentEndDelimiter, "BlockCommentEndDelimiter must not be null or empty");

		this.blockCommentEndDelimiter = blockCommentEndDelimiter;
	}

	/**
	 * Flag to indicate that all failures in SQL should be logged but not cause a failure.
	 * <p/>
	 * Defaults to {@literal false}.
	 *
	 * @param continueOnError {@literal true} if script execution should continue on error.
	 */
	public void setContinueOnError(boolean continueOnError) {
		this.continueOnError = continueOnError;
	}

	/**
	 * Flag to indicate that a failed SQL {@code DROP} statement can be ignored.
	 * <p/>
	 * This is useful for a non-embedded database whose SQL dialect does not support an {@code IF EXISTS} clause in a
	 * {@code DROP} statement.
	 * <p/>
	 * The default is {@literal false} so that if the populator runs accidentally, it will fail fast if a script starts
	 * with a {@code DROP} statement.
	 *
	 * @param ignoreFailedDrops {@literal true} if failed drop statements should be ignored.
	 */
	public void setIgnoreFailedDrops(boolean ignoreFailedDrops) {
		this.ignoreFailedDrops = ignoreFailedDrops;
	}

	/**
	 * Set the {@link DataBufferFactory} to use for {@link Resource} loading.
	 * <p/>
	 * Defaults to {@link DefaultDataBufferFactory}.
	 *
	 * @param dataBufferFactory the {@link DataBufferFactory} to use, must not be {@literal null}.
	 */
	public void setDataBufferFactory(DataBufferFactory dataBufferFactory) {

		Assert.notNull(dataBufferFactory, "DataBufferFactory must not be null!");

		this.dataBufferFactory = dataBufferFactory;
	}

	@Override
	public Mono<Void> populate(Connection connection) throws ScriptException {

		Assert.notNull(connection, "Connection must not be null");

		return Flux.fromIterable(this.scripts).concatMap(it -> {

			EncodedResource encodedScript = new EncodedResource(it, this.sqlScriptEncoding);

			return ScriptUtils.executeSqlScript(connection, encodedScript, this.dataBufferFactory, this.continueOnError,
					this.ignoreFailedDrops, this.commentPrefix, this.separator, this.blockCommentStartDelimiter,
					this.blockCommentEndDelimiter);
		}).then();
	}

	/**
	 * Execute this {@link ResourceDatabasePopulator} against the given {@link ConnectionFactory}.
	 * <p/>
	 * Delegates to {@link DatabasePopulatorUtils#execute}.
	 *
	 * @param connectionFactory the {@link ConnectionFactory} to execute against, must not be {@literal null}..
	 * @return {@link Mono} tthat initiates script execution and is notified upon completion.
	 * @throws ScriptException if an error occurs.
	 * @see #populate(Connection)
	 */
	public Mono<Void> execute(ConnectionFactory connectionFactory) throws ScriptException {
		return DatabasePopulatorUtils.execute(this, connectionFactory);
	}
}
