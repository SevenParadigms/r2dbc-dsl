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

import com.fasterxml.jackson.databind.JsonNode;
import io.r2dbc.postgresql.api.Notification;
import io.r2dbc.postgresql.api.PostgresqlConnection;
import io.r2dbc.postgresql.api.PostgresqlResult;
import io.r2dbc.spi.ConnectionFactory;
import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.reactivestreams.Publisher;
import org.springframework.context.ApplicationContext;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.annotation.CreatedBy;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.annotation.Version;
import org.springframework.data.domain.Example;
import org.springframework.data.domain.Sort;
import org.springframework.data.r2dbc.config.R2dbcDslProperties;
import org.springframework.data.r2dbc.convert.R2dbcConverter;
import org.springframework.data.r2dbc.core.R2dbcEntityOperations;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.r2dbc.core.ReactiveDataAccessStrategy;
import org.springframework.data.r2dbc.core.StatementMapper;
import org.springframework.data.r2dbc.dialect.DialectResolver;
import org.springframework.data.r2dbc.mapping.OutboundRow;
import org.springframework.data.r2dbc.query.CustomUpdateMapper;
import org.springframework.data.r2dbc.repository.R2dbcRepository;
import org.springframework.data.r2dbc.repository.cache.AbstractRepositoryCache;
import org.springframework.data.r2dbc.repository.cache.CacheApi;
import org.springframework.data.r2dbc.repository.query.*;
import org.springframework.data.r2dbc.repository.security.AuthenticationIdentifierResolver;
import org.springframework.data.r2dbc.support.*;
import org.springframework.data.relational.core.dialect.RenderContextFactory;
import org.springframework.data.relational.core.query.Criteria;
import org.springframework.data.relational.core.query.Query;
import org.springframework.data.relational.core.query.Update;
import org.springframework.data.relational.core.sql.*;
import org.springframework.data.relational.repository.query.RelationalEntityInformation;
import org.springframework.data.relational.repository.query.RelationalExampleMapper;
import org.springframework.data.repository.reactive.ReactiveSortingRepository;
import org.springframework.data.util.Streamable;
import org.springframework.lang.Nullable;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.r2dbc.core.PreparedOperation;
import org.springframework.r2dbc.core.binding.Bindings;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Field;
import java.time.temporal.Temporal;
import java.util.*;
import java.util.stream.Collectors;

import static org.springframework.data.r2dbc.support.DslUtils.*;

/**
 * Simple {@link ReactiveSortingRepository} implementation using R2DBC through {@link DatabaseClient}.
 *
 * @author Mark Paluch
 * @author Jens Schauder
 * @author Mingyuan Wu
 * @author Stephen Cohen
 * @author Greg Turnquist
 * @author Lao Tsing
 */
@Transactional(readOnly = true)
public class SimpleR2dbcRepository<T, ID> extends AbstractRepositoryCache<T, ID> implements R2dbcRepository<T, ID>, CacheApi<T, ID> {
    @Nullable
    private final RelationalEntityInformation<T, ID> entity;
    private final R2dbcEntityOperations entityOperations;
    private final RelationalExampleMapper exampleMapper;
    private final R2dbcConverter converter;
    private final org.springframework.data.r2dbc.core.DatabaseClient databaseClient;
    private ApplicationContext applicationContext;

    /**
     * Create a new {@link SimpleR2dbcRepository}.
     *
     * @param entity
     * @param entityOperations
     * @param converter
     * @since 1.1
     */
    public SimpleR2dbcRepository(@Nullable RelationalEntityInformation<T, ID> entity, R2dbcEntityOperations entityOperations,
                                 R2dbcConverter converter, ApplicationContext applicationContext) {
        super(entity, applicationContext);
        this.entity = entity;
        this.entityOperations = entityOperations;
        this.exampleMapper = new RelationalExampleMapper(converter.getMappingContext());
        this.converter = converter;
        this.databaseClient = org.springframework.data.r2dbc.core.DatabaseClient.create(entityOperations.getDatabaseClient().getConnectionFactory());
        this.applicationContext = applicationContext;
        var dslProperties = Beans.of(R2dbcDslProperties.class);
        if (entity != null && dslProperties.getSecondCache() &&
                (dslProperties.getListener().isEmpty() || dslProperties.getListener().contains(entity.getJavaType().getSimpleName()))) {
            listener().doOnNext(notification -> evictAll()).subscribe();
        }
    }

    /**
     * Create a new {@link SimpleR2dbcRepository}.
     *
     * @param entity
     * @param databaseClient
     * @param converter
     * @param accessStrategy
     * @since 1.2
     */
    public SimpleR2dbcRepository(RelationalEntityInformation<T, ID> entity, DatabaseClient databaseClient,
                                 R2dbcConverter converter, ReactiveDataAccessStrategy accessStrategy, ApplicationContext applicationContext) {
        super(entity, applicationContext);
        this.entity = entity;
        this.entityOperations = new R2dbcEntityTemplate(databaseClient, accessStrategy);
        this.exampleMapper = new RelationalExampleMapper(converter.getMappingContext());
        this.converter = converter;
        this.databaseClient = org.springframework.data.r2dbc.core.DatabaseClient.create(databaseClient.getConnectionFactory());
        this.applicationContext = applicationContext;
        if (Beans.of(R2dbcDslProperties.class).getSecondCache()) {
            listener().doOnNext(notification -> evictAll()).subscribe();
        }
    }

    /**
     * Create a new {@link SimpleR2dbcRepository}.
     *
     * @param entity
     * @param databaseClient
     * @param converter
     * @param accessStrategy
     * @deprecated since 1.2.
     */
    @Deprecated
    public SimpleR2dbcRepository(RelationalEntityInformation<T, ID> entity,
                                 org.springframework.data.r2dbc.core.DatabaseClient databaseClient, R2dbcConverter converter,
                                 ReactiveDataAccessStrategy accessStrategy, ApplicationContext applicationContext) {
        super(entity, applicationContext);
        this.entity = entity;
        this.entityOperations = new R2dbcEntityTemplate(databaseClient, accessStrategy);
        this.exampleMapper = new RelationalExampleMapper(converter.getMappingContext());
        this.converter = converter;
        this.databaseClient = databaseClient;
        this.applicationContext = applicationContext;
        if (Beans.of(R2dbcDslProperties.class).getSecondCache()) {
            listener().doOnNext(notification -> evictAll()).subscribe();
        }
    }

    // -------------------------------------------------------------------------
    // Methods from ReactiveCrudRepository
    // -------------------------------------------------------------------------

    /*
     * (non-Javadoc)
     * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#save(S)
     */
    @Override
    @Transactional
    public <S extends T> Mono<S> save(S objectToSave) {
        final var dslProperties = Beans.of(R2dbcDslProperties.class);
        Assert.notNull(objectToSave, "Object to save must not be null!");

        final var versionFields = getFields(objectToSave, Fields.version, Version.class);
        versionFields.addAll(getPropertyFields(objectToSave, dslProperties.getVersion()));

        final var nowStampFields = getFields(objectToSave, Fields.updatedAt, LastModifiedDate.class);
        nowStampFields.addAll(getPropertyFields(objectToSave, dslProperties.getUpdatedAt()));

        String idPropertyName = getIdColumnName();
        Object idValue = FastMethodInvoker.getValue(objectToSave, idPropertyName);
        final var resolver = Beans.of(AuthenticationIdentifierResolver.class, null);
        if (idValue == null) {
            for (Field version : versionFields) {
                setVersion(objectToSave, version, 0);
            }
            nowStampFields.addAll(getFields(objectToSave, Fields.createdAt, CreatedDate.class));
            nowStampFields.addAll(getPropertyFields(objectToSave, dslProperties.getCreatedAt()));
            for (Field field : nowStampFields) {
                setNowStamp(objectToSave, field);
            }
            if (resolver != null) {
                final var createdBy = getFields(objectToSave, Fields.createdBy, CreatedBy.class);
                createdBy.addAll(getPropertyFields(objectToSave, dslProperties.getCreatedBy()));
                return resolver.resolve().flatMap(userId -> {
                    for (Field field : createdBy) {
                        FastMethodInvoker.setValue(objectToSave, field.getName(), userId);
                    }
                    return createEntity(objectToSave);
                });
            }
            return createEntity(objectToSave);
        } else {
            final var readOnlyFields = getFields(objectToSave,
                    Arrays.asList(Fields.createdAt, Fields.createdBy), ReadOnly.class, CreatedDate.class, CreatedBy.class);
            readOnlyFields.addAll(getPropertyFields(objectToSave, dslProperties.getReadOnly()));
            readOnlyFields.addAll(getPropertyFields(objectToSave, dslProperties.getCreatedAt()));

            final var equalityFields = FastMethodInvoker.getFieldsByAnnotation(objectToSave.getClass(), Equality.class);
            equalityFields.addAll(getPropertyFields(objectToSave, dslProperties.getEquality()));

            if (!versionFields.isEmpty() || !nowStampFields.isEmpty() || !readOnlyFields.isEmpty() || !equalityFields.isEmpty()) {
                return findOne(Dsl.create().equals(idPropertyName, ConvertUtils.convert(idValue)))
                        .flatMap(previous -> {
                            for (Field version : versionFields) {
                                var versionValue = FastMethodInvoker.getValue(objectToSave, version.getName());
                                if (versionValue == null) {
                                    evict(Dsl.create().equals(idPropertyName, ConvertUtils.convert(idValue)));
                                    return Mono.error(new IllegalArgumentException("Version field " + version.getName() + " is not set"));
                                }
                                var previousVersionValue = FastMethodInvoker.getValue(previous, version.getName());
                                if (versionValue instanceof Temporal && previousVersionValue instanceof Temporal) {
                                    if (!DslUtils.compareDateTime(versionValue, previousVersionValue)) {
                                        return Mono.error(new OptimisticLockingFailureException("Incorrect version"));
                                    }
                                } else if (!Objects.equals(versionValue, previousVersionValue)) {
                                    return Mono.error(new OptimisticLockingFailureException("Incorrect version"));
                                }
                                setVersion(objectToSave, version, versionValue);
                            }
                            for (Field field : nowStampFields) {
                                setNowStamp(objectToSave, field);
                            }
                            for (Field field : readOnlyFields) {
                                var previousValue = FastMethodInvoker.getValue(previous, field.getName());
                                if (previousValue != null) {
                                    FastMethodInvoker.setValue(objectToSave, field.getName(), previousValue);
                                }
                            }
                            for (Field field : equalityFields) {
                                var previousValue = FastMethodInvoker.getValue(previous, field.getName());
                                var value = FastMethodInvoker.getValue(objectToSave, field.getName());
                                if (!Objects.equals(value, previousValue)) {
                                    evict(Dsl.create().equals(idPropertyName, ConvertUtils.convert(idValue)));
                                    return Mono.error(new IllegalArgumentException("Field " + field.getName() + " has different values"));
                                }
                            }
                            if (resolver != null) {
                                return updateEntity(objectToSave, idPropertyName, idValue, dslProperties);
                            }
                            evictAll().cache().put(objectToSave);
                            return simpleSave(idPropertyName, idValue, objectToSave);
                        })
                        .switchIfEmpty(Mono.error(new EmptyResultDataAccessException(1)));
            }
            if (resolver != null) {
                return updateEntity(objectToSave, idPropertyName, idValue, dslProperties);
            }
            evictAll().cache().put(objectToSave);
            return simpleSave(idPropertyName, idValue, objectToSave);
        }
    }

    private <S extends T> Mono<S> simpleSave(String idPropertyName, Object idValue, S objectToSave) {
        ReactiveDataAccessStrategy accessStrategy = entityOperations.getDataAccessStrategy();
        StatementMapper mapper = accessStrategy.getStatementMapper();
        OutboundRow columns = accessStrategy.getOutboundRow(objectToSave);
        Update update = null;
        for (SqlIdentifier column : columns.keySet()) {
            if (update == null) {
                update = Update.update(accessStrategy.toSql(SqlIdentifier.quoted(column.getReference())), columns.get(column));
            }
            update = update.set(accessStrategy.toSql(SqlIdentifier.quoted(column.getReference())), columns.get(column));
        }
        assert update != null;
        PreparedOperation<?> operation = mapper.getMappedObject(
                mapper.createUpdate(this.entity.getTableName(), update)
                        .withCriteria(Criteria.where(idPropertyName).is(idValue)));
        return databaseClient.execute(operation).fetch().rowsUpdated()
                .handle((rowsUpdated, sSynchronousSink) -> {
                    if (rowsUpdated > 0) {
                        sSynchronousSink.next(objectToSave);
                    }
                });
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#saveAll(java.lang.Iterable)
     */
    @Override
    @Transactional
    public <S extends T> Flux<S> saveAll(Iterable<S> objectsToSave) {

        Assert.notNull(objectsToSave, "Objects to save must not be null!");

        return Flux.fromIterable(objectsToSave).concatMap(this::save);
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#saveAll(org.reactivestreams.Publisher)
     */
    @Override
    @Transactional
    public <S extends T> Flux<S> saveAll(Publisher<S> objectsToSave) {

        Assert.notNull(objectsToSave, "Object publisher must not be null!");

        return Flux.from(objectsToSave).concatMap(this::save);
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#findById(java.lang.Object)
     */
    @Override
    public Mono<T> findById(ID id) {

        Assert.notNull(id, "Id must not be null!");

        return getMono(Dsl.create().id(id), this.entityOperations.selectOne(getIdQuery(id), this.entity.getJavaType()));
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#findById(org.reactivestreams.Publisher)
     */
    @Override
    public Mono<T> findById(Publisher<ID> publisher) {
        return Mono.from(publisher).flatMap(this::findById);
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#existsById(java.lang.Object)
     */
    @Override
    public Mono<Boolean> existsById(ID id) {

        Assert.notNull(id, "Id must not be null!");
        if (containsMono(Dsl.create().id(id))) return Mono.just(true);
        return this.entityOperations.exists(getIdQuery(id), this.entity.getJavaType());
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#existsById(org.reactivestreams.Publisher)
     */
    @Override
    public Mono<Boolean> existsById(Publisher<ID> publisher) {
        return Mono.from(publisher).flatMap(this::existsById);
    }

    @Override
    public Mono<T> findOne(Dsl dsl) {
        return getMono(dsl, databaseClient.execute(getMappedObject(dsl)).as(entity.getJavaType()).fetch().one());
    }

    @Override
    public Mono<Integer> delete(Dsl dsl) {
        evictAll();
        return databaseClient.execute(getDeleteMappedObject(dsl)).as(entity.getJavaType()).fetch().rowsUpdated();
    }

    @Override
    public Mono<Long> count(Dsl dsl) {
        var sql = StringUtils.EMPTY;
        if (dsl.getQuery().contains("@@")) {
            var whereFields = "*";
            var parts = DslUtils.getFtsPair(dsl, entity.getJavaType());
            var queryFields = new HashSet<>(DslUtils.getCriteriaFields(dsl));
            var mutableList = getSqlNames(queryFields);
            if (!mutableList.isEmpty()) {
                whereFields = String.join(Dsl.COMMA, mutableList);
            }
            sql = "SELECT count(s.*) FROM (SELECT " + whereFields +
                    " FROM " + entity.getTableName().getReference() + ", to_tsquery('" + getLanguage(dsl) + "', '" + parts.component2() + "') AS q" +
                    " WHERE (" + parts.component1() + " @@ q)) AS s ";
        } else
            sql = "SELECT count(*) FROM " + entity.getTableName().getReference() + " AS s ";
        return databaseClient.execute(sql + getBindingCriteria(dsl)).as(Long.class).fetch().one();
    }

    @Override
    public CacheApi<T, ID> cache() {
        return this;
    }

    @Override
    public R2dbcRepository<T, ID> evict(Dsl dsl) {
        evictMono(dsl);
        evictFlux(dsl);
        return this;
    }

    @Override
    public R2dbcRepository<T, ID> evict(@Nullable ID id) {
        if (ObjectUtils.isNotEmpty(id)) {
            evictMono(Dsl.create().id(id));
            evictFlux(Dsl.create().id(id));
        }
        return this;
    }

    @Override
    public R2dbcRepository<T, ID> evictAll() {
        getCache().clear();
        return this;
    }

    @Override
    public R2dbcRepository<T, ID> put(Dsl dsl, List<T> value) {
        putFlux(dsl, value);
        return this;
    }

    @Override
    public R2dbcRepository<T, ID> put(@Nullable T value) {
        if (ObjectUtils.isNotEmpty(value)) {
            var id = (ID) FastMethodInvoker.getValue(value, getIdColumnName());
            putMono(Dsl.create().id(id), value);
        }
        return this;
    }

    @Override
    public Flux<Notification> listener() {
        ConnectionFactory connectionFactory = entityOperations.getDatabaseClient().getConnectionFactory();
        return Mono.from(connectionFactory.create()).flatMapMany(connection -> {
            String tableName = entityOperations.getDataAccessStrategy().toSql(this.entity.getTableName()).toLowerCase();
            PostgresqlConnection postgresqlConnection = (PostgresqlConnection) connection;
            return postgresqlConnection.createStatement("LISTEN " + tableName)
                    .execute()
                    .flatMap(PostgresqlResult::getRowsUpdated)
                    .thenMany(postgresqlConnection.getNotifications());
        });
    }

    private List<String> getSqlFields(Set<String> fields) {
        var mutableList = new ArrayList<String>();
        var columns = entityOperations.getDataAccessStrategy().getAllColumns(entity.getJavaType())
                .stream().map(SqlIdentifier::getReference).collect(Collectors.toList());
        for (String field : fields) {
            if (field.contains(DOT)) {
                String fieldName = field.split(DOT_REGEX)[0];
                if (columns.contains(WordUtils.camelToSql(fieldName))) {
                    mutableList.add(DslUtils.toJsonbPath(field, entity.getJavaType()) + " as " + WordUtils.lastOctet(field));
                    continue;
                }
            }
            mutableList.add(WordUtils.camelToSql(field));
        }
        return mutableList;
    }

    private List<String> getSqlNames(Set<String> fields) {
        var mutableList = new ArrayList<String>();
        var columns = entityOperations.getDataAccessStrategy().getAllColumns(entity.getJavaType())
                .stream().map(SqlIdentifier::getReference).collect(Collectors.toList());
        for (String field : fields) {
            if (field.contains(DOT)) {
                String fieldName = field.split(DOT_REGEX)[0];
                if (columns.contains(WordUtils.camelToSql(fieldName))) {
                    mutableList.add(fieldName);
                    continue;
                }
            }
            mutableList.add(WordUtils.camelToSql(field));
        }
        return mutableList;
    }

    public Flux<T> fullTextSearch(Dsl dsl) {
        var lang = getLanguage(dsl);
        var parts = DslUtils.getFtsPair(dsl, entity.getJavaType());
        var fields = "";
        var whereFields = "";

        if (dsl.getFields().length == 0) {
            fields = "*";
            whereFields = "*";
        } else {
            var mutableList = getSqlFields(new HashSet<>(Arrays.asList(dsl.getFields())));
            fields = String.join(Dsl.COMMA, mutableList);

            var queryFields = new HashSet<>(DslUtils.getCriteriaFields(dsl));
            queryFields.addAll(Arrays.asList(dsl.getFields()));
            mutableList = getSqlFields(queryFields);
            whereFields = String.join(Dsl.COMMA, mutableList);
        }
        assert parts != null;
        if (!fields.equals("*")) {
            if (!fields.contains(parts.component1()) && !parts.component1().contains("->>")) {
                whereFields += Dsl.COMMA + parts.component1();
            }
        }
        var sql = "SELECT " + fields + " FROM (SELECT " + whereFields +
                " FROM " + entity.getTableName().getReference() + ", to_tsquery('" + lang + "', '" + parts.component2() + "') AS q" +
                " WHERE (" + parts.component1() + " @@ q)) AS s " + getBindingCriteria(dsl) +
                " ORDER BY ts_rank_cd(s." + parts.component1() + ", to_tsquery('" + lang + "', '" + parts.component2() + "')) DESC ";
        if (dsl.isPaged()) {
            sql += "LIMIT " + dsl.getSize() + " OFFSET " + (dsl.getSize() * dsl.getPage());
        }
        return databaseClient.execute(sql).as(entity.getJavaType()).fetch().all();
    }

    @Override
    public Flux<T> findAll(Dsl dsl) {
        if (dsl.getQuery().contains("@@")) return getFlux(dsl, fullTextSearch(dsl));
        return getFlux(dsl, databaseClient.execute(getMappedObject(dsl)).as(entity.getJavaType()).fetch().all());
    }

    @Override
    public Mono<MementoPage<T>> findAllPaged(Dsl dsl) {
        if (containsFlux(dsl)) {
            return count(dsl).map(totalElements -> new MementoPage<>(
                    new MementoPage.MementoPageRequest(dsl, totalElements), getList(Flux.class, dsl)
            ));
        } else {
            Mono<List<T>> result;
            if (dsl.getQuery().contains("@@"))
                result = fullTextSearch(dsl).collectList();
            else
                result = databaseClient.execute(getMappedObject(dsl)).as(entity.getJavaType()).fetch().all().collectList();
            return result.flatMap(content -> {
                putFlux(dsl.pageable(dsl.getPage() < 0 ? 0 : dsl.getPage(), dsl.getSize() < 0 ? 20 : dsl.getSize()), content);
                return count(dsl).map(totalElements -> new MementoPage<>(
                        new MementoPage.MementoPageRequest(dsl, totalElements), content));
            });
        }
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#findAll()
     */
    @Override
    public Flux<T> findAll() {
        return getFlux(Dsl.create(), this.entityOperations.select(Query.empty(), this.entity.getJavaType()));
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#findAllById(java.lang.Iterable)
     */
    @Override
    public Flux<T> findAllById(Iterable<ID> iterable) {

        Assert.notNull(iterable, "The iterable of Id's must not be null!");

        return findAllById(Flux.fromIterable(iterable));
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#findAllById(org.reactivestreams.Publisher)
     */
    @Override
    public Flux<T> findAllById(Publisher<ID> idPublisher) {

        Assert.notNull(idPublisher, "The Id Publisher must not be null!");

        return Flux.from(idPublisher).buffer().filter(ids -> !ids.isEmpty()).concatMap(ids -> {

            if (ids.isEmpty()) {
                return Flux.empty();
            }

            String idProperty = getIdColumnName();

            return this.entityOperations.select(Query.query(Criteria.where(idProperty).in(ids)), this.entity.getJavaType());
        });
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#count()
     */
    @Override
    public Mono<Long> count() {
        return this.entityOperations.count(Query.empty(), this.entity.getJavaType());
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#deleteById(java.lang.Object)
     */
    @Override
    @Transactional
    public Mono<Void> deleteById(ID id) {

        Assert.notNull(id, "Id must not be null!");
        evictAll();
        return this.entityOperations.delete(getIdQuery(id), this.entity.getJavaType()).then();
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#deleteById(org.reactivestreams.Publisher)
     */
    @Override
    @Transactional
    public Mono<Void> deleteById(Publisher<ID> idPublisher) {

        Assert.notNull(idPublisher, "The Id Publisher must not be null!");
        evictAll();
        return Flux.from(idPublisher).buffer().filter(ids -> !ids.isEmpty()).concatMap(ids -> {

            if (ids.isEmpty()) {
                return Flux.empty();
            }

            String idProperty = getIdColumnName();

            return this.entityOperations.delete(Query.query(Criteria.where(idProperty).in(ids)), this.entity.getJavaType());
        }).then();
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#delete(java.lang.Object)
     */
    @Override
    @Transactional
    public Mono<Void> delete(T objectToDelete) {
        Assert.notNull(objectToDelete, "Object to delete must not be null!");
        evictAll();
        return deleteById((ID) Objects.requireNonNull(FastMethodInvoker.getValue(objectToDelete, getIdColumnName())));
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#deleteAllById(java.lang.Iterable)
     */
    @Override
    public Mono<Void> deleteAllById(Iterable<? extends ID> ids) {

        Assert.notNull(ids, "The iterable of Id's must not be null!");
        evictAll();
        List<? extends ID> idsList = Streamable.of(ids).toList();
        String idProperty = getIdColumnName();
        return this.entityOperations.delete(Query.query(Criteria.where(idProperty).in(idsList)), this.entity.getJavaType())
                .then();
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#deleteAll(java.lang.Iterable)
     */
    @Override
    @Transactional
    public Mono<Void> deleteAll(Iterable<? extends T> iterable) {

        Assert.notNull(iterable, "The iterable of Id's must not be null!");
        evictAll();
        return deleteAll(Flux.fromIterable(iterable));
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#deleteAll(org.reactivestreams.Publisher)
     */
    @Override
    @Transactional
    public Mono<Void> deleteAll(Publisher<? extends T> objectPublisher) {

        Assert.notNull(objectPublisher, "The Object Publisher must not be null!");
        evictAll();
        Flux<ID> idPublisher = Flux.from(objectPublisher).map(p -> (ID) FastMethodInvoker.getValue(p, getIdColumnName()));

        return deleteById(idPublisher);
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#deleteAll()
     */
    @Override
    @Transactional
    public Mono<Void> deleteAll() {
        evictAll();
        return this.entityOperations.delete(Query.empty(), this.entity.getJavaType()).then();
    }

    // -------------------------------------------------------------------------
    // Methods from ReactiveSortingRepository
    // -------------------------------------------------------------------------

    /*
     * (non-Javadoc)
     * @see org.springframework.data.repository.reactive.ReactiveSortingRepository#findAll(org.springframework.data.domain.Sort)
     */
    @Override
    public Flux<T> findAll(Sort sort) {

        Assert.notNull(sort, "Sort must not be null!");
        evictAll();
        return this.entityOperations.select(Query.empty().sort(sort), this.entity.getJavaType());
    }

    // -------------------------------------------------------------------------
    // Methods from ReactiveQueryByExampleExecutor
    // -------------------------------------------------------------------------

    @Override
    public <S extends T> Mono<S> findOne(Example<S> example) {

        Assert.notNull(example, "Example must not be null!");

        Query query = this.exampleMapper.getMappedExample(example);

        return this.entityOperations.selectOne(query, example.getProbeType());
    }

    @Override
    public <S extends T> Flux<S> findAll(Example<S> example) {

        Assert.notNull(example, "Example must not be null!");

        return findAll(example, Sort.unsorted());
    }

    @Override
    public <S extends T> Flux<S> findAll(Example<S> example, Sort sort) {

        Assert.notNull(example, "Example must not be null!");
        Assert.notNull(sort, "Sort must not be null!");

        Query query = this.exampleMapper.getMappedExample(example).sort(sort);

        return this.entityOperations.select(query, example.getProbeType());
    }

    @Override
    public <S extends T> Mono<Long> count(Example<S> example) {

        Assert.notNull(example, "Example must not be null!");

        Query query = this.exampleMapper.getMappedExample(example);

        return this.entityOperations.count(query, example.getProbeType());
    }

    @Override
    public <S extends T> Mono<Boolean> exists(Example<S> example) {

        Assert.notNull(example, "Example must not be null!");

        Query query = this.exampleMapper.getMappedExample(example);

        return this.entityOperations.exists(query, example.getProbeType());
    }

    private String getIdColumnName() {
        if (entityOperations.getDataAccessStrategy().getAllColumns(entity.getJavaType()).stream().anyMatch(i -> i.getReference().equals(SqlField.id)))
            return SqlField.id;
        else
            return entityOperations.getDataAccessStrategy().toSql(
                    converter
                            .getMappingContext()
                            .getRequiredPersistentEntity(entity.getJavaType())
                            .getRequiredIdProperty()
                            .getColumnName()
            );
    }

    private Query getIdQuery(Object id) {
        return Query.query(Criteria.where(getIdColumnName()).is(id));
    }

    private String getLanguage(Dsl dsl) {
        final var dslProperties = Beans.of(R2dbcDslProperties.class);
        var lang = dslProperties.getFtsLang().isEmpty() ? dsl.getLang() : dslProperties.getFtsLang();
        if (ObjectUtils.isEmpty(lang)) {
            lang = Locale.getDefault().getDisplayLanguage(Locale.ENGLISH);
            if (ObjectUtils.isEmpty(lang)) lang = "English";
        }
        return lang;
    }

    private String getBindingCriteria(Dsl dsl) {
        var operation = getMappedObject(dsl);
        var criteria = operation.get().replaceAll(entity.getTableName().getReference(), "s");
        if (criteria.indexOf("WHERE") > 0) {
            int lastIndex = criteria.indexOf("FOR UPDATE") - 1;
            if (criteria.indexOf("LIMIT") > 0) lastIndex = criteria.indexOf("LIMIT") - 1;
            if (criteria.indexOf("ORDER") > 0) lastIndex = criteria.indexOf("ORDER") - 1;
            criteria = criteria.substring(criteria.indexOf("WHERE") - 1, lastIndex);
            var index = 1;
            for (Bindings.Binding bind : operation.getBindings()) {
                var bindValue = DslUtils.objectToSql(bind.getValue());
                criteria = criteria.replaceAll("(\\$" + index + " )", bindValue + " ");
                criteria = criteria.replaceAll("(\\$" + index + "$)", bindValue);
                criteria = criteria.replaceAll("(\\$" + index + "\\))", bindValue + ")");
                criteria = criteria.replaceAll("(\\$" + index + ",)", bindValue + ",");
                index++;
            }
            return criteria;
        }
        return StringUtils.EMPTY;
    }

    private DslPreparedOperation<Delete> getDeleteMappedObject(Dsl dsl) {
        var dialect = DialectResolver.getDialect(databaseClient.getConnectionFactory());
        ReactiveDataAccessStrategy accessStrategy = entityOperations.getDataAccessStrategy();
        var table = Table.create(accessStrategy.toSql(this.entity.getTableName()));
        var queryFields = DslUtils.getCriteriaFields(dsl);
        var joins = new HashMap<String, Table>();
        var jsonNodeFields = new ArrayList<String>();
        DeleteBuilder.DeleteWhere deleteBuilder = StatementBuilder.delete(table);
        if (!queryFields.isEmpty()) {
            for (String field : queryFields) {
                if (field.contains(DOT)) {
                    String tableField = WordUtils.camelToSql(field).split(DOT_REGEX)[0];
                    Field entityField = ReflectionUtils.findField(entity.getJavaType(), tableField);
                    if (entityField != null && entityField.getType() == JsonNode.class) {
                        jsonNodeFields.add(field);
                    }
                }
            }
        }
        joins.put(StringUtils.EMPTY, table);
        var bindings = Bindings.empty();
        var updateMapper = new CustomUpdateMapper(dialect, converter);
        var bindMarkers = dialect.getBindMarkersFactory().create();
        org.springframework.data.r2dbc.query.Criteria criteria = DslUtils.getCriteriaBy(dsl, entity.getJavaType(), jsonNodeFields);
        if (ObjectUtils.isNotEmpty(criteria)) {
            var mappedObject = updateMapper.getMappedObject(bindMarkers, criteria, joins);
            bindings = mappedObject.getBindings();
            deleteBuilder.where(mappedObject.getCondition());
        }
        return new DslPreparedOperation<>(
                deleteBuilder.build(),
                new RenderContextFactory(dialect).createRenderContext(), bindings);
    }

    private DslPreparedOperation<Select> getMappedObject(final Dsl dsl) {
        var dialect = DialectResolver.getDialect(databaseClient.getConnectionFactory());
        ReactiveDataAccessStrategy accessStrategy = entityOperations.getDataAccessStrategy();
        var table = Table.create(accessStrategy.toSql(this.entity.getTableName()));
        var joins = new HashMap<String, Table>();
        var entityColumns = accessStrategy.getAllColumns(entity.getJavaType()).stream().map(accessStrategy::toSql).collect(Collectors.toList());
        var queryFields = DslUtils.getCriteriaFields(dsl);
        var jsonNodeFields = new ArrayList<String>();
        if (!queryFields.isEmpty()) {
            for (String field : queryFields) {
                if (!joins.containsKey(field) && field.contains(DOT)) {
                    var split = WordUtils.camelToSql(field).split(DOT_REGEX);
                    String tableField = split[0].replaceAll(PREFIX, "");
                    Field entityField = ReflectionUtils.findField(entity.getJavaType(), tableField);
                    if (entityField != null && entityField.getType() == JsonNode.class || split.length > 2) {
                        jsonNodeFields.add(field.replaceAll(PREFIX, ""));
                    }
                    if (entityColumns.contains(tableField + "_" + SqlField.id)) {
                        joins.put(tableField, Table.create(tableField));
                    }
                }
            }
        }
        var columns = new ArrayList<Column>();
        var resultFields = dsl.getResultFields();
        if (resultFields.isEmpty()) {
            resultFields = entityColumns;
        }
        for (var fieldName : resultFields) {
            var sqlFieldName = WordUtils.camelToSql(fieldName.trim());
            if (entityColumns.contains(sqlFieldName)) {
                columns.add(Column.create(sqlFieldName, table));
            } else {
                if (sqlFieldName.contains(DOT)) {
                    var parts = sqlFieldName.split(DOT_REGEX);
                    var tableName = parts[0];
                    if (entityColumns.contains(tableName + "_" + SqlField.id)) {
                        if (!joins.containsKey(tableName)) {
                            joins.put(tableName, Table.create(tableName));
                        }
                        columns.add(Column.create(parts[1], joins.get(tableName)));
                        continue;
                    }
                    if (entityColumns.contains(tableName)) {
                        var jsonPath = toJsonbPath(sqlFieldName);
                        columns.add(Column.create(jsonPath + " as " + getJsonName(jsonPath), table));
                        continue;
                    }
                }
                throw new IllegalArgumentException("Field " + fieldName + " not found");
            }
        }
        var selectBuilder = new CustomSelectBuilder().select(columns).from(table);
        if (dsl.getDistinct()) {
            selectBuilder.distinct();
        }
        if (dsl.getTop() > 0) {
            selectBuilder.top(dsl.getTop());
        }
        for (var joinKey : joins.keySet()) {
            selectBuilder.join(
                    new CustomSelectBuilder.JoinBuilder(joins.get(joinKey), selectBuilder)
                            .on(Column.create(joinKey + "_" + SqlField.id, table)).equals(Column.create(SqlField.id, joins.get(joinKey)))
                            .finishJoin()
            );
        }
        joins.put(StringUtils.EMPTY, table);
        var updateMapper = new CustomUpdateMapper(dialect, converter);
        var bindMarkers = dialect.getBindMarkersFactory().create();
        var bindings = Bindings.empty();
        org.springframework.data.r2dbc.query.Criteria criteria = DslUtils.getCriteriaBy(dsl, entity.getJavaType(), jsonNodeFields);
        if (criteria != null) {
            var mappedObject = updateMapper.getMappedObject(bindMarkers, criteria, joins);
            bindings = mappedObject.getBindings();
            selectBuilder.where(mappedObject.getCondition());
        }
        if (dsl.isSorted()) {
            var mappedSort = updateMapper.getMappedObject(DslUtils.getSorted(dsl), null);
            var fields = new ArrayList<OrderByField>();
            for (var order : mappedSort) {
                var orderByField = OrderByField.from(table.column(order.getProperty()));
                if (order.isAscending()) fields.add(orderByField.asc());
                else fields.add(orderByField.desc());
            }
            selectBuilder.orderBy(fields);
        }
        if (dsl.isPaged()) {
            selectBuilder.limitOffset(dsl.getSize(), ((long) dsl.getSize() * dsl.getPage()));
        } else if (dsl.getSize() > 0) {
            selectBuilder.limitOffset(dsl.getSize(), 0);
        }
        return new DslPreparedOperation<>(
                selectBuilder.build(),
                new RenderContextFactory(dialect).createRenderContext(), bindings);
    }

    private <S extends T> Mono<S> createEntity(S objectToSave) {
        return databaseClient.insert()
                .into(this.entity.getJavaType())
                .table(this.entity.getTableName()).using(objectToSave)
                .map(converter.populateIdIfNecessary(objectToSave))
                .one().flatMap(updated -> {
                    evictAll().cache().put(updated);
                    return Mono.just(updated);
                });
    }

    private <S extends T> Mono<S> updateEntity(S objectToSave, String idPropertyName, Object idValue, R2dbcDslProperties dslProperties) {
        final var updatedBy = getFields(objectToSave, Fields.updatedBy, UpdatedBy.class);
        updatedBy.addAll(getPropertyFields(objectToSave, dslProperties.getUpdatedBy()));
        final var resolver = Beans.of(AuthenticationIdentifierResolver.class);
        return resolver.resolve().flatMap(userId -> {
            for (Field field : updatedBy) {
                FastMethodInvoker.setValue(objectToSave, field.getName(), userId);
            }
            evictAll().cache().put(objectToSave);
            return simpleSave(idPropertyName, idValue, objectToSave);
        });
    }
}
