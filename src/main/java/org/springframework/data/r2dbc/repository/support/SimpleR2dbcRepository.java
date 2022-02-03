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
import io.netty.util.internal.StringUtil;
import io.r2dbc.postgresql.PostgresqlConnectionFactory;
import io.r2dbc.postgresql.api.Notification;
import io.r2dbc.postgresql.api.PostgresqlConnection;
import io.r2dbc.postgresql.api.PostgresqlResult;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Result;
import org.apache.commons.beanutils.ConvertUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.reactivestreams.Publisher;
import org.springframework.context.ApplicationContext;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.data.annotation.*;
import org.springframework.data.domain.Example;
import org.springframework.data.domain.Sort;
import org.springframework.data.r2dbc.config.Beans;
import org.springframework.data.r2dbc.convert.R2dbcConverter;
import org.springframework.data.r2dbc.core.R2dbcEntityOperations;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.r2dbc.core.ReactiveDataAccessStrategy;
import org.springframework.data.r2dbc.core.StatementMapper;
import org.springframework.data.r2dbc.dialect.DialectResolver;
import org.springframework.data.r2dbc.mapping.OutboundRow;
import org.springframework.data.r2dbc.query.CustomUpdateMapper;
import org.springframework.data.r2dbc.repository.R2dbcRepository;
import org.springframework.data.r2dbc.repository.query.Dsl;
import org.springframework.data.r2dbc.repository.query.Equality;
import org.springframework.data.r2dbc.repository.query.ReadOnly;
import org.springframework.data.r2dbc.support.DslUtils;
import org.springframework.data.r2dbc.support.FastMethodInvoker;
import org.springframework.data.r2dbc.support.SqlField;
import org.springframework.data.r2dbc.support.WordUtils;
import org.springframework.data.relational.core.dialect.RenderContextFactory;
import org.springframework.data.relational.core.query.Criteria;
import org.springframework.data.relational.core.query.Query;
import org.springframework.data.relational.core.query.Update;
import org.springframework.data.relational.core.sql.*;
import org.springframework.data.relational.repository.query.RelationalEntityInformation;
import org.springframework.data.relational.repository.query.RelationalExampleMapper;
import org.springframework.data.repository.reactive.ReactiveSortingRepository;
import org.springframework.data.util.Streamable;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.r2dbc.core.PreparedOperation;
import org.springframework.r2dbc.core.binding.Bindings;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;
import org.springframework.util.ReflectionUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Field;
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
public class SimpleR2dbcRepository<T, ID> implements R2dbcRepository<T, ID> {

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
    public SimpleR2dbcRepository(RelationalEntityInformation<T, ID> entity, R2dbcEntityOperations entityOperations,
                                 R2dbcConverter converter, ApplicationContext applicationContext) {
        this.entity = entity;
        this.entityOperations = entityOperations;
        this.exampleMapper = new RelationalExampleMapper(converter.getMappingContext());
        this.converter = converter;
        this.databaseClient = org.springframework.data.r2dbc.core.DatabaseClient.create(entityOperations.getDatabaseClient().getConnectionFactory());
        this.applicationContext = applicationContext;
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
        this.entity = entity;
        this.entityOperations = new R2dbcEntityTemplate(databaseClient, accessStrategy);
        this.exampleMapper = new RelationalExampleMapper(converter.getMappingContext());
        this.converter = converter;
        this.databaseClient = org.springframework.data.r2dbc.core.DatabaseClient.create(databaseClient.getConnectionFactory());
        this.applicationContext = applicationContext;
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
        this.entity = entity;
        this.entityOperations = new R2dbcEntityTemplate(databaseClient, accessStrategy);
        this.exampleMapper = new RelationalExampleMapper(converter.getMappingContext());
        this.converter = converter;
        this.databaseClient = databaseClient;
        this.applicationContext = applicationContext;
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

        Assert.notNull(objectToSave, "Object to save must not be null!");

        String idPropertyName = getIdColumnName();
        Object idValue = FastMethodInvoker.getValue(objectToSave, idPropertyName);
        final var versionFields = getFields(objectToSave, Fields.version, Version.class);
        final var nowStampFields = nowStamp(objectToSave, Fields.updatedAt, LastModifiedDate.class);
        if (idValue == null) {
            for (Field version : versionFields) {
                setVersion(objectToSave, version, 0);
            }
            nowStamp(objectToSave, Fields.createdAt, CreatedDate.class);
            return databaseClient.insert()
                    .into(this.entity.getJavaType())
                    .table(this.entity.getTableName()).using(objectToSave)
                    .map(converter.populateIdIfNecessary(objectToSave))
                    .first().flatMap(Mono::just)
                    .defaultIfEmpty(objectToSave);
        } else {
            final var readOnlyFields = getFields(objectToSave, Fields.createdAt, ReadOnly.class, CreatedDate.class, CreatedBy.class);
            final var equalityFields = FastMethodInvoker.getFieldsByAnnotation(objectToSave.getClass(), Equality.class);
            if (!versionFields.isEmpty() || !nowStampFields.isEmpty() || !readOnlyFields.isEmpty() || !equalityFields.isEmpty()) {
                return findOne(Dsl.create().equals(idPropertyName, ConvertUtils.convert(idValue)))
                        .flatMap(previous -> {
                            for (Field version : versionFields) {
                                var versionValue = FastMethodInvoker.getValue(objectToSave, version.getName());
                                assert versionValue != null;
                                var previousVersionValue = FastMethodInvoker.getValue(previous, version.getName());
                                if (!Objects.equals(versionValue, previousVersionValue)) {
                                    return Mono.error(new OptimisticLockingFailureException("Incorrect version"));
                                }
                                setVersion(objectToSave, version, versionValue);
                            }
                            for (Field field : readOnlyFields) {
                                var previousValue = FastMethodInvoker.getValue(previous, field.getName());
                                if (previousValue != null) {
                                    FastMethodInvoker.setValue(objectToSave, field.getName(), previousValue);
                                }
                            }
                            for (Field field : equalityFields) {
                                var value = FastMethodInvoker.getValue(objectToSave, field.getName());
                                var previousValue = FastMethodInvoker.getValue(previous, field.getName());
                                if (!Objects.equals(value, previousValue)) {
                                    return Mono.error(new IllegalArgumentException("Field " + field.getName() + " has different values"));
                                }
                            }
                            return simpleSave(idPropertyName, idValue, objectToSave);
                        })
                        .switchIfEmpty(Mono.error(new EmptyResultDataAccessException(1)));
            }
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
                update = Update.update(accessStrategy.toSql(column), columns.get(column));
            }
            update = update.set(accessStrategy.toSql(column), columns.get(column));
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

        return this.entityOperations.selectOne(getIdQuery(id), this.entity.getJavaType());
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

        return this.entityOperations.exists(getIdQuery(id), this.entity.getJavaType());
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#existsById(org.reactivestreams.Publisher)
     */
    @Override
    public Mono<Boolean> existsById(Publisher<ID> publisher) {
        return Mono.from(publisher).flatMap(this::findById).hasElement();
    }

    @Override
    public Mono<T> findOne(Dsl dsl) {
        return databaseClient.execute(getMappedObject(dsl)).as(entity.getJavaType()).fetch().one();
    }

    @Override
    public Mono<Integer> delete(Dsl dsl) {
        return databaseClient.execute(getDeleteMappedObject(dsl)).as(entity.getJavaType()).fetch().rowsUpdated();
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

    public Flux<T> fullTextSearch(Dsl dsl) {
        var lang = applicationContext.getEnvironment().getProperty("spring.r2dbc.dsl.fts-lang", dsl.getLang());
        if (ObjectUtils.isEmpty(lang.isEmpty())) {
            lang = Locale.getDefault().getDisplayLanguage(Locale.ENGLISH);
        }
        var parts = DslUtils.getFtsPair(dsl, entity.getJavaType());
        var fields = "";
        
        if (dsl.getFields().length == 0)
            fields = "*";
        else {
            var mutableList = new ArrayList<String>();
            var columns = entityOperations.getDataAccessStrategy().getAllColumns(entity.getJavaType())
                    .stream().map(SqlIdentifier::getReference).collect(Collectors.toList());
            for (String field : dsl.getFields()) {
                if (field.contains(DOT)) {
                    String[] tmp = field.split(DOT_REGEX);
                    if (columns.contains(WordUtils.camelToSql(tmp[0]))) {
                        mutableList.add(DslUtils.toJsonbPath(field, entity.getJavaType()) + " as " + field);
                        continue;
                    }
                }
                mutableList.add(WordUtils.camelToSql(field));
            }
            fields = String.join(",", mutableList);
        }

        if (!fields.equals("*") && (!fields.contains(parts.component1()) && !parts.component1().contains("->>")))
            fields += "," + parts.component1();
        var sql = "SELECT * FROM (SELECT " + fields +
                " FROM " + entity.getTableName().getReference() + ", websearch_to_tsquery('" + lang + "', '" + parts.component2() + "') AS q" +
                " WHERE (" + parts.component1() + " @@ q)) AS s";
        var operation = getMappedObject(dsl);
        var criteria = operation.get().replaceAll(entity.getTableName().getReference(), "s");
        if (criteria.indexOf("WHERE") > 0) {
            int lastIndex = criteria.indexOf("FOR UPDATE") - 1;
            if (criteria.indexOf("LIMIT") > 0) lastIndex = criteria.indexOf("LIMIT") - 1;
            if (criteria.indexOf("ORDER") > 0) lastIndex = criteria.indexOf("ORDER") - 1;
            criteria = criteria.substring(criteria.indexOf("WHERE") - 1, lastIndex);
            sql += criteria;
        }
        sql += " ORDER BY ts_rank_cd(s." + parts.component1() + ", websearch_to_tsquery('" + lang + "', '" + parts.component2() + "')) DESC ";
        if (dsl.isPaged()) {
            sql += "LIMIT " + dsl.getSize() + " OFFSET " + (dsl.getSize() * dsl.getPage());
        }
        if (criteria.indexOf("WHERE") > 0) {
            var index = 1;
            for (Bindings.Binding bind : operation.getBindings()) {
                sql = sql.replaceAll("\\$" + index++, bind.getValue() == null ? "null" : DslUtils.objectToSql(bind.getValue()));
            }
        }
        return databaseClient.execute(sql).as(entity.getJavaType()).fetch().all();
    }

    @Override
    public <S> Flux<Result> saveBatch(Iterable<S> models) {
        var connectionFactory = (PostgresqlConnectionFactory) databaseClient.getConnectionFactory();
        try {
            var fields = new ArrayList<String>();
            var reflectionStorage = FastMethodInvoker.reflectionStorage(entity.getJavaType());
            for (var field : reflectionStorage) {
                if (!field.isAnnotationPresent(Id.class) && !field.getName().equals(SqlField.id)) {
                    fields.add(":".concat(field.getName()));
                }
            }
            var buildFields = String.join(",", fields);
            var template = "INSERT INTO " + entity.getTableName() + "(" + WordUtils.camelToSql(buildFields.replaceAll(":", "")) + ") " +
                    "VALUES(" + buildFields + ");";
            var query = new StringBuilder();
            for (S target : models) {
                query.append(DslUtils.binding(template, target));
            }
            return connectionFactory.create().flatMap(c -> c.createBatch().add(query.toString()).execute().collectList()).flatMapMany(Flux::fromIterable);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Flux<T> findAll(Dsl dsl) {
        if (dsl.getQuery().contains("@@")) return fullTextSearch(dsl);
        return databaseClient.execute(getMappedObject(dsl)).as(entity.getJavaType()).fetch().all();
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#findAll()
     */
    @Override
    public Flux<T> findAll() {
        return this.entityOperations.select(Query.empty(), this.entity.getJavaType());
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

        return deleteById((ID) FastMethodInvoker.getValue(objectToDelete, getIdColumnName()));
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.repository.reactive.ReactiveCrudRepository#deleteAllById(java.lang.Iterable)
     */
    @Override
    public Mono<Void> deleteAllById(Iterable<? extends ID> ids) {

        Assert.notNull(ids, "The iterable of Id's must not be null!");

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

    private DslPreparedOperation<Delete> getDeleteMappedObject(Dsl dsl) {
        if (ObjectUtils.isEmpty(applicationContext)) {
            assert Beans.getApplicationContext() != null;
            applicationContext = Beans.getApplicationContext();
        }
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
        joins.put(StringUtil.EMPTY_STRING, table);
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

    private DslPreparedOperation<Select> getMappedObject(Dsl dsl) {
        if (ObjectUtils.isEmpty(applicationContext)) {
            assert Beans.getApplicationContext() != null;
            applicationContext = Beans.getApplicationContext();
        }
        var dialect = DialectResolver.getDialect(databaseClient.getConnectionFactory());
        ReactiveDataAccessStrategy accessStrategy = entityOperations.getDataAccessStrategy();
        var table = Table.create(accessStrategy.toSql(this.entity.getTableName()));
        var joins = new HashMap<String, Table>();
        var entityColumns = accessStrategy.getAllColumns(entity.getJavaType()).stream().map(accessStrategy::toSql).collect(Collectors.toList());
        var queryFields = DslUtils.getCriteriaFields(dsl);
        var jsonNodeFields = new ArrayList<String>();
        if (!queryFields.isEmpty()) {
            for (String field : queryFields) {
                field = field.replaceAll(COMBINATORS, "");
                if (!joins.containsKey(field) && field.contains(DOT)) {
                    String tableField = WordUtils.camelToSql(field).split(DOT_REGEX)[0];
                    Field entityField = ReflectionUtils.findField(entity.getJavaType(), tableField);
                    if (entityField != null && entityField.getType() == JsonNode.class) {
                        jsonNodeFields.add(field);
                    }
                    if (entityColumns.contains(tableField + "_" + SqlField.id)) {
                        joins.put(tableField, Table.create(tableField));
                    }
                }
            }
        }
        var columns = new ArrayList<Column>();
        if (dsl.getResultFields().isEmpty()) {
            dsl.setResultFields(entityColumns);
        }
        for (var fieldName : dsl.getResultFields()) {
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
        for (var joinKey : joins.keySet()) {
            selectBuilder.join(
                    new CustomSelectBuilder.JoinBuilder(joins.get(joinKey), selectBuilder)
                            .on(Column.create(joinKey + "_" + SqlField.id, table)).equals(Column.create(SqlField.id, joins.get(joinKey)))
                            .finishJoin()
            );
        }
        joins.put(StringUtil.EMPTY_STRING, table);
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
}
