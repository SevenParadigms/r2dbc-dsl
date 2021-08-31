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

import io.netty.util.internal.StringUtil;
import io.r2dbc.postgresql.api.Notification;
import io.r2dbc.postgresql.api.PostgresqlConnection;
import io.r2dbc.postgresql.api.PostgresqlResult;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Wrapped;
import org.postgresql.ds.PGConnectionPoolDataSource;
import org.postgresql.jdbc2.optional.ConnectionPool;
import org.reactivestreams.Publisher;
import org.springframework.context.ApplicationContext;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Example;
import org.springframework.data.domain.Sort;
import org.springframework.data.r2dbc.convert.R2dbcConverter;
import org.springframework.data.r2dbc.core.R2dbcEntityOperations;
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate;
import org.springframework.data.r2dbc.core.ReactiveDataAccessStrategy;
import org.springframework.data.r2dbc.core.StatementMapper;
import org.springframework.data.r2dbc.dialect.DialectResolver;
import org.springframework.data.r2dbc.dialect.R2dbcDialect;
import org.springframework.data.r2dbc.mapping.OutboundRow;
import org.springframework.data.r2dbc.query.BoundCondition;
import org.springframework.data.r2dbc.query.CustomUpdateMapper;
import org.springframework.data.r2dbc.repository.R2dbcRepository;
import org.springframework.data.r2dbc.repository.query.Dsl;
import org.springframework.data.r2dbc.support.DslUtils;
import org.springframework.data.r2dbc.support.FastMethodInvoker;
import org.springframework.data.r2dbc.support.WordUtils;
import org.springframework.data.relational.core.dialect.RenderContextFactory;
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty;
import org.springframework.data.relational.core.query.Criteria;
import org.springframework.data.relational.core.query.Query;
import org.springframework.data.relational.core.query.Update;
import org.springframework.data.relational.core.sql.*;
import org.springframework.data.relational.repository.query.RelationalEntityInformation;
import org.springframework.data.relational.repository.query.RelationalExampleMapper;
import org.springframework.data.repository.reactive.ReactiveSortingRepository;
import org.springframework.data.util.Lazy;
import org.springframework.data.util.Streamable;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.r2dbc.core.PreparedOperation;
import org.springframework.r2dbc.core.binding.BindMarkers;
import org.springframework.r2dbc.core.binding.Bindings;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.Assert;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Field;
import java.util.*;
import java.util.stream.Collectors;

import static org.springframework.data.r2dbc.support.DslUtils.toJsonbPath;

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
    private final Lazy<RelationalPersistentProperty> idProperty;
    private final RelationalExampleMapper exampleMapper;
    private final R2dbcConverter converter;
    private final org.springframework.data.r2dbc.core.DatabaseClient databaseClient;
    private final ApplicationContext applicationContext;

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
        this.idProperty = Lazy.of(() -> Objects.requireNonNull(converter.getMappingContext()
                        .getPersistentEntity(entity.getJavaType()))
                .getPersistentProperty(getIdColumnName()));
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
        this.idProperty = Lazy.of(() -> Objects.requireNonNull(converter.getMappingContext()
                        .getPersistentEntity(entity.getJavaType()))
                .getPersistentProperty(getIdColumnName()));
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
        this.idProperty = Lazy.of(() -> Objects.requireNonNull(converter.getMappingContext()
                        .getPersistentEntity(entity.getJavaType()))
                .getPersistentProperty(getIdColumnName()));
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
        if (idValue == null) {
            return databaseClient.insert()
                    .into(this.entity.getJavaType())
                    .table(this.entity.getTableName()).using(objectToSave)
                    .map(converter.populateIdIfNecessary(objectToSave))
                    .first().flatMap(s -> Mono.just(s))
                    .defaultIfEmpty(objectToSave);
        } else {
            ReactiveDataAccessStrategy accessStrategy = entityOperations.getDataAccessStrategy();
            StatementMapper mapper = accessStrategy.getStatementMapper();
            OutboundRow columns = accessStrategy.getOutboundRow(objectToSave);
            Update update = null;
            Iterator<?> iterator = columns.keySet().iterator();
            while (iterator.hasNext()) {
                SqlIdentifier column = (SqlIdentifier) iterator.next();
                if (update == null) {
                    update = Update.update(accessStrategy.toSql(column), columns.get(column));
                }
                update = update.set(accessStrategy.toSql(column), columns.get(column));
            }
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
    public Flux<Notification> listener() {
        ConnectionFactory connectionFactory = entityOperations.getDatabaseClient().getConnectionFactory();
        return Mono.from(connectionFactory.create()).flatMapMany(connection -> {
            String tableName = entityOperations.getDataAccessStrategy().toSql(this.entity.getTableName()).toLowerCase();
            PostgresqlConnection postgresqlConnection = (PostgresqlConnection) ((Wrapped<Connection>) connection).unwrap();
            return postgresqlConnection.createStatement("LISTEN " + tableName)
                    .execute()
                    .flatMap(PostgresqlResult::getRowsUpdated)
                    .thenMany(postgresqlConnection.getNotifications());
        });
    }

    @Override
    public Flux<T> fullTextSearch(Dsl dsl) {
        var lang = applicationContext.getEnvironment().getProperty("spring.r2dbc.dsl.fts-lang", dsl.lang);
        var parts = dsl.query.split("@@");
        var fields = "";
        if (dsl.fields.length == 0)
            fields = "*";
        else {
            var mutableList = new ArrayList<String>();
            var columns = entityOperations.getDataAccessStrategy().getAllColumns(entity.getJavaType());
            for (String field : dsl.fields) {
                if (field.contains(".")) {
                    String[] tmp = field.split("\\.");
                    if (columns.contains(WordUtils.camelToSql(tmp[0]))) {
                        mutableList.add(DslUtils.toJsonbPath(field, entity.getJavaType()) + " as " + WordUtils.dotToSql(field));
                        continue;
                    }
                }
                mutableList.add(WordUtils.camelToSql(field));
            }
            fields = mutableList.stream().collect(Collectors.joining(","));
        }

        if (!fields.equals("*") && !fields.contains("tsv")) fields += ",tsv";
        var sql = "SELECT * FROM ( SELECT " + fields +
                " FROM " + entity.getTableName() + ", websearch_to_tsquery(:lang, :value) AS q" +
                " WHERE (" + parts[0] + " @@ q)) AS s" +
                " ORDER BY ts_rank_cd(s." + parts[0] + ", websearch_to_tsquery(:lang, :value))) DESC";
        return databaseClient.execute(sql)
                .bind("lang", lang)
                .bind("value", parts[1])
                .as(entity.getJavaType()).fetch().all();
    }

    @Override
    public <S> Long saveBatch(Iterable<S> models) {
        var dataSource = (PGConnectionPoolDataSource) databaseClient.getConnectionFactory();
        try {
            var fields = new ArrayList<String>();
            var reflectionStorage = FastMethodInvoker.reflectionStorage(entity.getJavaType());
            for (var field : reflectionStorage) {
                if (!field.isAnnotationPresent(Id.class) && !field.getName().equals(Dsl.idProperty)) {
                    fields.add(":".concat(field.getName()));
                }
            }
            var buildFields = fields.stream().collect(Collectors.joining(","));
            var template = "INSERT INTO " + entity.getTableName() + "(" + WordUtils.camelToSql(buildFields.replaceAll(":", "")) + ") " +
                    "VALUES(" + buildFields + ");";
            var query = new StringBuilder();
            for (S target : models) {
                query.append(DslUtils.binding(template, target));
            }
            return dataSource.getPooledConnection().getConnection().createStatement().executeLargeUpdate(query.toString());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Flux<T> findAll(Dsl dsl) {
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

            String idProperty = getIdProperty().getName();

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

            String idProperty = getIdProperty().getName();

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
        String idProperty = getIdProperty().getName();
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

    private RelationalPersistentProperty getIdProperty() {
        return this.idProperty.get();
    }

    private String getIdColumnName() {
        if (entityOperations.getDataAccessStrategy().getAllColumns(entity.getJavaType()).stream().anyMatch(i -> i.getReference().equals(Dsl.idProperty)))
            return Dsl.idProperty;
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
        return Query.query(Criteria.where(getIdProperty().getName()).is(id));
    }

    private PreparedOperation<Select> getMappedObject(Dsl dsl) {
        var connectionFactory = applicationContext.getBean(ConnectionFactory.class);
        var dialect = DialectResolver.getDialect(connectionFactory);
        ReactiveDataAccessStrategy accessStrategy = entityOperations.getDataAccessStrategy();
        var table = Table.create(accessStrategy.toSql(this.entity.getTableName()));
        var joins = new HashMap<String, Table>();
        var entityColumns = accessStrategy.getAllColumns(entity.getJavaType()).stream().map(accessStrategy::toSql).collect(Collectors.toList());
        var queryFields = dsl.getCriteriaFields();
        if (!queryFields.isEmpty()) {
            for (String field : queryFields) {
                if (!joins.containsKey(field) && field.contains(".")) {
                    String tableField = WordUtils.camelToSql(field).split(".")[0];
                    if (entityColumns.contains(tableField + "_id")) {
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
            var sqlFieldName = WordUtils.camelToSql(fieldName);
            if (entityColumns.contains(sqlFieldName)) {
                columns.add(Column.create(sqlFieldName, table));
            } else {
                if (sqlFieldName.contains(".")) {
                    var parts = sqlFieldName.split(".");
                    var tableName = parts[0];
                    if (entityColumns.contains(tableName + "_id")) {
                        if (!joins.containsKey(tableName)) {
                            joins.put(tableName, Table.create(tableName));
                        }
                        columns.add(Column.create(parts[1] + " as " + WordUtils.dotToSql(sqlFieldName), joins.get(tableName)));
                        continue;
                    }
                    if (entityColumns.contains(tableName)) {
                        columns.add(Column.create(toJsonbPath(sqlFieldName) + " as " + WordUtils.dotToSql(sqlFieldName), table));
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
                            .on(Column.create(joinKey + "_id", table)).equals(Column.create("id", joins.get(joinKey)))
                            .finishJoin()
            );
        }
        joins.put(StringUtil.EMPTY_STRING, table);
        var updateMapper = new CustomUpdateMapper(dialect, converter);
        var bindMarkers = dialect.getBindMarkersFactory().create();
        var bindings = Bindings.empty();
        org.springframework.data.r2dbc.query.Criteria criteria = dsl.getCriteriaBy(entity.getJavaType());
        if (criteria != null) {
            var mappedObject = updateMapper.getMappedObject(bindMarkers, criteria, joins);
            bindings = mappedObject.getBindings();
            selectBuilder.where(mappedObject.getCondition());
        }
        if (dsl.isSorted()) {
            var mappedSort = updateMapper.getMappedObject(dsl.getSorted(), null);
            var fields = new ArrayList<OrderByField>();
            for (var order : mappedSort) {
                var orderByField = OrderByField.from(table.column(order.getProperty()));
                if (order.isAscending()) fields.add(orderByField.asc());
                else fields.add(orderByField.desc());
            }
            selectBuilder.orderBy(fields);
        }
        if (dsl.isPaged()) {
            selectBuilder.limitOffset(dsl.size, ((long) dsl.size * dsl.page));
        }
        return new DslPreparedOperation(
                selectBuilder.build(),
                new RenderContextFactory(dialect).createRenderContext(), bindings);
    }
}
