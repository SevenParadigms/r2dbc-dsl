package com.sevenparadigms.dsl

import com.fasterxml.jackson.databind.JsonNode
import com.sevenparadigms.common.*
import io.netty.util.internal.StringUtil
import io.r2dbc.pool.ConnectionPool
import io.r2dbc.postgresql.api.Notification
import io.r2dbc.postgresql.api.PostgresqlConnection
import io.r2dbc.postgresql.api.PostgresqlResult
import io.r2dbc.spi.*
import org.apache.commons.beanutils.ConvertUtils
import org.reactivestreams.Publisher
import org.springframework.data.annotation.Id
import org.springframework.data.domain.Pageable
import org.springframework.data.domain.Sort
import org.springframework.data.mapping.PersistentPropertyAccessor
import org.springframework.data.r2dbc.convert.R2dbcConverter
import org.springframework.data.r2dbc.core.DatabaseClient
import org.springframework.data.r2dbc.core.DefaultReactiveDataAccessStrategy
import org.springframework.data.r2dbc.core.PreparedOperation
import org.springframework.data.r2dbc.core.StatementMapper
import org.springframework.data.r2dbc.dialect.BindMarkers
import org.springframework.data.r2dbc.dialect.Bindings
import org.springframework.data.r2dbc.dialect.DialectResolver
import org.springframework.data.r2dbc.mapping.SettableValue
import org.springframework.data.r2dbc.query.BoundCondition
import org.springframework.data.r2dbc.query.Criteria
import org.springframework.data.r2dbc.query.CustomUpdateMapper
import org.springframework.data.r2dbc.query.Update
import org.springframework.data.relational.core.dialect.RenderContextFactory
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty
import org.springframework.data.relational.core.sql.*
import org.springframework.data.relational.core.sql.render.SqlRenderer
import org.springframework.data.relational.repository.support.MappingRelationalEntityInformation
import org.springframework.transaction.annotation.Transactional
import org.springframework.util.ClassUtils
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.SynchronousSink
import java.io.Serializable
import java.util.*
import java.util.function.BiFunction
import kotlin.collections.ArrayList

@Suppress("UNCHECKED_CAST")
@Transactional(readOnly = true)
class R2dbcDslRepositoryImpl<T, ID : Serializable?>(private val entity: MappingRelationalEntityInformation<T, ID>,
                                                    private val databaseClient: DatabaseClient,
                                                    private val converter: R2dbcConverter,
                                                    private val accessStrategy: DefaultReactiveDataAccessStrategy) : R2dbcDslRepository<T, ID> {
    @Transactional
    override fun <S : Any> saveBatch(entities: Iterable<S>): Flux<Int> {
        val connectionPool: ConnectionPool = ApplicationContext.getBean(ConnectionFactory::class.java) as ConnectionPool
        return connectionPool.create().flatMapMany { connection ->
            val batch = connection.createBatch()
            val fields = ArrayList<String>()
            val reflectionStorage = FastMethodInvoker.reflectionStorage(entity.javaType)
            for (field in reflectionStorage) {
                if (!field.isAnnotationPresent(Id::class.java) && field.name != "id") {
                    fields.add(":${field.name}")
                }
            }
            val buildFields = fields.joinToString(separator = ",")
            val query = StringBuilder("insert into ${entity.tableName}(${buildFields.replace(":", "").deCamel()})values($buildFields)")
            for (target in entities) {
                batch.add(query.binding(target as Any))
            }
            (batch.execute() as Flux<Result>).flatMap(Result::getRowsUpdated)
        }
    }

    override fun listen(): Flux<Notification> {
        val connectionPool: ConnectionPool = ApplicationContext.getBean(ConnectionFactory::class.java) as ConnectionPool
        return connectionPool.create().flatMapMany { connection ->
            val postgresqlConnection = (connection as Wrapped<Connection>).unwrap() as PostgresqlConnection
            postgresqlConnection.createStatement("LISTEN ${entity.tableName.toLowerCase()}").execute()
                    .flatMap(PostgresqlResult::getRowsUpdated)
                    .thenMany(postgresqlConnection.notifications)
        }
    }

    override fun findAll(dsl: R2dbcDsl, pageable: Pageable): Flux<T> {
        return databaseClient.execute(getMappedObject(dsl, pageable)).`as`(entity.javaType).fetch().all()
    }

    override fun findAll(pageable: Pageable): Flux<T> {
        return findAll(R2dbcDsl.create(), pageable)
    }

    override fun findBy(dsl: R2dbcDsl): Mono<T> {
        return databaseClient.execute(getMappedObject(dsl, Pageable.unpaged())).`as`(entity.javaType).fetch().one()
    }

    override fun fts(dsl: R2dbcDsl, pageable: Pageable): Flux<T> {
        val lang = ApplicationContext.getProperty("spring.r2dbc.dsl.fts-lang", dsl.ftsLang)
        val parts = dsl.query.split("@@")
        var fields = if (dsl.fields.isEmpty()) "*" else dsl.fields.toJsonbPath().joinToString(separator = ",")
        if (!fields.contains("tsv")) fields = "$fields,tsv"
        return databaseClient.execute("""SELECT * FROM (
              SELECT $fields
              FROM ${entity.tableName}, websearch_to_tsquery('$lang', '${parts.last()}') AS q
              WHERE (${parts.first()} @@ q)
            ) AS s ORDER BY ts_rank_cd(s.${parts.first()}, websearch_to_tsquery('$lang', '${parts.last()}')) DESC """ +
                (if (pageable.isPaged) {
                    "LIMIT ${pageable.pageSize} OFFSET ${pageable.pageSize * pageable.pageNumber}"
                } else StringUtil.EMPTY_STRING)
                        .trimInline()).`as`(entity.javaType).fetch().all()
    }

    override fun oper(dsl: R2dbcDsl, pageable: Pageable): Flux<T> {
        val operator = dsl.query.replace("[^>@?]".toRegex(), "")
        val parts = dsl.query.split("(@>|@\\?|@@)".toRegex())
        val fields = if (dsl.fields.isEmpty()) "*" else dsl.fields.toJsonbPath().joinToString(separator = ",")
        return databaseClient.execute("SELECT $fields FROM ${entity.tableName} WHERE ${parts.first()} $operator '${parts.last()}' " +
                (if (pageable.sort.isSorted) {
                    "ORDER BY ${pageable.sort.toString().replace(":", "")} "
                } else StringUtil.EMPTY_STRING) +
                (if (pageable.isPaged) {
                    "LIMIT ${pageable.pageSize} OFFSET ${pageable.pageSize * pageable.pageNumber}"
                } else StringUtil.EMPTY_STRING))
                .`as`(entity.javaType).fetch().all()
    }

    @Transactional
    override fun <S : T> save(entity: S): Mono<S> {
        val idPropertyName = getIdColumnName()
        val idValue: Any? = entity!!.getValue(idPropertyName)
        return if (idValue == null) {
            databaseClient.insert()
                    .into(this.entity.javaType)
                    .table(this.entity.tableName).using(entity)
                    .map(populateIdIfNecessary(entity))
                    .first().flatMap { Mono.just(it as S) }
                    .defaultIfEmpty(entity)
        } else {
            val mapper: StatementMapper = accessStrategy.statementMapper
            val columns: Map<String, SettableValue> = accessStrategy.getOutboundRow(entity)
            var update: Update? = null
            val iterator: Iterator<*> = columns.keys.iterator()
            while (iterator.hasNext()) {
                val column = iterator.next() as String
                update = update?.set(column, columns[column]) ?: Update.update(column, columns[column])
            }
            val operation = mapper.getMappedObject(mapper.createUpdate(this.entity.tableName, update!!)
                    .withCriteria(Criteria.where(idPropertyName).`is`(idValue)))
            return databaseClient.execute(operation).fetch().rowsUpdated()
                    .handle { rowsUpdated: Int, sink: SynchronousSink<S> ->
                        if (rowsUpdated > 0) {
                            sink.next(entity)
                        }
                    }
        }
    }

    @Transactional
    override fun <S : T> saveAll(entities: Iterable<S>): Flux<S> {
        return Flux.fromIterable(entities).concatMap { objectToSave: S -> save(objectToSave) }
    }

    @Transactional
    override fun <S : T> saveAll(entities: Publisher<S>): Flux<S> {
        return Flux.from(entities).concatMap { objectToSave: S -> save(objectToSave) }
    }

    override fun findById(id: ID): Mono<T> {
        val columns = accessStrategy.getAllColumns(entity.javaType)
        val idColumnName = getIdColumnName()
        val mapper: StatementMapper = accessStrategy.statementMapper.forType(entity.javaType)
        val selectSpec = mapper.createSelect(entity.tableName)
                .withProjection(columns)
                .withCriteria(Criteria.where(idColumnName).`is`(id as Any))
        val operation = mapper.getMappedObject(selectSpec)
        return databaseClient.execute(operation)
                .`as`(entity.javaType)
                .fetch()
                .one()
    }

    override fun findById(publisher: Publisher<ID>): Mono<T> {
        return Mono.from(publisher).flatMap { id: ID -> this.findById(id) }
    }

    override fun existsById(id: ID): Mono<Boolean> {
        val idColumnName = getIdColumnName()
        val mapper: StatementMapper = accessStrategy.statementMapper.forType(entity.javaType)
        val selectSpec = mapper.createSelect(entity.tableName)
                .withProjection(listOf(idColumnName))
                .withCriteria(Criteria.where(idColumnName).`is`(id as Any))
        val operation = mapper.getMappedObject(selectSpec)
        return databaseClient.execute(operation)
                .map { r: Row?, _: RowMetadata? -> r }
                .first()
                .hasElement()
    }

    override fun existsById(publisher: Publisher<ID>): Mono<Boolean> {
        return Mono.from(publisher).flatMap { id: ID -> this.findById(id) }.hasElement()
    }

    override fun findAll(): Flux<T> {
        return databaseClient.select().from(entity.javaType).fetch().all()
    }

    override fun findAllById(iterable: Iterable<ID>): Flux<T> {
        return findAllById(Flux.fromIterable(iterable))
    }

    override fun findAllById(idPublisher: Publisher<ID>): Flux<T> {
        return Flux.from(idPublisher).buffer().concatMap { ids: List<ID> ->
            if (ids.isEmpty()) {
                return@concatMap Flux.empty<T>()
            }
            val columns = accessStrategy.getAllColumns(entity.javaType)
            val idColumnName = getIdColumnName()
            val mapper: StatementMapper = accessStrategy.statementMapper.forType(entity.javaType)
            val selectSpec = mapper.createSelect(entity.tableName)
                    .withProjection(columns)
                    .withCriteria(Criteria.where(idColumnName).`in`(ids))
            val operation = mapper.getMappedObject(selectSpec)
            databaseClient.execute(operation).`as`(entity.javaType).fetch().all()
        }
    }

    override fun count(): Mono<Long> {
        val table = Table.create(entity.tableName)
        val select = StatementBuilder
                .select(Functions.count(table.column(getIdColumnName())))
                .from(table)
                .build()
        return databaseClient.execute(SqlRenderer.toString(select))
                .map { r: Row, _: RowMetadata? -> r.get(0, Long::class.java)!! }
                .first()
                .defaultIfEmpty(0L)
    }

    @Transactional
    override fun deleteById(id: ID): Mono<Void> {
        return databaseClient.delete()
                .from(entity.javaType)
                .table(entity.tableName)
                .matching(Criteria.where(getIdColumnName()).`is`(id as Any))
                .fetch()
                .rowsUpdated()
                .then()
    }

    @Transactional
    override fun deleteById(idPublisher: Publisher<ID>): Mono<Void> {
        return Flux.from(idPublisher).buffer().filter { ids: List<ID> -> ids.isNotEmpty() }.concatMap { ids: List<ID> ->
            if (ids.isEmpty()) {
                return@concatMap Flux.empty<Int>()
            }
            databaseClient.delete()
                    .from(entity.javaType)
                    .table(entity.tableName)
                    .matching(Criteria.where(getIdColumnName()).`in`(ids))
                    .fetch()
                    .rowsUpdated()
        }.then()
    }

    @Transactional
    override fun delete(objectToDelete: T): Mono<Void> {
        return deleteById(entity.getRequiredId(objectToDelete))
    }

    @Transactional
    override fun deleteAll(iterable: Iterable<T>): Mono<Void> {
        return deleteAll(Flux.fromIterable(iterable))
    }

    @Transactional
    override fun deleteAll(objectPublisher: Publisher<out T>): Mono<Void> {
        val idPublisher = Flux.from(objectPublisher)
                .map { entity: T -> this.entity.getRequiredId(entity) }
        return deleteById(idPublisher)
    }

    @Transactional
    override fun deleteAll(): Mono<Void> {
        return databaseClient.delete().from(entity.tableName).then()
    }

    private fun getMappedObject(dsl: R2dbcDsl, pageable: Pageable): PreparedOperation<Select> {
        val connectionPool: ConnectionPool = ApplicationContext.getBean(ConnectionFactory::class.java) as ConnectionPool
        val dialect = DialectResolver.getDialect(connectionPool)
        val table = Table.create(entity.tableName)
        val joins = HashMap<String, Table>()
        val entityColumns = accessStrategy.getAllColumns(entity.javaType)
        val queryFields = dsl.getQueryFields()
        if (queryFields.isNotEmpty()) {
            for(field in queryFields) {
                if (!joins.contains(field) && field.contains('.')) {
                    val tableField = field.deCamel().split('.').first()
                    if (entityColumns.contains(tableField + "_id")) {
                        joins[tableField] = Table.create(tableField)
                    }
                }
            }
        }
        val columns = ArrayList<Column>()
        for (fieldName in dsl.getDslFields()) {
            val sqlFieldName = fieldName.deCamel()
            if (entityColumns.contains(sqlFieldName)) {
                columns.add(Column.create(sqlFieldName, table))
            } else {
                if (sqlFieldName.contains('.')) {
                    val parts = sqlFieldName.split('.')
                    val tableName = parts.first()
                    if (entityColumns.contains("${tableName}_id")) {
                        if (!joins.contains(tableName)) {
                            joins[tableName] = Table.create(tableName)
                        }
                        columns.add(Column.create(parts.last() + " as " + sqlFieldName.replaceDot(), joins[tableName]!!))
                        continue
                    }
                    if (entityColumns.contains(tableName)) {
                        columns.add(Column.create(sqlFieldName.toJsonbPath() + " as " + sqlFieldName.replaceDot(), table))
                        continue
                    }
                }
                throw IllegalArgumentException("Field $fieldName not found")
            }
        }
        val selectBuilder = CustomSelectBuilder().select(columns).from(table)
        for(join in joins) {
            selectBuilder.join(CustomSelectBuilder.JoinBuilder(joins[join.key]!!, selectBuilder)
                    .on(Column.create("${join.key}_id", table)).equals(Column.create("id", join.value))
                    .finishJoin())
        }
        joins[StringUtil.EMPTY_STRING] = table
        val updateMapper = CustomUpdateMapper(converter)
        val bindMarkers: BindMarkers = dialect.bindMarkersFactory.create()
        var bindings = Bindings.empty()
        val criteria = dsl.getCriteriaBy(entity.javaType)
        if (criteria != null) {
            val mappedObject: BoundCondition = updateMapper.getMappedObject(bindMarkers, criteria, joins)
            bindings = mappedObject.bindings
            selectBuilder.where(mappedObject.condition)
        }
        if (pageable.sort.isSorted) {
            val mappedSort: Sort = updateMapper.getMappedObject(pageable.sort, null)
            val fields: MutableList<OrderByField> = java.util.ArrayList()
            for (order in mappedSort) {
                val orderByField = OrderByField.from(table.column(order.property))
                fields.add(if (order.isAscending) orderByField.asc() else orderByField.desc())
            }
            selectBuilder.orderBy(fields)
        }
        if (pageable.isPaged) {
            selectBuilder.limitOffset(pageable.pageSize.toLong(), pageable.offset)
        }
        return DslPreparedOperation(selectBuilder.build(), RenderContextFactory(dialect).createRenderContext(), bindings)
    }

    private fun R2dbcDsl.getDslFields(): List<String> {
        val list = ArrayList<String>()
        if (this.fields.isNotEmpty()) {
            return this.fields.toList()
        } else {
            val properties = accessStrategy.mappingContext.getPersistentEntity(entity.javaType)!!
            for (property in properties) {
                list.add(property.name)
            }
        }
        return list
    }

    private fun getIdColumnName(): String {
        return try {
            converter
                    .mappingContext
                    .getRequiredPersistentEntity(entity.javaType)
                    .requiredIdProperty
                    .columnName
        } catch (ex: Exception) {
            "id"
        }
    }

    private fun <T> populateIdIfNecessary(`object`: T): BiFunction<Row, RowMetadata, T> {
        return BiFunction { row: Row, _: RowMetadata ->
            val idPropertyName = getIdColumnName()
            val properties = accessStrategy.mappingContext.getPersistentEntity(entity.javaType)!!
            val idProperty: RelationalPersistentProperty = properties.getPersistentProperty(idPropertyName)!!
            val userClass = ClassUtils.getUserClass(`object` as Any)
            val persistentEntity: RelationalPersistentEntity<*> = accessStrategy.mappingContext.getRequiredPersistentEntity(userClass)
            val propertyAccessor: PersistentPropertyAccessor<*> = persistentEntity.getPropertyAccessor(`object`)
            propertyAccessor.setProperty(idProperty, converter.conversionService.convert(row[idPropertyName], idProperty.type))
            propertyAccessor.bean as T
        }
    }

    private fun StringBuilder.binding(target: Any): String {
        var buildQuery = this.toString().trimIndent()
        for (field in FastMethodInvoker.reflectionStorage(target::class.java)) {
            if (buildQuery.contains(":${field.name}") && field.name != "id") {
                val value = target.getValue(field.name)
                val result = if (value == null) "null" else
                    when (value) {
                        is String -> "'$value'"
                        is UUID -> "'$value'::uuid"
                        is ByteArray -> "decode('${value.hex()}', 'hex')"
                        is Enum<*> -> "'${value.name}'"
                        is JsonNode -> "'${value.toString().replace("'", "")}'::jsonb"
                        else -> ConvertUtils.convert(value, String::class.java).toString()
                    }
                buildQuery = buildQuery.replace(":${field.name}", result)
            }
        }
        return buildQuery
    }

    private fun Array<String>.toJsonbPath(): List<String> {
        val columns = accessStrategy.getAllColumns(entity.javaType)
        val mutableList = mutableListOf<String>()
        for (field in this) {
            if (field.contains('.')) {
                val tmp = field.split('.')
                if (columns.contains(tmp.first().deCamel())) {
                    mutableList.add(field.toJsonbPath(entity.javaType) + " as " + field.replaceDot())
                    continue
                }
            }
            mutableList.add(field.deCamel())
        }
        return mutableList
    }
}