package com.sevenparadigms.dsl

import org.springframework.data.mapping.context.MappingContext
import org.springframework.data.projection.ProjectionFactory
import org.springframework.data.r2dbc.convert.R2dbcConverter
import org.springframework.data.r2dbc.core.DatabaseClient
import org.springframework.data.r2dbc.core.ReactiveDataAccessStrategy
import org.springframework.data.r2dbc.repository.query.R2dbcQueryMethod
import org.springframework.data.r2dbc.repository.query.StringBasedR2dbcQuery
import org.springframework.data.relational.core.mapping.RelationalPersistentEntity
import org.springframework.data.relational.core.mapping.RelationalPersistentProperty
import org.springframework.data.relational.repository.query.RelationalEntityInformation
import org.springframework.data.relational.repository.support.MappingRelationalEntityInformation
import org.springframework.data.repository.Repository
import org.springframework.data.repository.core.NamedQueries
import org.springframework.data.repository.core.RepositoryInformation
import org.springframework.data.repository.core.RepositoryMetadata
import org.springframework.data.repository.core.support.ReactiveRepositoryFactorySupport
import org.springframework.data.repository.core.support.RepositoryFactoryBeanSupport
import org.springframework.data.repository.core.support.RepositoryFactorySupport
import org.springframework.data.repository.query.QueryLookupStrategy
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider
import org.springframework.data.repository.query.RepositoryQuery
import org.springframework.expression.spel.standard.SpelExpressionParser
import org.springframework.lang.Nullable
import java.io.Serializable
import java.lang.reflect.Method
import java.util.*

class R2dbcDslRepositoryFactoryBean<T, ID : Serializable?, R : Repository<T, ID>?>(repositoryInterface: Class<out R>?) : RepositoryFactoryBeanSupport<R, T, ID>(repositoryInterface!!) {
    @Nullable
    private var client: DatabaseClient? = null
    @Nullable
    private var dataAccessStrategy: ReactiveDataAccessStrategy? = null

    fun setDataAccessStrategy(@Nullable dataAccessStrategy: ReactiveDataAccessStrategy?) {
        this.dataAccessStrategy = dataAccessStrategy
    }

    fun setDatabaseClient(@Nullable client: DatabaseClient?) {
        this.client = client
    }

    class DslRepositoryFactory<T, ID : Serializable?>(private val databaseClient: DatabaseClient, dataAccessStrategy: ReactiveDataAccessStrategy) : ReactiveRepositoryFactorySupport() {
        private val mappingContext: MappingContext<out RelationalPersistentEntity<*>, out RelationalPersistentProperty?>
        private val converter: R2dbcConverter = dataAccessStrategy.converter
        private val dataAccessStrategy: ReactiveDataAccessStrategy

        override fun <T, ID> getEntityInformation(domainClass: Class<T>): RelationalEntityInformation<T, ID> {
            return getEntityInformation(domainClass, null)
        }

        private fun <T, ID> getEntityInformation(domainClass: Class<T>, @Nullable information: RepositoryInformation?): RelationalEntityInformation<T, ID> {
            val entity = mappingContext.getRequiredPersistentEntity(domainClass)
            return MappingRelationalEntityInformation((entity as RelationalPersistentEntity<T>))
        }

        override fun getTargetRepository(information: RepositoryInformation): Repository<T, ID> {
            val entity = mappingContext.getRequiredPersistentEntity(information.domainType)
            val entityInformation = MappingRelationalEntityInformation<T, ID>(entity as RelationalPersistentEntity<T>)
            return getTargetRepositoryViaReflection(information, entityInformation, databaseClient, converter, dataAccessStrategy)
        }

        override fun getQueryLookupStrategy(@Nullable key: QueryLookupStrategy.Key?, evaluationContextProvider: QueryMethodEvaluationContextProvider): Optional<QueryLookupStrategy> {
            return Optional.of(R2dbcQueryLookupStrategy(databaseClient, evaluationContextProvider, converter))
        }

        override fun getRepositoryBaseClass(metadata: RepositoryMetadata): Class<*> {
            return R2dbcDslRepositoryImpl::class.java
        }

        init {
            mappingContext = converter.mappingContext
            this.dataAccessStrategy = dataAccessStrategy
        }
    }

    override fun createRepositoryFactory(): RepositoryFactorySupport {
        return DslRepositoryFactory<Any, Serializable>(client!!, dataAccessStrategy!!)
    }

    private class R2dbcQueryLookupStrategy internal constructor(private val databaseClient: DatabaseClient,
                                                                private val evaluationContextProvider: QueryMethodEvaluationContextProvider, private val converter: R2dbcConverter) : QueryLookupStrategy {
        override fun resolveQuery(method: Method, metadata: RepositoryMetadata, factory: ProjectionFactory,
                                  namedQueries: NamedQueries): RepositoryQuery {
            val queryMethod = R2dbcQueryMethod(method, metadata, factory, converter.mappingContext)
            val namedQueryName = queryMethod.namedQueryName
            if (namedQueries.hasQuery(namedQueryName)) {
                val namedQuery = namedQueries.getQuery(namedQueryName)
                return StringBasedR2dbcQuery(namedQuery, queryMethod, databaseClient, converter, EXPRESSION_PARSER, evaluationContextProvider)
            } else if (queryMethod.hasAnnotatedQuery()) {
                return StringBasedR2dbcQuery(queryMethod, databaseClient, converter, EXPRESSION_PARSER, evaluationContextProvider)
            }
            throw UnsupportedOperationException("Query derivation not yet supported!")
        }
    }

    companion object {
        private val EXPRESSION_PARSER = SpelExpressionParser()
    }
}