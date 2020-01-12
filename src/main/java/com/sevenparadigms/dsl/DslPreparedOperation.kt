package com.sevenparadigms.dsl

import org.springframework.data.r2dbc.core.PreparedOperation
import org.springframework.data.r2dbc.dialect.BindTarget
import org.springframework.data.r2dbc.dialect.Bindings
import org.springframework.data.relational.core.sql.Delete
import org.springframework.data.relational.core.sql.Insert
import org.springframework.data.relational.core.sql.Select
import org.springframework.data.relational.core.sql.Update
import org.springframework.data.relational.core.sql.render.RenderContext
import org.springframework.data.relational.core.sql.render.SqlRenderer

class DslPreparedOperation<T>(private val source: T, private val renderContext: RenderContext, private val bindings: Bindings) : PreparedOperation<T> {

    override fun getSource(): T {
        return source
    }

    override fun toQuery(): String {
        val sqlRenderer = SqlRenderer.create(renderContext)
        if (source is Select) {
            return sqlRenderer.render((source as Select))
        }
        if (source is Insert) {
            return sqlRenderer.render((source as Insert))
        }
        if (source is Update) {
            return sqlRenderer.render((source as Update))
        }
        if (source is Delete) {
            return sqlRenderer.render((source as Delete))
        }
        throw IllegalStateException("Cannot render " + getSource())
    }

    override fun bindTo(to: BindTarget) {
        bindings.apply(to)
    }
}