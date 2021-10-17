package org.springframework.data.r2dbc.repository.support;

import org.springframework.data.relational.core.sql.Delete;
import org.springframework.data.relational.core.sql.Insert;
import org.springframework.data.relational.core.sql.Select;
import org.springframework.data.relational.core.sql.Update;
import org.springframework.data.relational.core.sql.render.RenderContext;
import org.springframework.data.relational.core.sql.render.SqlRenderer;
import org.springframework.r2dbc.core.PreparedOperation;
import org.springframework.r2dbc.core.binding.BindTarget;
import org.springframework.r2dbc.core.binding.Bindings;

class DslPreparedOperation<T> implements PreparedOperation<T> {
    private T source;
    private RenderContext renderContext;
    private Bindings bindings;

    public DslPreparedOperation(T source, RenderContext renderContext, Bindings bindings) {
        this.source = source;
        this.renderContext = renderContext;
        this.bindings = bindings;
    }

    public Bindings getBindings() {
        return bindings;
    }

    @Override
    public T getSource() {
        return source;
    }

    @Override
    public void bindTo(BindTarget target) {
        bindings.apply(target);
    }

    @Override
    public String toQuery() {
        SqlRenderer sqlRenderer = SqlRenderer.create(renderContext);
        if (source instanceof Select) {
            return sqlRenderer.render((Select) source);
        }
        if (source instanceof Insert) {
            return sqlRenderer.render((Insert) source);
        }
        if (source instanceof Update) {
            return sqlRenderer.render((Update) source);
        }
        if (source instanceof Delete) {
            return sqlRenderer.render((Delete) source);
        }
        throw new IllegalStateException("Cannot render " + getSource());
    }
}