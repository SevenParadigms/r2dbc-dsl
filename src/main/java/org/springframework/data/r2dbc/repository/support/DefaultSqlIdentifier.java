package org.springframework.data.r2dbc.repository.support;

import org.springframework.data.relational.core.sql.IdentifierProcessing;
import org.springframework.data.relational.core.sql.SqlIdentifier;
import org.springframework.util.Assert;

import java.util.Collections;
import java.util.Iterator;
import java.util.function.UnaryOperator;

/**
 * Default {@link SqlIdentifier} implementation using a {@code name} and whether the identifier is quoted.
 *
 * @author Jens Schauder
 * @author Mark Paluch
 * @since 2.0
 */
public class DefaultSqlIdentifier implements SqlIdentifier {

    private final String name;
    private final boolean quoted;

    public DefaultSqlIdentifier(String name, boolean quoted) {

        Assert.hasText(name, "A database object name must not be null or empty");

        this.name = name;
        this.quoted = quoted;
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.relational.domain.SqlIdentifier#iterator()
     */
    @Override
    public Iterator<SqlIdentifier> iterator() {
        return Collections.<SqlIdentifier> singleton(this).iterator();
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.relational.domain.SqlIdentifier#transform(java.util.function.UnaryOperator)
     */
    @Override
    public SqlIdentifier transform(UnaryOperator<String> transformationFunction) {

        Assert.notNull(transformationFunction, "Transformation function must not be null");

        return new DefaultSqlIdentifier(transformationFunction.apply(name), quoted);
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.relational.domain.SqlIdentifier#toSql(org.springframework.data.relational.domain.IdentifierProcessing)
     */
    @Override
    public String toSql(IdentifierProcessing processing) {
        return quoted ? processing.quote(getReference(processing)) : getReference(processing);
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.relational.domain.SqlIdentifier#getReference(org.springframework.data.relational.domain.IdentifierProcessing)
     */
    @Override
    public String getReference(IdentifierProcessing processing) {
        return name;
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object o) {

        if (this == o) {
            return true;
        }

        if (o instanceof SqlIdentifier) {
            return toString().equals(o.toString());
        }

        return false;
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        return quoted ? toSql(IdentifierProcessing.ANSI) : this.name;
    }
}
