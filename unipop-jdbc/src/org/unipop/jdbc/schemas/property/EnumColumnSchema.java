package org.unipop.jdbc.schemas.property;

/**
 * Implemented by property schemas backed by a PostgreSQL native enum column, so the row schema can
 * tell the predicates translator which columns to compare as text (col::text = ?).
 */
public interface EnumColumnSchema {
    /** @return the enum column name this schema is backed by. */
    String getEnumColumn();
}
