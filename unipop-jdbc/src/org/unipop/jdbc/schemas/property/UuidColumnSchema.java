package org.unipop.jdbc.schemas.property;

/**
 * Implemented by property schemas backed by a PostgreSQL native uuid column, so the row schema can
 * tell the predicates translator which columns to compare as text (col::text = ?).
 */
public interface UuidColumnSchema {
    /** @return the uuid column name this schema is backed by. */
    String getUuidColumn();
}
