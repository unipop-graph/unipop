package org.unipop.jdbc.schemas.property;

/**
 * Implemented by property schemas backed by a PostgreSQL JSONB column, so the row schema can tell
 * the predicates translator which columns render JSON path extraction (col->>'k').
 */
public interface JsonbColumnSchema {
    /** @return the JSONB column name this schema is backed by. */
    String getJsonbColumn();
}
