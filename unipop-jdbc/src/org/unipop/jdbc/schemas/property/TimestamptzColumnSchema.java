package org.unipop.jdbc.schemas.property;

/**
 * Implemented by property schemas backed by a PostgreSQL native timestamptz column, so the row
 * schema can tell the predicates translator which columns take native-temporal predicate handling
 * (values coerced to OffsetDateTime and compared as real timestamps, not text).
 */
public interface TimestamptzColumnSchema {
    /** @return the timestamptz column name this schema is backed by. */
    String getTimestamptzColumn();
}
