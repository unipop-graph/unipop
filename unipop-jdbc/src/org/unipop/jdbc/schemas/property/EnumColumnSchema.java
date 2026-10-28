package org.unipop.jdbc.schemas.property;

/**
 * Implemented by property schemas backed by a PostgreSQL native enum column, so the row schema can
 * tell the predicates translator which columns are enums: eq/within compare on the enum type when
 * {@link #getEnumType()} is known, everything else as text (col::text = ?).
 */
public interface EnumColumnSchema {
    /** @return the enum column name this schema is backed by. */
    String getEnumColumn();

    /** @return the PostgreSQL enum type name, or null when the config does not declare one. */
    String getEnumType();
}
