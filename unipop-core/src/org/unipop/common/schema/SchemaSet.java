package org.unipop.common.schema;


import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class SchemaSet<S extends ElementSchema> extends HashSet<S> {

    public SchemaSet(Set<S> filteredSchemas) {
        super(filteredSchemas);
    }

    public <E extends S> SchemaSet<E> getSchemas(Class<E> schemaClass){
        Set<E> filteredSchemas = this.stream()
            .<E>filter(schema -> schemaClass.isAssignableFrom(schema.getClass()))
            .map(schema -> (E) schema)
            .collect(Collectors.toSet());

        return new SchemaSet<>(filteredSchemas);
    }
}
