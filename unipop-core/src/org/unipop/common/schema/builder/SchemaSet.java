package org.unipop.common.schema.builder;

import org.unipop.common.schema.ElementSchema;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class SchemaSet {

    Set<ElementSchema> schemas = new HashSet<>();

    public void add(Collection<? extends ElementSchema> schemas){
        this.schemas.addAll(schemas);
    }

    public Set<ElementSchema> get() {
        return schemas;
    }

    public <T extends ElementSchema> Set<T> get(Class<? extends T> c){
        return schemas.stream()
                .filter(schema -> c.isAssignableFrom(schema.getClass()))
                .map(schema -> (T) schema)
                .collect(Collectors.toSet());
    }
}
