package org.unipop.elastic.schema;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.unipop.controller.Predicates;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class SchemaManager{

    private Set<ElementSchema> schemas;

    public SchemaManager(Set<ElementSchema> schemas) {
        this.schemas = schemas;
    }

    public Set<ElementSchema> getSchemas(){
        return schemas;
    }

    public <S extends ElementSchema> Set<S> getSchemas(Class<S> schemaClass){
        return getSchemas().stream()
                .<S>filter(schema -> schemaClass.isAssignableFrom(schema.getClass()))
                .map(schema -> (S)schema)
                .collect(Collectors.toSet());
    }

    public <E extends Element, S extends ElementSchema<E>> Set<S> getSchemas(Class<ElementSchema> schemaClass, Class<E> elementClass){
        return getSchemas().stream()
                .<S>filter(schema -> schemaClass.isAssignableFrom(schema.getClass()) && elementClass.isAssignableFrom(schema.getElementType()))
                .map(schema -> (S)schema)
                .collect(Collectors.toSet());
    }
}
