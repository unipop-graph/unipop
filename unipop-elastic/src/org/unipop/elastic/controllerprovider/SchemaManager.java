package org.unipop.elastic.controllerprovider;

import org.unipop.elastic.schema.ElasticElementSchema;

import java.util.Set;
import java.util.stream.Collectors;

public class SchemaManager{

    private Set<ElasticElementSchema> schemas;

    public SchemaManager(Set<ElasticElementSchema> schemas) {
        this.schemas = schemas;
    }

    public Set<ElasticElementSchema> getSchemas(){
        return schemas;
    }

    public <S extends ElasticElementSchema> Set<S> getSchemas(Class<S> schemaClass){
        return getSchemas().stream()
                .<S>filter(schema -> schemaClass.isAssignableFrom(schema.getClass()))
                .map(schema -> (S)schema)
                .collect(Collectors.toSet());
    }
}
