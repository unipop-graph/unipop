package org.unipop.schema.element;

import java.util.HashSet;
import java.util.Set;

public class SchemaSet {

    Set<ElementSchema> schemas = new HashSet<>();

    public void add(ElementSchema schema){
        this.schemas.add(schema);
    }

    public Set<ElementSchema> get(Boolean recursive) {
        if(!recursive) return schemas;

        Set<ElementSchema> result = new HashSet<>();
        addRecursive(result, this.schemas);
        return result;
    }

    private void addRecursive(Set<ElementSchema> result, Set<ElementSchema> schemas) {
        schemas.forEach(schema -> {
            if(result.contains(schema)) return;
            result.add(schema);
            Set childSchemas = schema.getChildSchemas();
            addRecursive(result, childSchemas);
        });
    }

    public <T extends ElementSchema> Set<T> get(Class<? extends T> c, Boolean recursive){
        Set<T> result = new HashSet<>();
        this.get(recursive).forEach(schema -> {
            if(c.isAssignableFrom(schema.getClass())) result.add((T)schema);
        });
        return result;
    }
}