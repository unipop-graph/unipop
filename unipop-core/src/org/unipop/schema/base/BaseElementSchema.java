package org.unipop.schema.base;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.unipop.schema.ElementSchema;
import org.unipop.schema.property.*;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;
import org.unipop.structure.UniElement;
import org.unipop.structure.UniGraph;

import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public abstract class BaseElementSchema<E extends Element> implements ElementSchema<E> {

    protected final List<PropertySchema> propertySchemas;
    protected UniGraph graph;

    public BaseElementSchema(List<PropertySchema> propertySchemas, UniGraph graph) {
        this.propertySchemas = propertySchemas;
        this.graph = graph;
    }

    @Override
    public UniGraph getGraph() {
        return graph;
    }

    @Override
    public List<PropertySchema> getPropertySchemas() {
        return propertySchemas;
    }
}
