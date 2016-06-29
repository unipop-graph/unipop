package org.unipop.elastic.document.schema;

import io.searchbox.core.Search;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.search.DeferredVertexQuery;
import org.unipop.schema.EdgeSchema;
import org.unipop.schema.ElementSchema;
import org.unipop.schema.VertexSchema;
import org.unipop.schema.property.PropertySchema;
import org.unipop.structure.UniGraph;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DocVertexSchema extends DocSchema<Vertex> implements VertexSchema {
    List<EdgeSchema> edgeSchemas = new ArrayList<>();

    public DocVertexSchema(String index, String type, List<PropertySchema> properties, UniGraph graph) {
        super(index, type, properties, graph);
    }

    @Override
    public Set<ElementSchema> getWithChildSchemas() {
        HashSet<ElementSchema> schemas = new HashSet<>(this.edgeSchemas);
        schemas.add(this);
        return schemas;
    }

    public void add(DocEdgeSchema schema) {
        edgeSchemas.add(schema);
    }

    public Search toPredicates(DeferredVertexQuery query){
        PredicatesHolder predicatesHolder = this.toPredicates(query.getVertices());
        return getSearch(query, predicatesHolder);
    }
}
