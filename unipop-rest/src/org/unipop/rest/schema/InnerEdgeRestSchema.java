package org.unipop.rest.schema;

import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.json.JSONObject;
import org.unipop.rest.RestVertexSchema;
import org.unipop.rest.util.MatcherHolder;
import org.unipop.rest.util.TemplateHolder;
import org.unipop.schema.element.ElementSchema;
import org.unipop.schema.element.InnerEdgeSchema;
import org.unipop.schema.element.VertexSchema;
import org.unipop.structure.UniEdge;
import org.unipop.structure.UniGraph;
import org.unipop.structure.UniVertex;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class InnerEdgeRestSchema extends RestEdge implements InnerEdgeSchema {
    private final VertexSchema childVertexSchema;
    private final VertexSchema parentVertexSchema;

    public InnerEdgeRestSchema(JSONObject configuration, UniGraph graph, String url, TemplateHolder templateHolder, String resultPath, JSONObject opTranslator, int maxResultSize, MatcherHolder complexTranslator, boolean valuesToString, Direction parentDirection, RestVertexSchema parentVertexSchema, String resource){
        super(configuration, graph, url, templateHolder, resultPath, opTranslator, maxResultSize, complexTranslator, valuesToString);
        this.resource = resource;
        this.parentVertexSchema = parentVertexSchema;
        this.childVertexSchema = this.createVertexSchema("vertex", ((RestVertex)this.parentVertexSchema).getResource());
        this.outVertexSchema = parentDirection.equals(Direction.OUT) ? parentVertexSchema : this.childVertexSchema;
        this.inVertexSchema = parentDirection.equals(Direction.IN) ? parentVertexSchema : this.childVertexSchema;
    }

    public Set<ElementSchema> getChildSchema(){
        return Sets.newHashSet(new ElementSchema[] {this.childVertexSchema});
    }

    @Override
    public Collection<UniEdge> fromVertex(UniVertex parentVertex, Map<String, Object> fields) {
        Map edgeProperties = this.getProperties(fields);
        Object inVertex;
        Object outVertex;

        if (edgeProperties == null)
            return null;

        outVertex = this.outVertexSchema.equals(this.parentVertexSchema) ? parentVertex : this.outVertexSchema.createElement(fields);
        inVertex = this.outVertexSchema.equals(this.parentVertexSchema) ? parentVertex : this.inVertexSchema.createElement(fields);

        if (inVertex == null || outVertex == null)
            return null;

        UniEdge uniEdge = new UniEdge(edgeProperties, (Vertex)outVertex, (Vertex)inVertex, this, this.graph);
        return Collections.singleton(uniEdge);
    }
}
