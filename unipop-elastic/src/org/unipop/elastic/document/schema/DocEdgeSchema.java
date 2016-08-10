package org.unipop.elastic.document.schema;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.searchbox.core.Search;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.elasticsearch.index.query.QueryBuilder;
import org.json.JSONException;
import org.json.JSONObject;
import org.unipop.elastic.common.ElasticClient;
import org.unipop.elastic.document.DocumentEdgeSchema;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;
import org.unipop.query.search.SearchVertexQuery;
import org.unipop.schema.element.ElementSchema;
import org.unipop.schema.element.VertexSchema;
import org.unipop.schema.reference.ReferenceVertexSchema;
import org.unipop.structure.UniEdge;
import org.unipop.structure.UniGraph;
import org.unipop.util.ConversionUtils;

import java.util.*;

public class DocEdgeSchema extends AbstractDocSchema<Edge> implements DocumentEdgeSchema {
    protected VertexSchema outVertexSchema;
    protected VertexSchema inVertexSchema;

    public DocEdgeSchema(JSONObject configuration, ElasticClient client, UniGraph graph) throws JSONException {
        super(configuration, client, graph);
        this.outVertexSchema = createVertexSchema("outVertex");
        this.inVertexSchema = createVertexSchema("inVertex");
    }

    protected VertexSchema createVertexSchema(String key) throws JSONException {
        JSONObject vertexConfiguration = this.json.optJSONObject(key);
        if(vertexConfiguration == null) return null;
        if(vertexConfiguration.optBoolean("ref", false)) return new ReferenceVertexSchema(vertexConfiguration, graph);
        return new DocVertexSchema(vertexConfiguration, client, graph);
    }

    @Override
    public Set<ElementSchema> getChildSchemas() {
        return Sets.newHashSet(outVertexSchema, inVertexSchema);
    }

    @Override
    public Collection<Edge> fromFields(Map<String, Object> fields) {
        Map<String, Object> edgeProperties = getProperties(fields);
        if(edgeProperties == null) return null;
        Vertex outVertex = outVertexSchema.createElement(fields);
        if(outVertex == null) return null;
        Vertex inVertex = inVertexSchema.createElement(fields);
        if(inVertex == null) return null;
        UniEdge uniEdge = new UniEdge(edgeProperties, outVertex, inVertex, graph);
        return Collections.singleton(uniEdge);
    }

    @Override
    public Map<String, Object> toFields(Edge edge) {
        Map<String, Object> edgeFields = getFields(edge);
        Map<String, Object> inFields = inVertexSchema.toFields(edge.inVertex());
        Map<String, Object> outFields = outVertexSchema.toFields(edge.outVertex());
        return ConversionUtils.merge(Lists.newArrayList(edgeFields, inFields, outFields), this::mergeFields, false);
    }

    @Override
    public Set<String> toFields(Set<String> propertyKeys) {
        Set<String> fields = super.toFields(propertyKeys);
        Set<String> outFields = outVertexSchema.toFields(propertyKeys);
        fields.addAll(outFields);
        Set<String> inFields = inVertexSchema.toFields(propertyKeys);
        fields.addAll(inFields);
        return fields;
    }

    @Override
    public Search getSearch(SearchVertexQuery query) {
        PredicatesHolder edgePredicates = this.toPredicates(query.getPredicates());
        PredicatesHolder vertexPredicates = this.getVertexPredicates(query.getVertices(), query.getDirection());
        PredicatesHolder predicatesHolder = PredicatesHolderFactory.and(edgePredicates, vertexPredicates);
        if (predicatesHolder.isAborted()) return null;
        QueryBuilder queryBuilder = createQueryBuilder(predicatesHolder);
        return createSearch(query, queryBuilder);
    }

    protected PredicatesHolder getVertexPredicates(List<Vertex> vertices, Direction direction) {
        PredicatesHolder outPredicates = this.outVertexSchema.toPredicates(vertices);
        PredicatesHolder inPredicates = this.inVertexSchema.toPredicates(vertices);
        if(direction.equals(Direction.OUT) && outPredicates.notAborted()) return outPredicates;
        if(direction.equals(Direction.IN) && inPredicates.notAborted()) return inPredicates;
        if (outPredicates.notAborted() && inPredicates.notAborted())
            return PredicatesHolderFactory.or(inPredicates, outPredicates);
        else if (outPredicates.isAborted()) return inPredicates;
        else if (inPredicates.isAborted()) return outPredicates;
        else return PredicatesHolderFactory.abort();
    }

    @Override
    public String toString() {
        return "DocEdgeSchema{" +
                "index='" + index + '\'' +
                ", type='" + type + '\'' +
                '}';
    }

}
