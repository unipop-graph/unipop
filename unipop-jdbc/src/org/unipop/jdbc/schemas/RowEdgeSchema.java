package org.unipop.jdbc.schemas;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.structure.*;
import org.javatuples.Pair;
import org.jooq.Result;
import org.json.JSONException;
import org.json.JSONObject;
import org.unipop.jdbc.schemas.jdbc.JdbcEdgeSchema;
import org.unipop.query.aggregation.LocalQuery;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;
import org.unipop.query.search.SearchVertexQuery;
import org.unipop.schema.element.ElementSchema;
import org.unipop.schema.element.VertexSchema;
import org.unipop.schema.property.AbstractPropertyContainer;
import org.unipop.schema.property.PropertySchema;
import org.unipop.schema.reference.ReferenceVertexSchema;
import org.unipop.structure.UniEdge;
import org.unipop.structure.UniGraph;
import org.unipop.util.ConversionUtils;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Gur Ronen
 * @since 6/13/2016
 */
public class RowEdgeSchema extends AbstractJdbcEdgeSchema
{
    protected VertexSchema inVertexSchema;
    protected VertexSchema outVertexSchema;

    public RowEdgeSchema(JSONObject configuration, UniGraph graph) {
        super(configuration, graph);

        this.inVertexSchema = createVertexSchema("inVertex");
        this.outVertexSchema = createVertexSchema("outVertex");
    }

    protected VertexSchema createVertexSchema(String key) throws JSONException {
        JSONObject vertexConfiguration = this.json.optJSONObject(key);
        if(vertexConfiguration == null) return null;
        if(vertexConfiguration.optBoolean("ref", false)) return new ReferenceVertexSchema(vertexConfiguration, graph);
        return new RowVertexSchema(vertexConfiguration, table, graph);
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
    public PredicatesHolder toPredicates(PredicatesHolder predicatesHolder) {
        return super.toPredicates(predicatesHolder);
    }

    @Override
    public PredicatesHolder toPredicates(List<Vertex> vertices, Direction direction, PredicatesHolder predicates) {
        PredicatesHolder edgePredicates = this.toPredicates(predicates);
        PredicatesHolder vertexPredicates = this.getVertexPredicates(vertices, direction);
        return PredicatesHolderFactory.and(edgePredicates, vertexPredicates);
    }

    @Override
    public Collection<Pair<String, Element>> parseLocal(Result result, LocalQuery query) {
        SearchVertexQuery searchQuery = (SearchVertexQuery) query.getSearchQuery();
        ArrayList<Pair<String, Element>> finalResult = new ArrayList<>();
        List<Map<String, Object>> resultMap = result.intoMaps();
        if (searchQuery.getDirection().equals(Direction.OUT) || searchQuery.getDirection().equals(Direction.BOTH)) {
            resultMap.stream().flatMap(map -> {
                Vertex outVertex = getOutVertexSchema().createElement(map);
                String id = outVertex.id().toString();
                Collection<Edge> edges = fromFields(map);
                return edges == null ? Stream.empty() : edges.stream().flatMap(e -> {
                    if (query.getQueryClass().equals(Edge.class))
                        return Stream.of(Pair.with(id, (Element)e));
                    if (searchQuery.getDirection().equals(Direction.OUT))
                        return Stream.of(Pair.with(id, (Element)e.inVertex()));
                    if (searchQuery.getDirection().equals(Direction.IN))
                        return Stream.of(Pair.with(id, (Element)e.outVertex()));
                    return Stream.of(Pair.with(id, (Element)e.outVertex()), Pair.with(id, (Element)e.inVertex()));
                });
            }).forEach(finalResult::add);
        }
        if (searchQuery.getDirection().equals(Direction.IN) || searchQuery.getDirection().equals(Direction.BOTH)) {
            resultMap.stream().flatMap(map -> {
                Vertex outVertex = getInVertexSchema().createElement(map);
                if (outVertex == null) return Stream.empty();
                String id = outVertex.id().toString();
                Collection<Edge> edges = fromFields(map);
                return edges == null ? Stream.empty() : edges.stream().flatMap(e -> {
                    if (query.getQueryClass().equals(Edge.class))
                        return Stream.of(Pair.with(id, (Element)e));
                    if (searchQuery.getDirection().equals(Direction.OUT))
                        return Stream.of(Pair.with(id, (Element)e.inVertex()));
                    if (searchQuery.getDirection().equals(Direction.IN))
                        return Stream.of(Pair.with(id, (Element)e.outVertex()));
                    return Stream.of(Pair.with(id, (Element)e.outVertex()), Pair.with(id, (Element)e.inVertex()));
                });
            }).forEach(finalResult::add);
        }
        return finalResult;
    }

    @Override
    public VertexSchema getOutVertexSchema() {
        return outVertexSchema;
    }

    @Override
    public VertexSchema getInVertexSchema() {
        return inVertexSchema;
    }

    protected PredicatesHolder getVertexPredicates(List<Vertex> vertices, Direction direction) {
        PredicatesHolder outPredicates = this.outVertexSchema.toPredicates(vertices);
        PredicatesHolder inPredicates = this.inVertexSchema.toPredicates(vertices);
        if(direction.equals(Direction.OUT)) return outPredicates;
        if(direction.equals(Direction.IN)) return inPredicates;
        return PredicatesHolderFactory.or(inPredicates, outPredicates);
    }

    @Override
    public String toString() {
        return "RowEdgeSchema{" +
                "inVertexSchema=" + inVertexSchema +
                ", outVertexSchema=" + outVertexSchema +
                "} " + super.toString();
    }
}
