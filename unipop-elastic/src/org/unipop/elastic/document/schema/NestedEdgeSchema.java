package org.unipop.elastic.document.schema;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.searchbox.action.BulkableAction;
import io.searchbox.core.DocumentResult;
import io.searchbox.core.Update;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.shaded.jackson.core.JsonProcessingException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.support.QueryInnerHitBuilder;
import org.json.JSONException;
import org.json.JSONObject;
import org.unipop.elastic.common.ElasticClient;
import org.unipop.elastic.common.FilterHelper;
import org.unipop.elastic.document.DocumentEdgeSchema;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;
import org.unipop.schema.element.ElementSchema;
import org.unipop.schema.element.VertexSchema;
import org.unipop.schema.reference.ReferenceVertexSchema;
import org.unipop.structure.UniEdge;
import org.unipop.structure.UniGraph;
import org.unipop.util.ConversionUtils;

import java.util.*;
import java.util.stream.Collectors;

public class NestedEdgeSchema extends AbstractDocSchema<Edge> implements DocumentEdgeSchema {
    private String path;
    private final VertexSchema parentVertexSchema;
    private final VertexSchema childVertexSchema;
    private final Direction parentDirection;

    public NestedEdgeSchema(VertexSchema parentVertexSchema, Direction parentDirection, String index, String type, String path, JSONObject configuration, ElasticClient client, UniGraph graph) throws JSONException {
        super(configuration, client, graph);
        this.index = index;
        this.type = type;
        this.path = path;
        this.parentVertexSchema = parentVertexSchema;
        this.parentDirection = parentDirection;
//        JSONObject childVertexJson = this.json.getJSONObject("vertex");
        this.childVertexSchema = createVertexSchema("vertex");

        client.validateNested(index, type, path);
    }

    protected VertexSchema createVertexSchema(String key) throws JSONException {
        JSONObject vertexConfiguration = this.json.optJSONObject(key);
        if (vertexConfiguration == null) return null;
        if (vertexConfiguration.optBoolean("ref", false)) return new ReferenceVertexSchema(vertexConfiguration, graph);
        return new NestedVertexSchema(vertexConfiguration, path, index, type, client, graph);
    }

    @Override
    public Set<ElementSchema> getChildSchemas() {
        return Sets.newHashSet(childVertexSchema);
    }

    @Override
    public Collection<Edge> fromFields(Map<String, Object> fields) {
        Object pathValue = fields.get(this.path);
        if (pathValue == null) return null;

        Vertex parentVertex = parentVertexSchema.createElement(fields);
        if (parentVertex == null) return null;

        if (pathValue instanceof Collection) {
            List<Edge> edges = new ArrayList<>(((Collection) pathValue).size());
            Collection<Map<String, Object>> edgesFields = (Collection<Map<String, Object>>) pathValue;
            for (Map<String, Object> edgeFields : edgesFields) {
                UniEdge edge = createEdge(parentVertex, edgeFields);
                if (edge == null) continue;
                edges.add(edge);
            }
            return edges;
        } else if (pathValue instanceof Map) {
            Map<String, Object> edgeFields = (Map<String, Object>) pathValue;
            UniEdge edge = createEdge(parentVertex, edgeFields);
            return Collections.singleton(edge);
        }
        return null;
    }

    private UniEdge createEdge(Vertex parentVertex, Map<String, Object> edgeFields) {
        Map<String, Object> edgeProperties = getProperties(edgeFields);
        if (edgeProperties == null) return null;
        Vertex childVertex = childVertexSchema.createElement(edgeFields);
        if (childVertex == null) return null;
        UniEdge edge = new UniEdge(edgeProperties,
                parentDirection.equals(Direction.OUT) ? parentVertex : childVertex,
                parentDirection.equals(Direction.IN) ? parentVertex : childVertex, graph);
        return edge;
    }

    @Override
    public Map<String, Object> toFields(Edge edge) {
        Map<String, Object> parentFields = getVertexFields(edge, parentDirection);
        if (parentFields == null) return null;
        Map<String, Object> edgeFields = getFields(edge);
        Map<String, Object> childFields = getVertexFields(edge, parentDirection.opposite());
        Map<String, Object> nestedFields = ConversionUtils.merge(Lists.newArrayList(edgeFields, childFields), this::mergeFields, false);
        if (nestedFields == null) return null;
        parentFields.put(this.path, new Object[]{nestedFields});
        return parentFields;
    }

    private Map<String, Object> getVertexFields(Edge edge, Direction direction) {
        VertexSchema vertexSchema = direction.equals(parentDirection) ? parentVertexSchema : childVertexSchema;
        Vertex parent = edge.vertices(direction).next();
        return vertexSchema.toFields(parent);
    }

    @Override
    public Set<String> toFields(Set<String> propertyKeys) {
        Set<String> fields = super.toFields(propertyKeys).stream()
                .map(key -> path + "." + key).collect(Collectors.toSet());
        Set<String> parentFields = parentVertexSchema.toFields(propertyKeys);
        fields.addAll(parentFields);
        Set<String> childFields = childVertexSchema.toFields(propertyKeys).stream()
                .map(key -> path + "." + key).collect(Collectors.toSet());
        fields.addAll(childFields);
        return fields;
    }

    @Override
    public PredicatesHolder toPredicates(PredicatesHolder predicatesHolder) {
        return super.toPredicates(predicatesHolder).map(has -> new HasContainer(path + "." + has.getKey(), has.getPredicate()));
    }

    @Override
    public PredicatesHolder toPredicates(List<Vertex> vertices, Direction direction, PredicatesHolder predicates) {
        PredicatesHolder edgePredicates = this.toPredicates(predicates);
        PredicatesHolder vertexPredicates = this.getVertexPredicates(vertices, direction);
        return PredicatesHolderFactory.and(edgePredicates, vertexPredicates);
    }

    protected PredicatesHolder getVertexPredicates(List<Vertex> vertices, Direction direction) {
        PredicatesHolder parentPredicates = parentVertexSchema.toPredicates(vertices);
        PredicatesHolder childPredicates = childVertexSchema.toPredicates(vertices)
                .map(has -> new HasContainer(path + "." + has.getKey(), has.getPredicate()));
        if (direction.equals(parentDirection)) return parentPredicates;
        if (direction.equals(parentDirection.opposite())) return childPredicates;
        return PredicatesHolderFactory.or(parentPredicates, childPredicates); //Direction.BOTH
    }

    @Override
    public QueryBuilder createQueryBuilder(PredicatesHolder predicatesHolder) {
        QueryBuilder queryBuilder = FilterHelper.createFilterBuilder(predicatesHolder);
        queryBuilder = QueryBuilders.nestedQuery(this.path, queryBuilder).innerHit(new QueryInnerHitBuilder().setFetchSource(false));
        queryBuilder = QueryBuilders.indicesQuery(queryBuilder, index).noMatchQuery("none");
        queryBuilder = QueryBuilders.constantScoreQuery(queryBuilder);
        return queryBuilder;
    }

    @Override
    public BulkableAction<DocumentResult> addElement(Edge edge) {
        Document document = toDocument(edge);
        if (document == null) return null;
        Map<String, Object> edgeFields = getFields(edge);
        Map<String, Object> childFields = getVertexFields(edge, parentDirection.opposite());
        Map<String, Object> nestedFields = ConversionUtils.merge(Lists.newArrayList(edgeFields, childFields), this::mergeFields, false);
        if (nestedFields == null) return null;
        Set<String> idField = propertySchemas.stream()
                .map(schema -> schema.toFields(Collections.singleton(T.id.getAccessor()))).findFirst().get();
        try {
            HashMap<String, Object> params = new HashMap<>();
            params.put("nestedDoc", nestedFields);
            params.put("path", path);
            params.put("edgeId", edge.id());
            params.put("idField", idField.iterator().next());
            HashMap<String, Object> docMap = new HashMap<>();
            docMap.put("params", params);
            docMap.put("script", UPDATE_SCRIPT);
            docMap.put("upsert", document.getFields());
            String json = mapper.writeValueAsString(docMap);
            return new Update.Builder(json).index(document.getIndex()).type(document.getType()).id(document.getId()).build();
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            return null;
        }
    }

    static String UPDATE_SCRIPT = "if (!ctx._source.containsKey(path)) {ctx._source[path] = [nestedDoc]}; " +
            "else { items_to_remove = []; ctx._source[path].each { item -> if (item[idField] == edgeId) { items_to_remove.add(item); } };" +
            "items_to_remove.each { item -> ctx._source[path].remove(item) }; ctx._source[path] += nestedDoc;}";
}
