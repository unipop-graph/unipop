package org.unipop.elastic.document.schema.nested;

import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.ChildScoreMode;
import co.elastic.clients.elasticsearch._types.query_dsl.NestedQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.json.JSONException;
import org.json.JSONObject;
import org.unipop.elastic.common.ElasticClient;
import org.unipop.elastic.common.FilterHelper;
import org.unipop.elastic.document.Document;
import org.unipop.elastic.document.DocumentEdgeSchema;
import org.unipop.elastic.document.schema.AbstractDocSchema;
import org.unipop.elastic.document.schema.DocVertexSchema;
import org.unipop.elastic.document.schema.property.IndexPropertySchema;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;
import org.unipop.query.search.SearchQuery;
import org.unipop.query.search.SearchVertexQuery;
import org.unipop.schema.element.ElementSchema;
import org.unipop.schema.element.VertexSchema;
import org.unipop.structure.UniEdge;
import org.unipop.structure.UniGraph;
import org.unipop.util.ConversionUtils;

import java.util.*;
import java.util.stream.Collectors;

public class NestedEdgeSchema extends AbstractDocSchema<Edge> implements DocumentEdgeSchema {
    private String path;
    private final DocVertexSchema parentVertexSchema;
    private final VertexSchema childVertexSchema;
    private final Direction parentDirection;

    public NestedEdgeSchema(DocVertexSchema parentVertexSchema, Direction parentDirection, IndexPropertySchema index, String path, JSONObject configuration, ElasticClient client, UniGraph graph) throws JSONException {
        super(configuration, client, graph);
        this.index = index;
        this.path = path;
        this.parentVertexSchema = parentVertexSchema;
        this.parentDirection = parentDirection;
        this.childVertexSchema = createVertexSchema("vertex");
    }

    protected VertexSchema createVertexSchema(String key) throws JSONException {
        JSONObject vertexConfiguration = this.json.optJSONObject(key);
        if (vertexConfiguration == null) return null;
        if (vertexConfiguration.optBoolean("ref", false)) return new NestedReferenceVertexSchema(vertexConfiguration, path, graph);
        return new NestedVertexSchema(vertexConfiguration, path, index, client, graph);
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
                parentDirection.equals(Direction.IN) ? parentVertex : childVertex, this, graph);
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
        Set<String> childFields = childVertexSchema.toFields(propertyKeys);
        fields.addAll(childFields);
        return fields;
    }

    @Override
    public String getFieldByPropertyKey(String key) {
        String field = super.getFieldByPropertyKey(key);
        if (field == null) return field;
        return path + "." + field;
    }

    @Override
    public PredicatesHolder toPredicates(PredicatesHolder predicatesHolder) {
        return super.toPredicates(predicatesHolder).map(has -> new HasContainer(path + "." + has.getKey(), has.getPredicate()));
    }

    @Override
    public Query getSearch(SearchQuery<Edge> query) {
        PredicatesHolder predicatesHolder = this.toPredicates(query.getPredicates());
        Query innerQuery = FilterHelper.createFilterBuilder(predicatesHolder);
        if (innerQuery == null) return null;
        return NestedQuery.of(n -> n.path(this.path).query(innerQuery).scoreMode(ChildScoreMode.None))._toQuery();
    }

    @Override
    public Query getSearch(SearchVertexQuery query) {
        return createQueryBuilder(query);
    }

    public Query createQueryBuilder(SearchVertexQuery query) {
        PredicatesHolder edgePredicates = this.toPredicates(query.getPredicates());
        if (edgePredicates.isAborted()) return null;

        PredicatesHolder childPredicates = childVertexSchema.toPredicates(query.getVertices());
        childPredicates = PredicatesHolderFactory.and(edgePredicates, childPredicates);
        Query childQuery = createNestedQueryBuilder(childPredicates);

        if (query.getDirection().equals(parentDirection.opposite())) {
            if (childPredicates.isAborted()) return null;
            return childQuery;
        } else if (!query.getDirection().equals(Direction.BOTH)) childQuery = null;

        PredicatesHolder parentPredicates = parentVertexSchema.toPredicates(query.getVertices());
        Query parentQuery = FilterHelper.createFilterBuilder(parentPredicates);
        if (parentPredicates.isAborted()) parentQuery = null;
        if (parentQuery != null) {
//            if (parentPredicates.isAborted()) return null;
            Query edgeQuery = createNestedQueryBuilder(edgePredicates);
            if (edgeQuery != null) {
                final Query pq = parentQuery;
                final Query eq = edgeQuery;
                parentQuery = BoolQuery.of(b -> b.must(pq).must(eq))._toQuery();
            }
        }
        if (query.getDirection().equals(parentDirection) && parentPredicates.notAborted()) return parentQuery;
        else if (childQuery == null && parentPredicates.notAborted()) return parentQuery;
        else if (parentQuery == null && childPredicates.notAborted()) return childQuery;
        else if (parentPredicates.isAborted() && childPredicates.isAborted()) return null;
        else {
            final Query pq = parentQuery;
            final Query cq = childQuery;
            return BoolQuery.of(b -> b.should(pq).should(cq))._toQuery();
        }
    }

    private Query createNestedQueryBuilder(PredicatesHolder nestedPredicates) {
        if (nestedPredicates.isAborted()) return null;
        Query innerQuery = FilterHelper.createFilterBuilder(nestedPredicates);
        if (innerQuery == null) return null;
        return NestedQuery.of(n -> n.path(this.path).query(innerQuery).scoreMode(ChildScoreMode.None))._toQuery();
    }

    @Override
    public BulkOperation addElement(Edge edge, boolean create) {
        //TODO: use the 'create' parameter to differentiate between add and update
        // TODO(nested best-effort): Painless scripted upsert (Groovy removed in ES 8); currently falls back to a plain
        // index operation for the parent document including the nested array. Nested-edge process tests are skipped
        // in Phase 1, so this does not block the module compile. Implement a proper Painless scripted upsert in Phase 2.
        Vertex parentVertex = parentDirection.equals(Direction.OUT) ? edge.outVertex() : edge.inVertex();
        Document parentDoc = parentVertexSchema.toDocument(parentVertex);
        if (parentDoc == null) return null;
        Map<String, Object> edgeFields = getFields(edge);
        Map<String, Object> childFields = getVertexFields(edge, parentDirection.opposite());
        Map<String, Object> nestedFields = ConversionUtils.merge(Lists.newArrayList(edgeFields, childFields), this::mergeFields, false);
        if (nestedFields == null) return null;

        // Build the full document with the nested array embedded (best-effort plain index, no script)
        Map<String, Object> docFields = new HashMap<>(parentDoc.getFields());
        docFields.put(this.path, new Object[]{nestedFields});

        return BulkOperation.of(op -> op.index(idx -> idx
                .index(parentDoc.getIndex())
                .id(parentDoc.getId())
                .document(docFields)));
    }
}
