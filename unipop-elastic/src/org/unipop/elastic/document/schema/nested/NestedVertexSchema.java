package org.unipop.elastic.document.schema.nested;

import co.elastic.clients.elasticsearch._types.query_dsl.ChildScoreMode;
import co.elastic.clients.elasticsearch._types.query_dsl.NestedQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.json.JSONException;
import org.json.JSONObject;
import org.unipop.elastic.common.ElasticClient;
import org.unipop.elastic.common.FilterHelper;
import org.unipop.elastic.document.DocumentVertexSchema;
import org.unipop.elastic.document.schema.AbstractDocSchema;
import org.unipop.elastic.document.schema.property.IndexPropertySchema;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.search.DeferredVertexQuery;
import org.unipop.structure.UniElement;
import org.unipop.structure.UniGraph;
import org.unipop.structure.UniVertex;
import org.unipop.util.ConversionUtils;

import java.util.*;
import java.util.stream.Collectors;

public class NestedVertexSchema extends AbstractDocSchema<Vertex> implements DocumentVertexSchema {
    private String path;

    public NestedVertexSchema(JSONObject configuration, String path, IndexPropertySchema index, ElasticClient client, UniGraph graph) throws JSONException {
        super(configuration, client, graph);
        this.path = path;
        this.index = index;
    }

    @Override
    protected Map<String, Object> getFields(Vertex element) {
        Map<String, Object> properties = UniElement.fullProperties(element);
        List<Map<String, Object>> fieldMaps = this.getPropertySchemas().stream().map(schema ->
                schema.toFields(properties)).filter(prop -> prop != null).collect(Collectors.toList());
        return ConversionUtils.merge(fieldMaps, this::mergeFields, false);
    }

    @Override
    public Set<String> toFields(Set<String> propertyKeys) {
        return super.toFields(propertyKeys).stream().map(key -> path + "." + key).collect(Collectors.toSet());
    }

    @Override
    public String getFieldByPropertyKey(String key) {
        String field = super.getFieldByPropertyKey(key);
        if (field == null) return field;
        return path + "." + field;
    }

    @Override
    public Collection<Vertex> fromFields(Map<String, Object> fields) {
        Object pathValue = fields.get(this.path);
        if (pathValue == null) return null;
        if (pathValue instanceof Collection) {
            List<Vertex> vertices = new ArrayList<>(((Collection) pathValue).size());
            Collection<Map<String, Object>> verticesFields = (Collection<Map<String, Object>>) pathValue;
            verticesFields.forEach(vertexField -> {
                Vertex vertex = createElement(vertexField);
                if (vertex != null)
                    vertices.add(vertex);
            });
            return vertices;
        } else if (pathValue instanceof Map) {
            Map<String, Object> vertexField = (Map<String, Object>) pathValue;
            Vertex vertex = createElement(vertexField);
            if (vertex != null)
                return Collections.singleton(vertex);
        }
        return null;
    }

    @Override
    public Vertex createElement(Map<String, Object> fields) {
        Map<String, Object> properties = getProperties(fields);
        if (properties == null) return null;
        return new UniVertex(properties, this, graph);
    }

    @Override
    public co.elastic.clients.elasticsearch.core.bulk.BulkOperation addElement(Vertex element, boolean create) {
        return null;
    }

    /** Build a nested query wrapping an inner ES 8 Query for this path. */
    protected Query createNestedQuery(PredicatesHolder predicatesHolder) {
        Query innerQuery = FilterHelper.createFilterBuilder(predicatesHolder);
        if (innerQuery == null) return null;
        return NestedQuery.of(n -> n.path(this.path).query(innerQuery).scoreMode(ChildScoreMode.None))._toQuery();
    }

    @Override
    public PredicatesHolder toPredicates(PredicatesHolder predicatesHolder) {
        return super.toPredicates(predicatesHolder).map(has -> new HasContainer(path + "." + has.getKey(), has.getPredicate()));
    }

    @Override
    public Query getSearch(DeferredVertexQuery query) {
        PredicatesHolder predicatesHolder = this.toPredicates(query.getVertices());
        if (predicatesHolder.isAborted()) return null;
        return createNestedQuery(predicatesHolder);
    }
}
