package org.unipop.elastic.document.schema;

import io.searchbox.action.BulkableAction;
import io.searchbox.core.DocumentResult;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.support.QueryInnerHitBuilder;
import org.json.JSONException;
import org.json.JSONObject;
import org.unipop.elastic.common.ElasticClient;
import org.unipop.elastic.common.FilterHelper;
import org.unipop.elastic.document.DocumentVertexSchema;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;
import org.unipop.structure.UniElement;
import org.unipop.structure.UniGraph;
import org.unipop.structure.UniVertex;
import org.unipop.util.ConversionUtils;

import java.util.*;
import java.util.stream.Collectors;

public class NestedVertexSchema extends AbstractDocSchema<Vertex> implements DocumentVertexSchema {
    private String path;

    public NestedVertexSchema(JSONObject configuration, String path, String index, String type, ElasticClient client, UniGraph graph) throws JSONException {
        super(configuration, client, graph);
        this.path = path;
        this.index = index;
        this.type = type;
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
        return new UniVertex(properties, graph);
    }

    @Override
    public BulkableAction<DocumentResult> addElement(Vertex element) {
        return null;
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
    public PredicatesHolder toPredicates(PredicatesHolder predicatesHolder) {
        return super.toPredicates(predicatesHolder).map(has -> new HasContainer(path + "." + has.getKey(), has.getPredicate()));
    }
}
