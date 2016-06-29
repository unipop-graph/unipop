package org.unipop.elastic.document.schema;

import io.searchbox.core.Search;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.shaded.jackson.databind.JsonNode;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.unipop.elastic.common.FilterHelper;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;
import org.unipop.query.search.SearchQuery;
import org.unipop.schema.base.BaseElementSchema;
import org.unipop.schema.property.PropertySchema;
import org.unipop.structure.UniGraph;

import java.io.IOException;
import java.util.*;

public abstract class DocSchema<E extends Element> extends BaseElementSchema<E> {

    private String type;
    private String index;

    public DocSchema(String index, String type, List<PropertySchema> propertySchemas, UniGraph graph) {
        super(propertySchemas, graph);
        this.type = type;
        this.index = index;
    }

    protected String getType() {
        return type;
    }

    protected String getIndex() {
        return index;
    }

    public Document toDocument(E element) {
        Map<String, Object> fields = this.toFields(element);
        if(fields == null) return null;
        String type = ObjectUtils.firstNonNull(fields.remove("_type"), this.getType()).toString();
        if (!checkType(type)) return null;
        String id = ObjectUtils.firstNonNull(fields.remove("_id"), fields.remove("id"), element.id()).toString();
        String index = getIndex();
        return new Document(index, type, id, fields);
    }

    public E fromDocument(Document document){
        if(!checkType(document.getType())) return null;
        if(!checkIndex(document.getIndex())) return null;
        Map<String, Object> fields = document.getFields();
        fields.put("_id", document.getId());
        fields.put("_type", document.getType());
        return this.fromFields(fields);
    }

    public Search toPredicates(SearchQuery<E> query){
        PredicatesHolder predicatesHolder = toPredicates(query.getPredicates());
        return getSearch(query, predicatesHolder);
    }

    protected Search getSearch(SearchQuery<E> query, PredicatesHolder predicatesHolder) {
        if (predicatesHolder.isAborted()) return null;

        if (predicatesHolder.findKey("_type") == null && getType() != null) {
            PredicatesHolder type = PredicatesHolderFactory.predicate(new HasContainer("_type", P.eq(getType())));
            predicatesHolder = PredicatesHolderFactory.and(predicatesHolder, type);
        }

        QueryBuilder queryBuilder = createQueryBuilder(predicatesHolder);
        return createSearch(query, queryBuilder);
    }

    protected QueryBuilder createQueryBuilder(PredicatesHolder predicatesHolder) {
        QueryBuilder queryBuilder = FilterHelper.createFilterBuilder(predicatesHolder);
        queryBuilder = QueryBuilders.constantScoreQuery(queryBuilder);
        queryBuilder = QueryBuilders.indicesQuery(queryBuilder, getIndex()).noMatchQuery("none");
        return queryBuilder;
    }

    protected Search createSearch(SearchQuery<E> query, QueryBuilder queryBuilder) {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(queryBuilder)
                .size(query.getLimit() == -1 ? 10000 : query.getLimit());

        if(query.getPropertyKeys() == null) searchSourceBuilder.fetchSource(true);
        else {
            Set<String> fields = toFields(query.getPropertyKeys());
            if(fields.size() == 0) searchSourceBuilder.fetchSource(false);
            else searchSourceBuilder.fetchSource(fields.toArray(new String[fields.size()]), null);
        }

        return new Search.Builder(searchSourceBuilder.toString().replace("\n", "")).build();
    }

    public List<E> parseResults(String result) {
        ObjectMapper mapper = new ObjectMapper();
        List<E> results = new ArrayList<>();
        try {
            JsonNode hits = mapper.readTree(result).get("hits").get("hits");
            for (JsonNode hit : hits) {
                Map<String, Object> source = mapper.readValue(hit.get("_source").toString(), Map.class);
                Document document = new Document(hit.get("_index").asText(), hit.get("_type").asText(), hit.get("_id").asText(), source);
                E element = fromDocument(document);
                if(element != null) results.add(element);
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return results;
    }

    boolean checkIndex(String index) {
        return getIndex().equals(index);
    }

    boolean checkType(String type) {
        return type != null && (getType() == null || getType().equals(type));
    }


    public class Document {
        private final String index;
        private final String type;
        private final String id;
        private final Map<String, Object> fields;

        public Document(String index, String type, String id, Map<String, Object> fields) {
            this.index = index;
            this.type = type;
            this.id = id;
            this.fields = fields;
        }

        public String getIndex() {
            return index;
        }

        public String getType() {
            return type;
        }

        public String getId() {
            return id;
        }

        public Map<String, Object> getFields() {
            return fields;
        }
    }
}
