package org.unipop.elastic.document.schema;

import io.searchbox.core.Search;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.unipop.elastic.common.FilterHelper;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;
import org.unipop.query.search.SearchQuery;
import org.unipop.schema.ElementSchema;

import java.util.Map;
import java.util.Set;

public interface DocSchema<E extends Element> extends ElementSchema<E> {

    String getIndex();
    String getType();

    default Document toDocument(E element) {
        Map<String, Object> fields = this.toFields(element);
        if(fields == null) return null;
        String type = ObjectUtils.firstNonNull(fields.remove("_type"), this.getType()).toString();
        if (!checkType(type)) return null;
        String id = ObjectUtils.firstNonNull(fields.remove("_id"), fields.remove("id"), element.id()).toString();
        String index = getIndex();
        return new Document(index, type, id, fields);
    }

    default E fromDocument(Document document){
        if(!checkType(document.getType())) return null;
        if(!checkIndex(document.getIndex())) return null;
        Map<String, Object> fields = document.getFields();
        fields.put("_id", document.getId());
        fields.put("_type", document.getType());
        return this.fromFields(fields);
    }

    default Search toPredicates(SearchQuery<E> query){
        PredicatesHolder predicatesHolder = toPredicates(query.getPredicates());
        return createSearchSource(query, predicatesHolder);
    }

    default Search createSearchSource(SearchQuery<E> query, PredicatesHolder predicatesHolder) {
        if(predicatesHolder.isAborted()) return null;

        if(predicatesHolder.findKey("_type") == null && getType() != null) {
            PredicatesHolder type = PredicatesHolderFactory.predicate(new HasContainer("_type", P.eq(getType())));
            predicatesHolder = PredicatesHolderFactory.and(predicatesHolder,type);
        }

        QueryBuilder queryBuilder = FilterHelper.createFilterBuilder(predicatesHolder);
        queryBuilder = QueryBuilders.constantScoreQuery(queryBuilder);
        queryBuilder = QueryBuilders.indicesQuery(queryBuilder, getIndex()).noMatchQuery("none");

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

    default boolean checkIndex(String index) {
        return getIndex().equals(index);
    }

    default boolean checkType(String type) {
        return type != null && (getType() == null || getType().equals(type));
    }

    class Document {
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
