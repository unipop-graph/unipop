package org.unipop.elastic.document.schema;

import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.search.Hit;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.javatuples.Pair;
import org.json.JSONException;
import org.json.JSONObject;
import org.unipop.elastic.common.ElasticClient;
import org.unipop.elastic.common.FilterHelper;
import org.unipop.elastic.document.Document;
import org.unipop.elastic.document.DocumentSchema;
import org.unipop.elastic.document.schema.property.IndexPropertySchema;
import org.unipop.query.predicates.PredicateQuery;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.search.SearchQuery;
import org.unipop.schema.element.AbstractElementSchema;
import org.unipop.structure.UniGraph;
import org.unipop.util.PropertySchemaFactory;

import java.util.*;

public abstract class AbstractDocSchema<E extends Element> extends AbstractElementSchema<E> implements DocumentSchema<E> {
    protected final ElasticClient client;
    protected IndexPropertySchema index;

    public AbstractDocSchema(JSONObject configuration, ElasticClient client, UniGraph graph) throws JSONException {
        super(configuration, graph);
        this.client = client;
        this.index = (IndexPropertySchema) PropertySchemaFactory.createPropertySchema("index", json.opt("index"), this);
        if (index != null) index.addValidation(client::validateIndex);
    }

    @Override
    public Query getSearch(SearchQuery<E> query) {
        PredicatesHolder predicatesHolder = this.toPredicates(query.getPredicates());
        if (predicatesHolder.getClause().equals(PredicatesHolder.Clause.Abort)) return null;
        return FilterHelper.createFilterBuilder(predicatesHolder);
    }

    @Override
    public SearchRequest.Builder buildSearch(SearchQuery<E> query, Query q) {
        SearchRequest.Builder b = new SearchRequest.Builder()
                .query(q)
                .size(query.getLimit() == -1 ? 10000 : query.getLimit());
        if (query.getPropertyKeys() == null) {
            b.source(s -> s.fetch(true));
        } else {
            Set<String> fields = toFields(query.getPropertyKeys());
            if (fields.isEmpty()) b.source(s -> s.fetch(false));
            else b.source(s -> s.filter(f -> f.includes(new ArrayList<>(fields))));
        }
        List<Pair<String, Order>> orders = query.getOrders();
        if (orders != null) {
            for (Pair<String, Order> order : orders) {
                String field = getFieldByPropertyKey(order.getValue0());
                Order orderValue = order.getValue1();
                if (orderValue == Order.desc) {
                    b.sort(so -> so.field(fb -> fb.field(field).order(SortOrder.Desc)));
                } else if (orderValue == Order.asc) {
                    b.sort(so -> so.field(fb -> fb.field(field).order(SortOrder.Asc)));
                }
                // shuffle: no sort applied
            }
        }
        return b;
    }

    @Override
    public List<E> parseResults(List<Hit<Map<String, Object>>> hits, PredicateQuery query) {
        List<E> results = new ArrayList<>();
        for (Hit<Map<String, Object>> hit : hits) {
            Map<String, Object> source = hit.source() != null ? new HashMap<>(hit.source()) : new HashMap<>();
            Document document = new Document(hit.index(), hit.id(), source);
            Collection<E> elements = fromDocument(document);
            if (elements != null) {
                elements.forEach(element -> {
                    if (element != null && query.test(element, query.getPredicates())) results.add(element);
                });
            }
        }
        return results;
    }

    public Document toDocument(E element) {
        Map<String, Object> fields = this.toFields(element);
        if (fields == null) return null;
        String id = ObjectUtils.firstNonNull(fields.remove("_id"), fields.remove("id"), element.id()).toString();
        return new Document(index.getIndex(fields), id, fields);
    }

    protected Collection<E> fromDocument(Document document) {
        if (!checkIndex(document.getIndex())) return null;
        Map<String, Object> fields = document.getFields();
        fields.put("_id", document.getId());
        return this.fromFields(fields);
    }

    @Override
    public BulkOperation addElement(E element, boolean create) {
        Document document = toDocument(element);
        if (document == null) return null;
        return BulkOperation.of(op -> op.index(idx -> idx
                .index(document.getIndex())
                .id(document.getId())
                .document(document.getFields())));
    }

    @Override
    public BulkOperation delete(E element) {
        Document document = toDocument(element);
        if (document == null) return null;
        return BulkOperation.of(op -> op.delete(d -> d
                .index(document.getIndex())
                .id(document.getId())));
    }

    @Override
    public IndexPropertySchema getIndex() {
        return index;
    }

    protected boolean checkIndex(String index) {
        return this.index.validateIndex(index);
    }
}
