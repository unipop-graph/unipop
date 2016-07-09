package org.unipop.elastic.document;

import io.searchbox.core.MultiSearch;
import io.searchbox.core.MultiSearchResult;
import io.searchbox.core.Search;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MeanGlobalStep;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.elasticsearch.common.collect.Tuple;
import org.unipop.elastic.common.ElasticClient;
import org.unipop.elastic.common.SchemaUtils;
import org.unipop.query.aggregation.reduce.ReduceQuery;
import org.unipop.query.aggregation.reduce.controllers.*;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.search.SearchQuery;
import org.unipop.structure.UniGraph;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author Gur Ronen
 * @since 7/7/2016
 */
public class DocumentReduceController implements CountController, SumController, MeanController, MaxController, MinController {
    private final ElasticClient client;
    private final UniGraph graph;

    private Set<? extends DocumentVertexSchema> vertexSchemas = new HashSet<>();
    private Set<? extends DocumentEdgeSchema> edgeSchemas = new HashSet<>();

    public DocumentReduceController(Set<DocumentSchema> schemas, ElasticClient client, UniGraph graph) {
        this.client = client;
        this.graph = graph;

        Pair<Set<DocumentVertexSchema>, Set<DocumentEdgeSchema>> pair = SchemaUtils.extractDocumentSchemas(schemas);

        this.vertexSchemas = pair.getLeft();
        this.edgeSchemas = pair.getRight();
    }

    @Override
    public long count(ReduceQuery reduceQuery) {
        Function<DocumentSchema, PredicatesHolder> toPredicatesFunction = (schema) -> schema.toPredicates(reduceQuery.getPredicates());

        throw new NotImplementedException("in progress");
    }

    @Override
    public long max(ReduceQuery reduceQuery) {
        return 0;
    }

    @Override
    public MeanGlobalStep.MeanNumber mean(ReduceQuery reduceQuery) {
        return null;
    }

    @Override
    public long min(ReduceQuery reduceQuery) {
        return 0;
    }

    @Override
    public long sum(ReduceQuery query) {
        return 0;
    }

    private <E extends Element, S extends DocumentSchema<E>> Iterator<E> search(Set<? extends S> allSchemas,
                                                                                ReduceQuery reduceQuery,
                                                                                Function<S, PredicatesHolder> toSearchFunction) {
        SearchQuery<E> searchQuery /*= new SearchQuery<>(Vertex.class, reduceQuery.getPredicates(), 0, reduceQuery.getFieldNames())*/;

        Map<S, Search> schemas = allSchemas.stream()
                .map(schema -> Tuple.tuple(schema, toSearchFunction.apply(schema)))
                .filter(tuple -> tuple.v2() != null)
                .map(tuple -> Tuple.tuple(tuple.v1(), tuple.v1().getSearch(searchQuery, tuple.v2())))
                .filter(tuple -> tuple.v2() != null)
                .collect(Collectors.toMap(Tuple::v1, Tuple::v2));

        if(schemas.size() == 0) return EmptyIterator.instance();

        client.refresh();
        MultiSearch multiSearch = new MultiSearch.Builder(schemas.values()).build();
        MultiSearchResult results = client.execute(multiSearch);
        if(results == null || !results.isSucceeded()) {
            return EmptyIterator.instance();
        }
        Iterator<S> schemaIterator = schemas.keySet().iterator();
        return results.getResponses().stream().filter(this::valid).flatMap(result ->
                schemaIterator.next().parseResults(result.searchResult.getJsonString(), searchQuery).stream()).iterator();
    }

    private boolean valid(MultiSearchResult.MultiSearchResponse multiSearchResponse) {
        if(multiSearchResponse.isError) {
            return false;
        }
        return true;
    }

    private <E extends Element> Set<? extends DocumentSchema<E>> getSchemas(Class elementClass) {
        if (Vertex.class.isAssignableFrom(elementClass)) {
            return (Set<? extends DocumentSchema<E>>) vertexSchemas;
        } else {
            return (Set<? extends DocumentSchema<E>>) edgeSchemas;
        }
    }
}
