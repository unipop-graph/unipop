package org.unipop.elastic.controller.schema;

import com.google.common.collect.FluentIterable;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.unipop.controller.Predicates;
import org.unipop.controller.aggregation.SemanticKeyTraversal;
import org.unipop.controller.aggregation.SemanticReducerTraversal;
import org.unipop.elastic.controller.schema.helpers.*;
import org.unipop.elastic.controller.schema.helpers.aggregationConverters.*;
import org.unipop.elastic.controller.schema.helpers.elementConverters.ElementConverter;
import org.unipop.elastic.controller.schema.helpers.elementConverters.utils.ElementFactory;
import org.unipop.elastic.controller.schema.helpers.elementConverters.utils.RecycledElementFactory;
import org.unipop.elastic.controller.schema.helpers.elementConverters.utils.SearchHitElement;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphElementSchemaProvider;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.SearchHit;
import org.unipop.elastic.helpers.ElasticMutations;
import org.unipop.elastic.helpers.TimingAccessor;
import org.unipop.structure.BaseElement;
import org.unipop.structure.UniGraph;

import java.util.*;

/**
 * Created by Gilad on 14/10/2015.
 */
public abstract class SchemaElementController {
    SchemaElementController(
            UniGraph graph,
            GraphElementSchemaProvider schemaProvider,
            Client client,
            ElasticMutations elasticMutations,
            ElasticGraphConfiguration elasticGraphConfiguration,
            ElementConverter<Element, Element> elementConverter
    ) {
        this.schemaProvider = schemaProvider;
        this.graph = graph;
        this.client = client;
        this.elasticMutations = elasticMutations;
        this.configuration = elasticGraphConfiguration;
        this.elementConverter = elementConverter;
        this.timingAccessor = new TimingAccessor();
        this.searchHitElementFactory = new RecycledElementFactory<>(Arrays.asList(
                new SearchHitElement(null, graph),
                new SearchHitElement(null, graph),
                new SearchHitElement(null, graph),
                new SearchHitElement(null, graph),
                new SearchHitElement(null, graph)
        ));
    }

    protected Iterator<? extends Element> elements(Class elementType) {
        SearchBuilder searchBuilder = buildElementsQuery(elementType);
        Iterable<SearchHit> scrollIterable = getSearchHits(searchBuilder);
        return transformSearchHitsToElements(scrollIterable);
    }

    protected Iterator<? extends Element> elements(Object[] elementsIds, Class elementType) {
        SearchBuilder searchBuilder = buildElementsQuery(elementsIds, elementType);
        Iterable<SearchHit> scrollIterable = getSearchHits(searchBuilder);
        return transformSearchHitsToElements(scrollIterable);
    }

    protected Iterator<? extends Element> elements(Predicates predicates, Class elementType) {
        SearchBuilder searchBuilder = buildElementsQuery(predicates, elementType);
        Iterable<SearchHit> scrollIterable = getSearchHits(searchBuilder);
        return transformSearchHitsToElements(scrollIterable);
    }

    protected SearchBuilder buildElementsQuery(Class elementType) {
        this.elasticMutations.refreshIfDirty();
        SearchBuilder searchBuilder = new SearchBuilder();
        searchBuilder.getIncludeSourceFields().add("*");
        searchBuilder.setLimit(configuration.getElasticGraphDefaultSearchSize());
        searchBuilder.getQueryBuilder().seekRoot().query().filtered().query().matchAll();

        translateLabelsPredicate(Collections.emptyList(), searchBuilder, elementType);
        return searchBuilder;
    }

    protected SearchBuilder buildElementsQuery(Object[] elementsIds, Class elementType) {
        SearchBuilder searchBuilder = buildElementsQuery(elementType);

        // Add to the elastic query a condition for each of the given vertex ids:
        if (elementsIds != null && elementsIds.length > 0) {
            appendIds(elementsIds, searchBuilder);
        }

        return searchBuilder;
    }

    protected SearchBuilder buildElementsQuery(Predicates predicates, Class elementType) {
        SearchBuilder searchBuilder = buildElementsQuery(elementType);

        translateHasContainers(searchBuilder, predicates.hasContainers);
        translateLimits(predicates.limitHigh, predicates.limitLow, searchBuilder);
        return searchBuilder;
    }

    protected SearchBuilder buildElementsQuery(Predicates predicates, Object[] elementsIds, Class elementType) {
        SearchBuilder searchBuilder = buildElementsQuery(elementType);

        translateHasContainers(searchBuilder, predicates.hasContainers);
        translateLimits(predicates.limitHigh, predicates.limitLow, searchBuilder);

        if (elementsIds != null && elementsIds.length > 0) {
            appendIds(elementsIds, searchBuilder);
        }

        return searchBuilder;
    }

    private void appendIds(Object[] elementsIds, SearchBuilder searchBuilder) {
        // Add to the elastic query a condition for each of the given vertex ids:
        if (elementsIds != null && elementsIds.length > 0) {
            Iterable<String> properIds = FluentIterable.from(Arrays.asList(elementsIds))
                    .filter(id -> id != null).transform(id -> id.toString());

            searchBuilder.getQueryBuilder().seekRoot().query().filtered().filter().bool()
                    .must().ids(properIds, FluentIterable.from(searchBuilder.getTypes()).toArray(String.class));
        }
    }

    protected abstract Iterator<? extends Element> transformSearchHitsToElements(Iterable<SearchHit> scrollIterable);

    protected void translateLabelsPredicate(Iterable<String> labels, SearchBuilder searchBuilder, Class elementType) {
        if (labels != null && FluentIterable.from(labels).size() > 0) {
            SearchBuilderHelper.applyIndices(searchBuilder, schemaProvider, labels, elementType);
            SearchBuilderHelper.applyTypes(searchBuilder, schemaProvider, labels, elementType);
        } else {
            SearchBuilderHelper.applyIndices(searchBuilder, schemaProvider, elementType);
            SearchBuilderHelper.applyTypes(searchBuilder, schemaProvider, elementType);
        }
    }

    protected void translateLimits(long limitHigh, long limitLow, SearchBuilder searchBuilder) {
        searchBuilder.setLimit(limitHigh);
        //TODO: implement limit low
    }

    protected void translateHasContainers(SearchBuilder searchBuilder, ArrayList<HasContainer> hasContainers) {
        HasContainersTranslator hasContainersTranslator = new HasContainersTranslator();
        hasContainersTranslator.updateSearchBuilder(searchBuilder, hasContainers);
    }

    protected Iterable<SearchHit> getSearchHits(SearchBuilder searchBuilder) {
        SearchRequestBuilder searchRequest = searchBuilder.getSearchRequest(client);
        if (searchRequest == null) {
            return Collections.emptyList();
        }

        Iterable<SearchHit> scrollIterable = new SearchHitScrollIterable(
                configuration,
                searchRequest,
                searchBuilder.getLimit(),
                client);

        return scrollIterable;
    }

    protected MapAggregationConverter getAggregationConverter(AggregationBuilder aggregationBuilder) {

        MapAggregationConverter mapAggregationConverter = new MapAggregationConverter();

        FilteredMapAggregationConverter filteredMapAggregationConverter = new FilteredMapAggregationConverter(
                aggregationBuilder,
                mapAggregationConverter);

        FilteredMapAggregationConverter filteredStatsAggregationConverter = new FilteredMapAggregationConverter(
                aggregationBuilder,
                new StatsAggregationConverter()
        );

        CompositeAggregationConverter compositeAggregationConverter = new CompositeAggregationConverter(
                filteredMapAggregationConverter,
                filteredStatsAggregationConverter,
                new SingleValueAggregationConverter()
        );

        mapAggregationConverter.setInnerConverter(compositeAggregationConverter);
        mapAggregationConverter.setUseSimpleFormat(true);
        return mapAggregationConverter;
    }

    protected void applyAggregationBuilder(AggregationBuilder aggregationBuilder, Traversal keyTraversal, Traversal reducerTraversal) {
        if (SemanticKeyTraversal.class.isAssignableFrom(keyTraversal.getClass())) {
            SemanticKeyTraversal semanticKeyTraversal = (SemanticKeyTraversal) keyTraversal;
            aggregationBuilder.terms("key")
                    .field(semanticKeyTraversal.getKey())
                    .size(this.configuration.getElasticGraphAggregationsDefaultTermsSize())
                    .shardSize(this.configuration.getElasticGraphAggregationsDefaultTermsShardSize())
                    .executionHint(this.configuration.getElasticGraphAggregationsDefaultTermsExecutonHint());

            if (reducerTraversal != null && SemanticReducerTraversal.class.isAssignableFrom(reducerTraversal.getClass())) {
                SemanticReducerTraversal semanticReducerTraversalInstance = (SemanticReducerTraversal)reducerTraversal;
                String reduceAggregationName = "reduce";
                switch (semanticReducerTraversalInstance.getType()) {
                    case count:
                        aggregationBuilder.count(reduceAggregationName)
                                .field(semanticReducerTraversalInstance.getKey());
                        break;

                    case min:
                        aggregationBuilder.min(reduceAggregationName)
                                .field(semanticReducerTraversalInstance.getKey());
                        break;

                    case max:
                        aggregationBuilder.max(reduceAggregationName)
                                .field(semanticReducerTraversalInstance.getKey());
                        break;

                    case cardinality:
                        aggregationBuilder.cardinality(reduceAggregationName)
                                .field(semanticReducerTraversalInstance.getKey())
                                .precisionThreshold(1000L);
                        break;
                }
            }
        }
    }

    protected ElementFactory<SearchHit,SearchHitElement> getSearchHitElementFactory() {
        return searchHitElementFactory;
    }

    protected ElementConverter<Element,Element> getElementConverter() {
        return elementConverter;
    }

    protected ElasticGraphConfiguration getConfiguration() {
        return this.configuration;
    }

    //region properties
    protected final GraphElementSchemaProvider schemaProvider;
    protected Client client = null;
    protected UniGraph graph;
    protected ElasticMutations elasticMutations;
    protected ElementConverter<Element, Element> elementConverter;
    protected ElementFactory<SearchHit, SearchHitElement> searchHitElementFactory;
    protected ElasticGraphConfiguration configuration;
    protected TimingAccessor timingAccessor;
    //endregion

}
