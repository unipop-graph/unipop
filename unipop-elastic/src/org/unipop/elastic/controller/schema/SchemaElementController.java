package org.unipop.elastic.controller.schema;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.unipop.controller.Predicates;
import org.unipop.elastic.controller.schema.helpers.*;
import org.unipop.elastic.controller.schema.helpers.elementConverters.ElementConverter;
import org.unipop.elastic.controller.schema.helpers.elementConverters.utils.ElementFactory;
import org.unipop.elastic.controller.schema.helpers.elementConverters.utils.RecycledElementFactory;
import org.unipop.elastic.controller.schema.helpers.elementConverters.utils.SearchHitElement;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphElementSchemaProvider;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.search.SearchHit;
import org.unipop.elastic.helpers.AggregationHelper;
import org.unipop.elastic.helpers.ElasticMutations;
import org.unipop.elastic.helpers.TimingAccessor;
import org.unipop.structure.UniGraph;

import java.util.*;
import java.util.stream.StreamSupport;

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

    public SchemaElementController(){}

    protected abstract Iterator<? extends Element> transformSearchHitsToElements(Iterable<SearchHit> scrollIterable);

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

    protected SearchBuilder buildElementsQuery(Predicates predicates, Class elementType) {
        SearchBuilder searchBuilder = buildElementsQuery(elementType);

        translateHasContainers(searchBuilder, predicates.hasContainers);
        translateLimits(predicates.limitHigh, searchBuilder);
        return searchBuilder;
    }

    protected void translateLabelsPredicate(Iterable<String> labels, SearchBuilder searchBuilder, Class elementType) {
        if (labels != null && StreamSupport.stream(labels.spliterator(), false).count() > 0) {
            SearchBuilderHelper.applyIndices(searchBuilder, schemaProvider, labels, elementType);
            SearchBuilderHelper.applyTypes(searchBuilder, schemaProvider, labels, elementType);
        } else {
            SearchBuilderHelper.applyIndices(searchBuilder, schemaProvider, elementType);
            SearchBuilderHelper.applyTypes(searchBuilder, schemaProvider, elementType);
        }
    }

    protected void translateLimits(long limitHigh, SearchBuilder searchBuilder) {
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

    protected void applyAggregationBuilder(AggregationBuilder aggregationBuilder, Traversal keyTraversal, Traversal reducerTraversal) {
        AggregationHelper.applyAggregationBuilder(
                aggregationBuilder,
                keyTraversal,
                reducerTraversal,
                this.configuration.getElasticGraphAggregationsDefaultTermsSize(),
                this.configuration.getElasticGraphAggregationsDefaultTermsShardSize(),
                this.configuration.getElasticGraphAggregationsDefaultTermsExecutonHint()
        );
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
    protected GraphElementSchemaProvider schemaProvider;
    protected Client client = null;
    protected UniGraph graph;
    protected ElasticMutations elasticMutations;
    protected ElementConverter<Element, Element> elementConverter;
    protected ElementFactory<SearchHit, SearchHitElement> searchHitElementFactory;
    protected ElasticGraphConfiguration configuration;
    protected TimingAccessor timingAccessor;
    //endregion

}
