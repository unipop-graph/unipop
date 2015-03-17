package com.tinkerpop.gremlin.elastic.process.graph.traversal.strategy;

import com.tinkerpop.gremlin.elastic.elasticservice.ElasticService;
import com.tinkerpop.gremlin.elastic.process.graph.traversal.sideEffect.*;
import com.tinkerpop.gremlin.elastic.structure.ElasticGraph;
import com.tinkerpop.gremlin.process.*;
import com.tinkerpop.gremlin.process.graph.marker.HasContainerHolder;
import com.tinkerpop.gremlin.process.graph.step.map.*;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GraphStep;
import com.tinkerpop.gremlin.process.graph.strategy.AbstractTraversalStrategy;
import com.tinkerpop.gremlin.process.graph.util.HasContainer;
import com.tinkerpop.gremlin.process.util.*;
import com.tinkerpop.gremlin.structure.*;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.*;

import java.util.*;

public class ElasticGraphStepStrategy extends AbstractTraversalStrategy {
    private static final ElasticGraphStepStrategy INSTANCE = new ElasticGraphStepStrategy();

    public static ElasticGraphStepStrategy instance() {
        return INSTANCE;
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal, final TraversalEngine engine) {
        if (engine.equals(TraversalEngine.COMPUTER)) return;

        Step<?, ?> startStep = TraversalHelper.getStart(traversal);

        if(startStep instanceof GraphStep) {
            ElasticGraph graph = (ElasticGraph) ((GraphStep) startStep).getGraph(ElasticGraph.class);
            processStep(startStep, traversal, graph.elasticService);
        }

    }

    private void processStep(Step<?, ?> currentStep, Traversal.Admin<?, ?> traversal, ElasticService elasticService) {
        BoolFilterBuilder boolFilter = FilterBuilders.boolFilter();
        List<String> labels = new ArrayList<>();
        Step<?, ?> nextStep = currentStep.getNextStep();
        while(nextStep instanceof HasContainerHolder) {
            ((HasContainerHolder) nextStep).getHasContainers().forEach((has)->addFilter(boolFilter, has));
            if (nextStep.getLabel().isPresent()) labels.add(nextStep.getLabel().get());
            traversal.removeStep(nextStep);
            nextStep  = nextStep.getNextStep();
        }
        if(currentStep.getLabel().isPresent()){ labels.add(currentStep.getLabel().get()); }
        String[] labelArray = labels.toArray(new String[labels.size()]);

        if (currentStep instanceof GraphStep) {
            final ElasticGraphStep<?> elasticGraphStep = new ElasticGraphStep<>((GraphStep) currentStep, boolFilter, labelArray, elasticService);
            TraversalHelper.replaceStep(currentStep, (Step) elasticGraphStep, traversal);
        }
        else if (currentStep instanceof VertexStep) {
            ElasticVertexStep<Element> elasticVertexStep = new ElasticVertexStep<>((VertexStep) currentStep, boolFilter, labelArray, elasticService);
            TraversalHelper.replaceStep(currentStep, (Step) elasticVertexStep, traversal);
        }
        else if (currentStep instanceof EdgeVertexStep){
            ElasticEdgeVertexStep newSearchStep = new ElasticEdgeVertexStep((EdgeVertexStep)currentStep, boolFilter, labelArray, elasticService);
            TraversalHelper.replaceStep(currentStep, (Step) newSearchStep, traversal);
        }
        else {
            //TODO
        }

        if(!(currentStep instanceof EmptyStep)) processStep(nextStep, traversal, elasticService);
    }

    private void addFilter(BoolFilterBuilder boolFilterBuilder, HasContainer has){
        if (has.predicate instanceof Compare) {
            String predicateString = has.predicate.toString();
            switch (predicateString) {
                case ("eq"):
/*                        if (has.key.equals("~label")) searchStep.setLabel(has.value.toString());
                else if(has.key.equals("~id")) {
                    searchStep.addId(has.value);
                    ids = searchStep.getIds();
                }
                else*/
                    boolFilterBuilder.must(FilterBuilders.termFilter(has.key, has.value));
                    break;
                case ("neq"):
                    boolFilterBuilder.mustNot(FilterBuilders.termFilter(has.key, has.value));
                    break;
                case ("gt"):
                    boolFilterBuilder.must(FilterBuilders.rangeFilter(has.key).gt(has.value));
                    break;
                case ("gte"):
                    boolFilterBuilder.must(FilterBuilders.rangeFilter(has.key).gte(has.value));
                    break;
                case ("lt"):
                    boolFilterBuilder.must(FilterBuilders.rangeFilter(has.key).lt(has.value));
                    break;
                case ("lte"):
                    boolFilterBuilder.must(FilterBuilders.rangeFilter(has.key).lte(has.value));
                    break;
                default:
                    throw new IllegalArgumentException("predicate not supported in has step: " + has.predicate.toString());
            }
        } else if (has.predicate instanceof Contains) {
            if (has.predicate == Contains.without) boolFilterBuilder.mustNot(FilterBuilders.existsFilter(has.key));
            else if (has.predicate == Contains.within) boolFilterBuilder.must(FilterBuilders.existsFilter(has.key));
        } else if (has.predicate instanceof Geo) boolFilterBuilder.must(new GeoShapeFilterBuilder(has.key, GetShapeBuilder(has.value), ((Geo) has.predicate).getRelation()));
        else throw new IllegalArgumentException("predicate not supported by elastic-gremlin: " + has.predicate.toString());
    }

    private ShapeBuilder GetShapeBuilder(Object object) {
        try {
            String geoJson = (String) object;
            XContentParser parser = JsonXContent.jsonXContent.createParser(geoJson);
            parser.nextToken();

            return ShapeBuilder.parse(parser);
        } catch (Exception e) {
            return null;
        }
    }
}
