package com.tinkerpop.gremlin.elastic.process.graph.traversal.sideEffect;

import com.tinkerpop.gremlin.elastic.elasticservice.ElasticService;
import com.tinkerpop.gremlin.elastic.structure.ElasticEdge;
import com.tinkerpop.gremlin.elastic.structure.ElasticGraph;
import com.tinkerpop.gremlin.elastic.structure.ElasticVertex;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraverserGenerator;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GraphStep;
import com.tinkerpop.gremlin.process.graph.util.HasContainer;
import com.tinkerpop.gremlin.process.util.EmptyStep;
import com.tinkerpop.gremlin.process.util.TraversalMetrics;
import com.tinkerpop.gremlin.structure.*;
import org.apache.lucene.queries.TermFilter;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.*;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.*;

public class ElasticGraphStep<E extends Element> extends GraphStep<E> {

    ElasticService elasticService;
    Direction direction;
    List<Object> idsExtended;
    BoolFilterBuilder boolFilterBuilder;

    public final List<HasContainer> hasContainers = new ArrayList<HasContainer>();
    public ElasticGraphStep(Direction direction,final Traversal traversal, final ElasticGraph graph, final Class<E> returnClass, Optional<String> label, final Object... ids) {
        super(traversal, graph, returnClass, ids);
        this.direction = direction;
        this.idsExtended = new ArrayList<>();
        this.boolFilterBuilder = FilterBuilders.boolFilter();
        idsExtended.addAll(Arrays.asList(ids));

        if (label.isPresent()) this.setLabel(label.get());
        this.setIteratorSupplier(() -> (Iterator<E>) (Vertex.class.isAssignableFrom(this.returnClass) ? this.vertices() : this.edges()));
        this.elasticService = graph.elasticService;
    }

    private Iterator<? extends Vertex> vertices() {
        return elasticService.searchVertices(getFilter(), label());
    }

    private Iterator<? extends Edge> edges() {
        return elasticService.searchEdges(getFilter(), label());
    }


    private String label() {
        return this.getLabel().isPresent() ? this.getLabel().get() : null;
    }

    public Iterator<E> getResults(){
        if(this.previousStep!=null && (!(this.previousStep instanceof EmptyStep))) {
            ElasticGraphStep previousStep = (ElasticGraphStep) this.getPreviousStep();
            Iterator<? extends Element> iterator =  previousStep.getResults();
            List<Object> ids = new ArrayList<Object>();
            while(iterator.hasNext()){
                Element next = iterator.next();
                if(next instanceof Edge ){
                    ElasticEdge edge = (ElasticEdge) next;
                    ids.addAll(edge.getVertexId(direction));
                }
                else {
                    ids.add(next.id());
                }

            }

            addPredicateAccordingToIds(ids);
        }
         return this.iteratorSupplier.get();
    }

    private void addPredicateAccordingToIds(List<Object> ids){
        if(Vertex.class.isAssignableFrom(this.returnClass)){
            //moving from edges to vertices, we don't need direction
            this.idsExtended.addAll(ids);
        }

        else {
            //moving from vertices to edges we need direction
            //this is a little patchy - could have done it with adding hasContainer for In and Out but not for BOTH (no or)
            TermsFilterBuilder inFilter =  FilterBuilders.termsFilter(ElasticEdge.InId, ids.toArray());
            TermsFilterBuilder outFilter =  FilterBuilders.termsFilter(ElasticEdge.OutId, ids.toArray());
            if(direction == Direction.IN) boolFilterBuilder.must(inFilter);
            if(direction == Direction.OUT) boolFilterBuilder.must(outFilter);
            if(direction == Direction.BOTH) boolFilterBuilder.must(FilterBuilders.orFilter(inFilter, outFilter));
        }
    }


    @Override
    public void generateTraversers(final TraverserGenerator traverserGenerator) {
        if (PROFILING_ENABLED) TraversalMetrics.start(this);
        try {
            this.start = this.getResults();
            //this is code from supers super
            if (this.start instanceof Iterator) {
                this.starts.add(traverserGenerator.generateIterator((Iterator<E>) this.start, this, 1l));
            } else {
                this.starts.add(traverserGenerator.generate((E) this.start, this, 1l));
            }

        } catch (final Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        } finally {
            if (PROFILING_ENABLED) TraversalMetrics.stop(this);
        }
    }


    private  FilterBuilder getFilter(){
        Object[] ids = this.idsExtended.toArray();
        List<HasContainer> hasContainers = this.hasContainers;
        if (hasContainers.size() == 0 && ids.length == 0) return null;

        for (HasContainer has : hasContainers) {
            if (has.predicate instanceof Compare) {
                String predicateString = has.predicate.toString();
                if (predicateString.equals("eq")) {
                    if (has.key.equals("~label")) this.setLabel(has.value.toString());
                    else boolFilterBuilder = boolFilterBuilder.must(FilterBuilders.termFilter(has.key, has.value));
                } else if (predicateString.equals("neq"))
                    boolFilterBuilder = boolFilterBuilder.mustNot(FilterBuilders.termFilter(has.key, has.value));
                else if (predicateString.equals("gt"))
                    boolFilterBuilder.must(FilterBuilders.rangeFilter(has.key).gt(has.value));
                else if (predicateString.equals("gte"))
                    boolFilterBuilder.must(FilterBuilders.rangeFilter(has.key).gte(has.value));
                else if (predicateString.equals("lt"))
                    boolFilterBuilder.must(FilterBuilders.rangeFilter(has.key).lt(has.value));
                else if (predicateString.equals("lte"))
                    boolFilterBuilder.must(FilterBuilders.rangeFilter(has.key).lte(has.value));
                else
                    throw new IllegalArgumentException("predicate not supported in has step: " + has.predicate.toString());
            } else if (has.predicate instanceof Contains) {
                if (has.predicate == Contains.without)
                    boolFilterBuilder = boolFilterBuilder.mustNot(FilterBuilders.existsFilter(has.key));
                else if (has.predicate == Contains.within)
                    boolFilterBuilder = boolFilterBuilder.must(FilterBuilders.existsFilter(has.key));
            } else if (has.predicate instanceof Geo)
                boolFilterBuilder = boolFilterBuilder.must(new GeoShapeFilterBuilder(has.key, GetShapeBuilder(has.value), ((Geo) has.predicate).getRelation()));
            else throw new NotImplementedException();
        }
        if (ids.length > 0) {
            String[] stringIds = new String[ids.length];
            for(int i = 0; i<ids.length; i++)
                stringIds[i] = ids[i].toString();
            boolFilterBuilder = boolFilterBuilder.must(FilterBuilders.idsFilter().addIds(stringIds));
        }
        if (!boolFilterBuilder.hasClauses()) return null;
        return boolFilterBuilder;
    }

    private  ShapeBuilder GetShapeBuilder(Object object) {
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
