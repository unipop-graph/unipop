package com.tinkerpop.gremlin.elastic.process.graph.traversal.sideEffect;

import com.tinkerpop.gremlin.elastic.ElasticService;
import com.tinkerpop.gremlin.elastic.structure.ElasticGraph;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GraphStep;
import com.tinkerpop.gremlin.process.graph.util.HasContainer;
import com.tinkerpop.gremlin.structure.*;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.GeoShapeFilterBuilder;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.*;

public class ElasticGraphStep<E extends Element> extends GraphStep<E> {
    public final List<HasContainer> hasContainers = new ArrayList<HasContainer>();
    ElasticService elasticService;


    public ElasticGraphStep(final Traversal traversal, final ElasticGraph graph, final Class<E> returnClass, Optional<String> label, final Object... ids) {
        super(traversal, graph, returnClass, ids);
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

    private FilterBuilder getFilter() {
        if (this.hasContainers.size() == 0 && this.getIds().length == 0)
            return null;

        BoolFilterBuilder boolFilterBuilder = FilterBuilders.boolFilter();
        for (HasContainer has : this.hasContainers) {
            if(has.predicate instanceof Compare) {
                String predicateString = has.predicate.toString();
                if (predicateString.equals("eq")) {
                    if (has.key.equals("~label"))
                        this.setLabel(has.value.toString());
                    else
                        boolFilterBuilder = boolFilterBuilder.must(FilterBuilders.termFilter(has.key, has.value));
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
                    throw new NotImplementedException();
            }
            else if(has.predicate instanceof Contains){
                if(has.predicate == Contains.without)
                    boolFilterBuilder = boolFilterBuilder.mustNot(FilterBuilders.existsFilter(has.key));
                else if(has.predicate == Contains.within)
                    boolFilterBuilder = boolFilterBuilder.must(FilterBuilders.existsFilter(has.key));
            }
            else if(has.predicate instanceof Geo)
                boolFilterBuilder = boolFilterBuilder.must(new GeoShapeFilterBuilder(has.key, GetShapeBuilder(has.value), ((Geo) has.predicate).getRelation()));
            else throw new NotImplementedException();
        }
        if (this.getIds().length > 0) {
            String[] stringIds = Arrays.copyOf(getIds(), getIds().length, String[].class);
            boolFilterBuilder = boolFilterBuilder.must(FilterBuilders.idsFilter().addIds(stringIds));
        }
        if(!boolFilterBuilder.hasClauses()) return null;
        return boolFilterBuilder;
    }

    private ShapeBuilder GetShapeBuilder(Object object)  {
        try {
            String geoJson = (String) object;
            XContentParser parser = JsonXContent.jsonXContent.createParser(geoJson);
            parser.nextToken();

            return ShapeBuilder.parse(parser);
        }
        catch (Exception e){
            return null;
        }
    }

    private String label(){
        return this.getLabel().isPresent() ? this.getLabel().get() : null;
    }
}
