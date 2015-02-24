package com.tinkerpop.gremlin.elastic.process.graph.traversal.sideEffect;

import com.tinkerpop.gremlin.elastic.ElasticService;
import com.tinkerpop.gremlin.elastic.structure.ElasticGraph;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GraphStep;
import com.tinkerpop.gremlin.process.graph.util.HasContainer;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;
import com.tinkerpop.gremlin.structure.Vertex;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.*;

public class ElasticGraphStep<E extends Element> extends GraphStep<E> {
    public final List<HasContainer> hasContainers = new ArrayList<>();
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
            switch (has.predicate.toString()) {
                case ("eq"):
                    boolFilterBuilder = boolFilterBuilder.must(FilterBuilders.termFilter(has.key, has.value));
                    break;
                default:
                    throw new NotImplementedException();
            }
        }

        if (this.getIds().length > 0)
            boolFilterBuilder = boolFilterBuilder.must(FilterBuilders.idsFilter().addIds(Arrays.toString(this.getIds())));

        return boolFilterBuilder;
    }

    private String label(){
        return this.getLabel().isPresent() ? this.getLabel().get() : null;
    }
}
