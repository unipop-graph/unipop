package com.tinkerpop.gremlin.elastic.process.graph.traversal.sideEffect;

import com.tinkerpop.gremlin.elastic.elasticservice.ElasticService;
import com.tinkerpop.gremlin.elastic.structure.ElasticEdge;
import com.tinkerpop.gremlin.elastic.structure.ElasticGraph;
import com.tinkerpop.gremlin.elastic.structure.ElasticVertex;
import com.tinkerpop.gremlin.process.Traversal;
import com.tinkerpop.gremlin.process.TraverserGenerator;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.GraphStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.IdentityStep;
import com.tinkerpop.gremlin.process.graph.step.sideEffect.SideEffectStep;
import com.tinkerpop.gremlin.process.graph.step.util.MarkerIdentityStep;
import com.tinkerpop.gremlin.process.graph.util.HasContainer;
import com.tinkerpop.gremlin.process.util.BulkSet;
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

public class ElasticGraphStep<E extends Element> extends GraphStep<E> implements ElasticSearchStep {

    ElasticService elasticService;

    List<Object> idsExtended;
    BoolFilterBuilder boolFilterBuilder;

    public  List<HasContainer> hasContainers = new ArrayList<HasContainer>();
    public ElasticGraphStep(final Traversal traversal, final ElasticGraph graph, final Class<E> returnClass, Optional<String> label, final Object... ids) {
        super(traversal, graph, returnClass, ids);
        this.idsExtended = new ArrayList<>();
        this.boolFilterBuilder = FilterBuilders.boolFilter();
        idsExtended.addAll(Arrays.asList(ids));

        if (label.isPresent()) this.setLabel(label.get());
        this.setIteratorSupplier(() -> (Iterator<E>) (Vertex.class.isAssignableFrom(this.returnClass) ? this.vertices() : this.edges()));
        this.elasticService = graph.elasticService;
    }

    private Iterator<? extends Vertex> vertices() {
        Iterator<Vertex> vertexIterator = elasticService.searchVertices(FilterBuilderProvider.getFilter(this), label());
        return vertexIterator;
    }

    private Iterator<? extends Edge> edges() {
        return elasticService.searchEdges(FilterBuilderProvider.getFilter(this), label());
    }


    private String label() {
        return this.getLabel().isPresent() ? this.getLabel().get() : null;
    }

    @Override
    public void addIds(Object[] ids) {
        idsExtended.addAll(Arrays.asList(ids));
    }

    @Override
    public void addPredicates(List<HasContainer> containerList) {
        this.hasContainers.addAll(containerList);
    }

    @Override
    public List<HasContainer> getPredicates() {
        return this.hasContainers;
    }

    @Override
    public void addId(Object id) {
        this.idsExtended.add(id);
    }

    @Override
    public void clearIds() {
        this.idsExtended = new ArrayList<Object>();
    }
    @Override
    public void clearPredicates() {
        this.hasContainers = new ArrayList<HasContainer>();
    }

}
