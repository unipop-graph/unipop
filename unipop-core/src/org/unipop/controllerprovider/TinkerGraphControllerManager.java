package org.unipop.controllerprovider;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.unipop.controller.EdgeController;
import org.unipop.controller.Predicates;
import org.unipop.controller.VertexController;
import org.unipop.structure.BaseEdge;
import org.unipop.structure.BaseVertex;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;

public abstract class TinkerGraphControllerManager implements ControllerManager {

    protected String controller = "controller";
    private final GraphTraversalSource g;
    protected TinkerGraph schema;

    public TinkerGraphControllerManager() {
        this.schema = TinkerGraph.open();
        this.g = schema.traversal();
    }

    @Override
    public Iterator<BaseVertex> vertices(Object[] ids) {
        return defaultVertexControllers()
                .flatMap(controller -> controller.get().vertices(ids));
    }

    @Override
    public Iterator<BaseVertex> vertices(Predicates predicates, MutableMetrics metrics) {
        GraphTraversal<Vertex, VertexController> controllers = g.V()
                .where(filterPredicates(predicates))
                .<VertexController>values(controller).dedup();

        return orDefault(controllers, defaultVertexControllers())
                .flatMap(controller -> controller.get().vertices(predicates, metrics));
    }



    @Override
    public BaseVertex fromEdge(Direction direction, Object vertexId, String vertexLabel) {
        GraphTraversal<Vertex, VertexController> controllers = g.V()
                .hasLabel(vertexLabel) //TODO: filter by edge
                .<VertexController>values(controller).dedup();

        return orDefault(controllers, defaultVertexControllers())
                .map(controller -> controller.get().fromEdge(direction, vertexId, vertexLabel))
                .next(); //only supposed to get 1
    }


    @Override
    public BaseVertex addVertex(Object id, String label, Map<String, Object> properties) {
        GraphTraversal<Vertex, VertexController> controllers = g.V()
                .has(T.label, label)
                .<VertexController>values(controller).dedup();

        return orDefault(controllers, defaultVertexControllers())
                .map(controller -> controller.get().addVertex(id, label, properties))
                .next();//only supposed to get 1 vertex
    }

    @Override
    public Iterator<BaseEdge> edges(Object[] ids) {
        return defaultEdgeControllers()
                .flatMap(controller -> controller.get().edges(ids));
    }

    @Override
    public Iterator<BaseEdge> edges(Predicates predicates, MutableMetrics metrics) {
        GraphTraversal<Edge, EdgeController> controllers = g.E()
                .where(filterPredicates(predicates))
                .<EdgeController>values(controller).dedup();
        return orDefault(controllers, defaultEdgeControllers())
                .flatMap(controller -> controller.get().edges(predicates, metrics));
    }

    @Override
    public Iterator<BaseEdge> fromVertex(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates, MutableMetrics metrics) {
        return inject(vertices)
                .group().by(flatMap(traverser -> getEdgeControllers((Vertex) traverser.get(), direction, edgeLabels, predicates)))
                .<Map.Entry<EdgeController, Set<Vertex>>>unfold()
                .flatMap(entry -> getEdges(entry.get().getKey(), entry.get().getValue(), direction, edgeLabels, predicates, metrics));
    }

    private Iterator<EdgeController>getEdgeControllers(Vertex vertex, Direction direction, String[] edgeLabels, Predicates predicates) {
        GraphTraversal<Vertex, EdgeController> controllers = g.V()
                .hasLabel(vertex.label())
                .toE(direction, edgeLabels)
                .where(filterPredicates(predicates))
                .<EdgeController>values(controller).dedup();

        return orDefault(controllers, defaultEdgeControllers());
    }
    
    private Iterator<BaseEdge> getEdges(EdgeController controller, Set<Vertex> vertices, Direction direction, String[] edgeLabels, Predicates predicates, MutableMetrics metrics) {
        return controller.fromVertex(vertices.toArray(new Vertex[vertices.size()]), direction, edgeLabels, predicates, metrics);

    }

    @Override
    public BaseEdge addEdge(Object edgeId, String label, Vertex outV, Vertex inV, Map<String, Object> properties) {
        GraphTraversal<?, EdgeController> controllers =
                g.E().has(T.label, label)
                .where(outV().label().is(outV.label()).and().inV().label().is(inV.label()))
                .<EdgeController>values(controller).dedup();

        return orDefault(controllers, defaultEdgeControllers())
            .map(controller -> controller.get().addEdge(edgeId, label, outV, inV, properties)).next();
    }

    protected <C>GraphTraversal<?, C> orDefault(GraphTraversal<?, C> traversal, GraphTraversal<?, C> defaultTraversal) {
        GraphTraversal.Admin<?, C> clone = traversal.asAdmin().clone();
        List<C> list = traversal.toList();
        if(list.size() > 0)
            return clone;
        return defaultTraversal;
    }

    protected GraphTraversal<?, VertexController> defaultVertexControllers() {
        return g.V().<VertexController>values(controller).dedup();
    }

    protected GraphTraversal<?, EdgeController> defaultEdgeControllers() {
        return g.E().<EdgeController>values(controller).dedup();
    }

    protected Traversal<?, ?> filterPredicates(Predicates predicates) {
        //if(predicates.hasContainers.size() == 0) //TODO: filter predicates
            return constant(true).is(true);
//        GraphTraversal<?, ?> traversal = start();
//        predicates.hasContainers.forEach(predicate ->
//                traversal.where(not(has(predicate.getKey())).or().has(predicate.getKey(), predicate.getPredicate())));
//        return traversal;
    }
}