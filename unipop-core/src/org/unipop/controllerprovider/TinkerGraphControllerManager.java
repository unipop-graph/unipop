package org.unipop.controllerprovider;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.unipop.controller.EdgeController;
import org.unipop.controller.Predicates;
import org.unipop.controller.VertexController;
import org.unipop.structure.BaseEdge;
import org.unipop.structure.BaseVertex;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.inject;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.select;

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
        return g.V()
                //TODO: filter vertices
                .<VertexController>values(controller).dedup()
                .flatMap(controller -> controller.get().vertices(ids));
    }

    @Override
    public Iterator<BaseVertex> vertices(Predicates predicates, MutableMetrics metrics) {
        return g.V()
                //TODO: filter vertices
                .<VertexController>values(controller).dedup()
                .flatMap(controller -> controller.get().vertices(predicates, metrics));
    }

    @Override
    public BaseVertex fromEdge(Direction direction, Object vertexId, String vertexLabel) {
        return g.V()
                //TODO: filter vertices
                .<VertexController>values(controller).dedup()
                .map(controller -> controller.get().fromEdge(direction, vertexId, vertexLabel))
                .next(); //only supposed to get 1
    }


    @Override
    public BaseVertex addVertex(Object id, String label, Object[] properties) {
        return g.V()
                //TODO: filter vertices
                .<VertexController>values(controller).dedup() //get controller
                .map(controller -> controller.get().addVertex(id, label, properties)) //add fromEdge
                .next(); //only supposed to get 1 fromEdge
    }

    @Override
    public Iterator<BaseEdge> edges(Object[] ids) {
        return g.E()
                //TODO: filter edges
                .<EdgeController>values(controller).dedup()
                .flatMap(controller -> controller.get().edges(ids));
    }

    @Override
    public Iterator<BaseEdge> edges(Predicates predicates, MutableMetrics metrics) {
        return g.E()
                //TODO: filter edges
                .<EdgeController>values(controller).dedup() //get controllers
                .flatMap(controller -> controller.get().edges(predicates, metrics)); //get fromVertex
    }

    @Override
    public Iterator<BaseEdge> fromVertex(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates, MutableMetrics metrics) {
        return inject(vertices).as("vertex")
                .flatMap(traverser -> getEdgeControllers(traverser.get(), direction, edgeLabels, predicates)).as(controller)
                .select("vertex").group().by(select(controller)).<Map.Entry<EdgeController, Set<Vertex>>>unfold()
                .flatMap(entry -> getEdges(entry.get().getKey(), entry.get().getValue(), direction, edgeLabels, predicates, metrics));
    }

    private Iterator<EdgeController>getEdgeControllers(Vertex vertex, Direction direction, String[] edgeLabels, Predicates predicates) {
        return g.E()//TODO: filter edges
                .<EdgeController>values(controller).dedup();
    }
    
    private Iterator<BaseEdge> getEdges(EdgeController controller, Set<Vertex> vertices, Direction direction, String[] edgeLabels, Predicates predicates, MutableMetrics metrics) {
        return controller.fromVertex(vertices.toArray(new Vertex[vertices.size()]), direction, edgeLabels, predicates, metrics);

    }

    @Override
    public BaseEdge addEdge(Object edgeId, String label, Vertex outV, Vertex inV, Object[] properties) {
        return g.E()
                //TODO: filter edges //.where(filterSchema(label))//.and().(outV().label().is(outV.label()).and().inV().label().is(inV.label()))
                .<EdgeController>values(controller).dedup()
                .map(controller -> controller.get().addEdge(edgeId, label, outV, inV, properties))
                .next();
    }
}