package org.unipop.controllerprovider;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.unipop.controller.EdgeController;
import org.unipop.controller.Predicates;
import org.unipop.controller.VertexController;
import org.unipop.structure.BaseVertex;

import java.util.Iterator;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.inject;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.select;

public abstract class GraphSchemaControllerProvider implements EdgeController, VertexController {

    protected String controller = "controller";
    private final GraphTraversalSource g;
    protected TinkerGraph schema;

    public GraphSchemaControllerProvider() {
        this.schema = TinkerGraph.open();
        this.g = schema.traversal();
    }

    @Override
    public Iterator<Vertex> vertices(Object[] ids) {
        return g.V().<VertexController>values(controller).as(controller) //get all controllers
                .<Vertex>flatMap(controller -> controller.get().vertices(ids)) // get vertices
                .sideEffect(vertex -> vertex.get().property(controller, controller)); //save controller as property
    }

    @Override
    public Iterator<Vertex> vertices(Predicates predicates, MutableMetrics metrics) {
        return g.V().<VertexController>values(controller).as(controller) //get all controllers
                .<Vertex>flatMap(controller -> controller.get().vertices(predicates, metrics)) //get vertices
                .sideEffect(vertex -> vertex.get().property(controller, controller)); //save controller as property
    }

    @Override
    public BaseVertex vertex(Edge edge, Direction direction, Object vertexId, String vertexLabel) {
        Edge controllerEdge = edge.<Edge>property(controller).value();
         return g.E(controllerEdge).toV(direction).dedup().<VertexController>values(controller) //get controllers
                .<BaseVertex>map(controller -> controller.get().vertex(edge, direction, vertexId, vertexLabel)) //get vertices
                .sideEffect(vertex -> vertex.get().property(controller, controller)) //save controller as property
                .next(); //only supposed to get 1 vertex
    }

    @Override
    public BaseVertex addVertex(Object id, String label, Object[] properties) {
        return g.V().hasLabel(label).<VertexController>values(controller) //get controller
            .<BaseVertex>map(controller -> controller.get().addVertex(id, label, properties)) //get vertices
            .sideEffect(vertex -> vertex.get().property(controller, controller)) //save controller as property
            .next(); //only supposed to get 1 vertex
    }

    @Override
    public Iterator<Edge> edges(Object[] ids) {
        return g.E().<EdgeController>values(controller).as(controller) //get all controllers
                .<Edge>flatMap(controller -> controller.get().edges(ids)) // get edges
                .sideEffect(edge -> edge.get().property(controller, controller)); //save controller as property
    }

    @Override
    public Iterator<Edge> edges(Predicates predicates, MutableMetrics metrics) {
        return g.E().<EdgeController>values(controller).as(controller) //get all controllers
                .<Edge>flatMap(controller -> controller.get().edges(predicates, metrics)) //get edges
                .sideEffect(edge -> edge.get().property(controller, controller)); //save controller as property
    }

    @Override
    public Iterator<Edge> edges(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates, MutableMetrics metrics) {
        return inject(vertices).group().by(controller).as("controllerGroup") //group by controller
                .mapKeys().<Vertex>value().flatMap(controllerVertex -> g.V(controllerVertex).toE(direction, edgeLabels)).as("controllerEdge")
                .<EdgeController>values(controller).as(controller)
                .<Edge>flatMap(controller -> controller.get().edges(select("controllerGroup").<Vertex>mapValues(), direction, edgeLabels, predicates, metrics));

    }

    @Override
    public Edge addEdge(Object edgeId, String label, Vertex outV, Vertex inV, Object[] properties) {
        return null;
    }


}
