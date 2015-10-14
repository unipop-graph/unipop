package org.unipop.controllerprovider;

import org.apache.commons.lang3.tuple.Pair;
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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;

public abstract class GraphSchemaControllerProvider implements ControllerProvider {

    protected String controller = "~controller";
    protected String schemaElement = "~schemaElement";
    private final GraphTraversalSource g;
    protected TinkerGraph schema;

    public GraphSchemaControllerProvider() {
        this.schema = TinkerGraph.open();
        this.g = schema.traversal();
    }

    @Override
    public Iterator<Vertex> vertices(Object[] ids) {
        return g.V().as(schemaElement).<VertexController>values(controller) //get all controllers
                .<Vertex>flatMap(controller -> controller.get().vertices(ids)) // get vertices
                .sideEffect(vertex -> vertex.get().property(schemaElement, select(schemaElement))); //save schema as property
    }

    @Override
    public Iterator<Vertex> vertices(Predicates predicates, MutableMetrics metrics) {
        return g.V().as(schemaElement).<VertexController>values(controller) //get all controllers
                .<Vertex>flatMap(controller -> controller.get().vertices(predicates, metrics)) //get vertices
                .sideEffect(vertex -> vertex.get().property(schemaElement, select(schemaElement))); //save schema as property
    }

    @Override
    public BaseVertex vertex(Edge edge, Direction direction, Object vertexId, String vertexLabel) {
        Edge schemaEdge = edge.<Edge>property(schemaElement).value();
        Vertex schemaVertex = schemaEdge.vertices(direction).next(); //only supposed to get 1 vertex
        BaseVertex result = schemaVertex.<VertexController>property(controller).value().vertex(edge, direction, vertexId, vertexLabel);
        result.property(schemaElement, schemaVertex);
        return result;
    }

    @Override
    public BaseVertex addVertex(Object id, String label, Object[] properties) {
        return g.V().as(schemaElement).hasLabel(label).<VertexController>values(controller) //get controller
            .<BaseVertex>map(controller -> controller.get().addVertex(id, label, properties)) //add vertex
            .sideEffect(vertex -> vertex.get().property(schemaElement, select(schemaElement))) //save schema as property
            .next(); //only supposed to get 1 vertex
    }

    @Override
    public Iterator<Edge> edges(Object[] ids) {
        return g.E().as(schemaElement).<EdgeController>values(controller) //get all controllers
                .<Edge>flatMap(controller -> controller.get().edges(ids)) // get edges
                .sideEffect(edge -> edge.get().property(schemaElement, select(schemaElement))); //save schema as property
    }

    @Override
    public Iterator<Edge> edges(Predicates predicates, MutableMetrics metrics) {
        return g.E().as(schemaElement).<EdgeController>values(controller) //get all controllers
                .<Edge>flatMap(controller -> controller.get().edges(predicates, metrics)) //get edges
                .sideEffect(edge -> edge.get().property(schemaElement, select(schemaElement))); //save schema as property
    }

    @Override
    public Iterator<Edge> edges(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates, MutableMetrics metrics) {
        return inject(vertices).group().by(schemaElement).<Map.Entry<Vertex, Iterator<Vertex>>>unfold() //group by schema
                .flatMap(entry -> getEdgeSchema(entry.get(), direction, edgeLabels)).as("pair")
                .flatMap(pair -> pair.get().getKey().<EdgeController>value(controller).edges(pair.get().getValue(), direction, edgeLabels, predicates, metrics))
                .sideEffect(edge -> edge.get().property(schemaElement, select("pair").map(pair-> ((Pair<Edge, Vertex[]>)pair.get())))); //save schema as property
    }

    private Iterator<Pair<Edge, Vertex[]>> getEdgeSchema(Map.Entry<Vertex, Iterator<Vertex>> entry, Direction direction, String[] edgeLabels){
        List<Pair<Edge, Vertex[]>> result = new ArrayList<>();
        Vertex[] vertices = (Vertex[]) IteratorUtils.list(entry.getValue()).toArray();
        entry.getKey().edges(direction, edgeLabels).forEachRemaining(edge -> result.add(Pair.of(edge, vertices)));
        return result.iterator();
    }

    @Override
    public Edge addEdge(Object edgeId, String label, Vertex outV, Vertex inV, Object[] properties) {
        return g.E().hasLabel(label).as(schemaElement)
                .where(outV().label().is(outV.label()))
                .where(inV().label().is(inV.label()))
                .<EdgeController>values(controller).map(controller -> controller.get().addEdge(edgeId, label, outV, inV, properties))
                .sideEffect(edge -> edge.get().property(schemaElement, select(schemaElement)))
                .next();
    }
}
