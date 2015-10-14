package org.unipop.controllerprovider;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.unipop.controller.EdgeController;
import org.unipop.controller.Predicates;
import org.unipop.controller.VertexController;
import org.unipop.structure.BaseEdge;
import org.unipop.structure.BaseVertex;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;

public abstract class GraphSchemaControllerProvider implements ControllerProvider {

    protected String controller = "controller";
    protected String schemaElement = "~schemaElement";
    private final GraphTraversalSource g;
    protected TinkerGraph schema;

    public GraphSchemaControllerProvider() {
        this.schema = TinkerGraph.open();
        this.g = schema.traversal();
    }

    @Override
    public Iterator<BaseVertex> vertices(Object[] ids) {
        return g.V().as(schemaElement).<VertexController>values(controller) //get all controllers
                .<BaseVertex>flatMap(controller -> controller.get().vertices(ids)) // get vertices
                .property(schemaElement, select(schemaElement)); //save schema as property
    }

    @Override
    public Iterator<BaseVertex> vertices(Predicates predicates, MutableMetrics metrics) {
        return g.V().as(schemaElement).<VertexController>values(controller) //get all controllers
                .<BaseVertex>flatMap(controller -> controller.get().vertices(predicates, metrics)) //get vertices
                .property(schemaElement, select(schemaElement)); //save schema as property
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
        return g.V().as(schemaElement).hasLabel(label) //get controller
            .<BaseVertex>map(schemaVertex -> getController(schemaVertex.get()).addVertex(id, label, properties)) //add vertex
            .property(schemaElement, select(schemaElement))
            .next(); //only supposed to get 1 vertex
    }

    private VertexController getController(Vertex schemaVertex) {
        return schemaVertex.value(schemaElement);
    }

    @Override
    public Iterator<BaseEdge> edges(Object[] ids) {
        return g.E().as(schemaElement).<EdgeController>values(controller) //get all controllers
                .<BaseEdge>flatMap(controller -> controller.get().edges(ids)) // get edges
                .property(schemaElement, select(schemaElement)); //save schema as property
    }

    @Override
    public Iterator<BaseEdge> edges(Predicates predicates, MutableMetrics metrics) {
        return g.E().as(schemaElement).<EdgeController>values(controller) //get all controllers
                .<BaseEdge>flatMap(controller -> controller.get().edges(predicates, metrics)) //get edges
                .property(schemaElement, select(schemaElement)); //save schema as property
    }

    @Override
    public Iterator<BaseEdge> edges(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates, MutableMetrics metrics) {
        return inject(vertices).group().by(schemaElement).<BulkSet<Vertex>>mapValues() //group by schema
                .flatMap(bulkVertices -> getEdgeSchema(bulkVertices.get(), direction, edgeLabels)).as("pair")
                .flatMap(pair -> pair.get().getKey().<EdgeController>value(controller).edges(pair.get().getValue(), direction, edgeLabels, predicates, metrics)).as("edge")
                .<Pair<Edge, Vertex[]>>select("pair").map(pair -> pair.get().getKey()).as(schemaElement)
                .<BaseEdge>select("edge").property(schemaElement, select(schemaElement));
    }

    private Iterator<Pair<Edge, Vertex[]>> getEdgeSchema(BulkSet<Vertex> vertices, Direction direction, String[] edgeLabels){
        if(vertices.size() == 0) return EmptyIterator.instance();

        Vertex[] arrayVertices = new Vertex[vertices.size()];
        Iterator<Vertex> vertexIterator = vertices.iterator();
        for(int i = 0; i < arrayVertices.length; i++)
            arrayVertices[i] = vertexIterator.next();

        List<Pair<Edge, Vertex[]>> result = new ArrayList<>();
        arrayVertices[0].<Vertex>property(schemaElement).value().edges(direction, edgeLabels).forEachRemaining(edge -> result.add(Pair.of(edge, arrayVertices)));
        return result.iterator();
    }

    @Override
    public BaseEdge addEdge(Object edgeId, String label, Vertex outV, Vertex inV, Object[] properties) {
        return g.E().hasLabel(label).as(schemaElement)
                .where(outV().label().is(outV.label()))
                .where(inV().label().is(inV.label()))
                .<EdgeController>values(controller).map(controller -> controller.get().addEdge(edgeId, label, outV, inV, properties))
                .property(schemaElement, select(schemaElement)) //save schema as property
                .next();
    }
}
