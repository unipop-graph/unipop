package org.unipop.controllerprovider;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
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

import java.util.*;

import static org.apache.tinkerpop.gremlin.process.traversal.P.eq;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;

public abstract class GraphSchemaControllerProvider implements ControllerProvider {

    protected String controller = "controller";
    private final GraphTraversalSource g;
    protected TinkerGraph schema;

    public GraphSchemaControllerProvider() {
        this.schema = TinkerGraph.open();
        this.g = schema.traversal();
    }

    @Override
    public Iterator<BaseVertex> vertices(Object[] ids) {
        return g.V()
                .<VertexController>values(controller).dedup()
                .flatMap(controller -> controller.get().vertices(ids));
    }

    @Override
    public Iterator<BaseVertex> vertices(Predicates predicates, MutableMetrics metrics) {
        return g.V().where(filterSchema(predicates))
                .<VertexController>values(controller).dedup()
                .flatMap(controller -> controller.get().vertices(predicates, metrics));
    }

    @Override
    public BaseVertex fromEdge(Direction direction, Object vertexId, String vertexLabel) {
        return g.V().where(filterSchema(vertexLabel))
                .<VertexController>values(controller).dedup()
                .map(controller -> controller.get().fromEdge(direction, vertexId, vertexLabel))
                .next(); //only supposed to get 1
    }


    @Override
    public BaseVertex addVertex(Object id, String label, Object[] properties) {
        return g.V().where(filterSchema(label))
                .<VertexController>values(controller) //get controller
                .map(controller -> controller.get().addVertex(id, label, properties)) //add fromEdge
                .next(); //only supposed to get 1 fromEdge
    }

    @Override
    public Iterator<BaseEdge> edges(Object[] ids) {
        return g.E()
                .<EdgeController>values(controller).dedup()
                .flatMap(controller -> controller.get().edges(ids));
    }

    @Override
    public Iterator<BaseEdge> edges(Predicates predicates, MutableMetrics metrics) {
        return g.E()//.where(filterSchema(predicates))
                .<EdgeController>values(controller).dedup() //get controllers
                .flatMap(controller -> controller.get().edges(predicates, metrics)); //get fromVertex
    }

    @Override
    public Iterator<BaseEdge> fromVertex(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates, MutableMetrics metrics) {
        return inject(vertices).group().by(g.E().hasLabel(edgeLabels).where(filterSchema(predicates)).values(controller)).where(is(true))
                .<Map.Entry<EdgeController, Set<Vertex>>>unfold().flatMap(entry ->
                        getEdges(entry.get().getKey(), entry.get().getValue(), direction, edgeLabels, predicates, metrics));
    }

    private Iterator<BaseEdge> getEdges(EdgeController controller, Set<Vertex> vertices, Direction direction, String[] edgeLabels, Predicates predicates, MutableMetrics metrics) {
        return controller.fromVertex(vertices.toArray(new Vertex[vertices.size()]), direction, edgeLabels, predicates, metrics);

    }

    private Iterator<Pair<Edge, Vertex[]>> getEdgeSchema(BulkSet<Vertex> vertices, Direction direction, String[] edgeLabels){
        if(vertices.size() == 0) return EmptyIterator.instance();

        Vertex[] arrayVertices = new Vertex[vertices.size()];
        Iterator<Vertex> vertexIterator = vertices.iterator();
        for(int i = 0; i < arrayVertices.length; i++)
            arrayVertices[i] = vertexIterator.next();

        List<Pair<Edge, Vertex[]>> result = new ArrayList<>();
        //arrayVertices[0].<Vertex>property(schemaElement).value().fromVertex(direction, edgeLabels).forEachRemaining(edge -> result.add(Pair.of(edge, arrayVertices)));
        return result.iterator();
    }

    @Override
    public BaseEdge addEdge(Object edgeId, String label, Vertex outV, Vertex inV, Object[] properties) {
        return g.E()//.where(filterSchema(label))//.and().(outV().label().is(outV.label()).and().inV().label().is(inV.label()))
                .<EdgeController>values(controller).dedup()
                .map(controller -> controller.get().addEdge(edgeId, label, outV, inV, properties))
                .next();
    }

    private GraphTraversal<?, ?> filterSchema(Predicates predicates) {
        GraphTraversal<Object, Object> traversal = start();

        for(HasContainer hasContainer :  predicates.hasContainers)
            traversal.has(hasContainer.getKey(),hasContainer.getPredicate());

        return traversal;
    }


    private GraphTraversal<Object, Object> filterSchema(String vertexLabel) {
        return choose(hasLabel(vertexLabel).count().is(eq(1)),
                hasLabel(vertexLabel),
                is(true));
    }
}
