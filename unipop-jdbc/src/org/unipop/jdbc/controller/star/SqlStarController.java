package org.unipop.jdbc.controller.star;

import org.apache.commons.collections.iterators.EmptyIterator;
import org.apache.commons.lang.NotImplementedException;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.jooq.*;
import org.unipop.controller.EdgeController;
import org.unipop.controller.Predicates;
import org.unipop.jdbc.controller.star.inneredge.InnerEdgeController;
import org.unipop.jdbc.controller.vertex.SqlVertex;
import org.unipop.jdbc.controller.vertex.SqlVertexController;
import org.unipop.jdbc.utils.JooqHelper;
import org.unipop.structure.BaseEdge;
import org.unipop.structure.BaseVertex;
import org.unipop.structure.UniGraph;

import java.sql.Connection;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;


/**
 * Created by sbarzilay on 2/17/16.
 */
public class SqlStarController extends SqlVertexController implements EdgeController {

    Set<InnerEdgeController> innerEdgeControllers;
    Set<String> propertiesNames;

    public SqlStarController(String tableName, UniGraph graph, Connection conn, Set<String> propertiesNames, InnerEdgeController... innerEdgeControllers) {
        super(tableName, graph, conn);
        this.innerEdgeControllers = new HashSet<>();
        Collections.addAll(this.innerEdgeControllers, innerEdgeControllers);
        vertexMapper = new StarVertexMapper();
        this.propertiesNames = propertiesNames;

    }

    public SqlStarController() {
    }

    public String getTableName() {
        return tableName;
    }

    @Override
    protected SqlVertex createVertex(Object id, String label, Map<String, Object> properties) {
        SqlStarVertex starVertex = new SqlStarVertex(id, label, null, null, tableName, this, graph, propertiesNames);
        properties.forEach(starVertex::addPropertyLocal);
        return starVertex;
    }

    @Override
    public Iterator<BaseVertex> vertices(Predicates predicates) {
        SelectJoinStep<Record> select = createSelect(predicates);
        try {
            return select.fetch(vertexMapper).stream().distinct().collect(Collectors.toList()).iterator();
        }
        catch (Exception e){
            return EmptyIterator.INSTANCE;
        }
    }

    @Override
    public Iterator<BaseEdge> edges(Predicates predicates) {
        SelectJoinStep<Record> select = dslContext.select().from(tableName);
        predicates.hasContainers.forEach(has -> select.where(JooqHelper.createCondition(has)));
        return select.fetch(vertexMapper).stream().flatMap(vertex -> ((SqlStarVertex) vertex).getInnerEdges(predicates).stream()).iterator();
    }

    @Override
    public Iterator<BaseEdge> edges(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates) {

        Stream<BaseEdge> innerEdges = StreamSupport.stream(Arrays.asList(vertices).stream().filter(vertex1 -> vertex1 instanceof SqlStarVertex)
                .map(vertex2 -> ((SqlStarVertex) vertex2).getInnerEdges(direction, Arrays.asList(edgeLabels), predicates)).spliterator(), false).flatMap(Collection::stream);

        SelectJoinStep<Record> select = dslContext.select().from(tableName);
        Set<Object> ids = Stream.of(vertices).map(Element::id).collect(Collectors.toSet());
        ArrayList<HasContainer> hasContainers = new ArrayList<>();
        predicates.hasContainers.forEach(hasContainers::add);
        for (String edgeLabel : edgeLabels) {
            hasContainers.add(new HasContainer(edgeLabel, P.within(ids)));
        }
        hasContainers.forEach(has -> select.where(JooqHelper.createCondition(has)));

        Stream<BaseEdge> outerEdges = select.fetch(vertexMapper).stream()
                .flatMap(vertex -> ((SqlStarVertex) vertex).getInnerEdges(direction.opposite(), Arrays.asList(edgeLabels), predicates).stream());
        return Stream.of(innerEdges, outerEdges).flatMap(stream -> stream.collect(Collectors.toList()).stream()).iterator();

    }

    @Override
    public long edgeCount(Predicates predicates) {
        throw new NotImplementedException();
    }

    @Override
    public long edgeCount(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates) {
        throw new NotImplementedException();
    }

    @Override
    public Map<String, Object> edgeGroupBy(Predicates predicates, Traversal keyTraversal, Traversal valuesTraversal, Traversal reducerTraversal) {
        throw new NotImplementedException();
    }

    @Override
    public Map<String, Object> edgeGroupBy(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates, Traversal keyTraversal, Traversal valuesTraversal, Traversal reducerTraversal) {
        throw new NotImplementedException();
    }

    private InnerEdgeController getControllerByLabel(String label) {
        Optional<InnerEdgeController> edgeControllerOptional = innerEdgeControllers.stream().filter(innerEdgeController -> innerEdgeController.getEdgeLabel().equals(label)).findFirst();
        if (edgeControllerOptional.isPresent())
            return edgeControllerOptional.get();
        throw new RuntimeException("no edge mapping for label: " + label);
    }

    @Override
    public BaseEdge addEdge(Object edgeId, String label, BaseVertex outV, BaseVertex inV, Map<String, Object> properties) {
        return getControllerByLabel(label).addEdge(edgeId, label, outV, inV, properties);
    }

    private SqlStarController self = this;

    private class StarVertexMapper implements RecordMapper<Record, BaseVertex> {

        @Override
        public BaseVertex map(Record record) {
            //Change keys to lower-case. TODO: make configurable mapping
            Map<String, Object> stringObjectMap = new HashMap<>();
            record.intoMap().forEach((key, value) -> stringObjectMap.put(key.toLowerCase(), value));
            SqlStarVertex star = (SqlStarVertex) createVertex(stringObjectMap.get("id"), tableName.toLowerCase(), stringObjectMap);
            SelectJoinStep<Record> select = dslContext.select().from(tableName);
            select.where(JooqHelper.createCondition(new HasContainer(T.id.getAccessor(), P.eq(stringObjectMap.get("id")))));
            select.fetch().forEach(vertex -> {
                Map<String, Object> vertexStringObjectMap = new HashMap<>();
                record.intoMap().forEach((key, value) -> vertexStringObjectMap.put(key.toLowerCase(), value));
                innerEdgeControllers.forEach(innerEdgeController -> {
                    if (vertexStringObjectMap.get(innerEdgeController.getEdgeLabel()) != null)
                        innerEdgeController.parseEdge(star, vertexStringObjectMap);
                });
            });
            return star;
        }
    }
}
