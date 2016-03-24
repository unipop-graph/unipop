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
import org.unipop.controller.InnerEdgeController;
import org.unipop.controller.Predicates;
import org.unipop.jdbc.controller.vertex.SqlVertexController;
import org.unipop.jdbc.utils.JooqHelper;
import org.unipop.structure.*;

import java.sql.Connection;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.jooq.impl.DSL.field;


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

    @Override
    public void init(Map<String, Object> conf, UniGraph graph) throws Exception {
        super.init(conf, graph);
        vertexMapper = new StarVertexMapper();
        for (Map<String, Object> edge : ((List<Map<String, Object>>) conf.get("edges"))) {
            InnerEdgeController innerEdge = ((InnerEdgeController) Class.forName(edge.get("class").toString()).newInstance());
            edge.put("context", dslContext);
            innerEdge.init(edge);
            innerEdgeControllers.add(innerEdge);
        }
    }

    public String getTableName() {
        return tableName;
    }

    protected UniStarVertex createVertex(Object id, String label, Map<String, Object> properties) {
        UniStarVertex starVertex = new UniStarVertex(id, label, null,graph.getControllerManager(), graph, innerEdgeControllers);
        starVertex.addTransientProperty(new TransientProperty(starVertex, "resource", getResource()));
        properties.forEach(starVertex::addPropertyLocal);
        return starVertex;
    }

    @Override
    public BaseVertex vertex(Direction direction, Object vertexId, String vertexLabel) {
        UniDelayedStarVertex uniVertex = new UniDelayedStarVertex(vertexId, vertexLabel, graph.getControllerManager(), graph, innerEdgeControllers);
        uniVertex.addTransientProperty(new TransientProperty(uniVertex, "resource", getResource()));
        return uniVertex;
    }

    @Override
    public Iterator<BaseVertex> vertices(Predicates predicates) {
        SelectJoinStep<Record> select = createSelect(predicates);
        try {
            Map<Object, List<BaseVertex>> group = select.fetch(vertexMapper).stream().collect(Collectors.groupingBy(baseVertex -> baseVertex.id()));
            List<BaseVertex> groupedVertices = new ArrayList<>();
            group.values().forEach(baseVertices -> {
                UniStarVertex star = (UniStarVertex) baseVertices.get(0);
                baseVertices.forEach(baseVertex -> ((UniStarVertex) baseVertex).getInnerEdges(new Predicates()).forEach(baseEdge -> star.addInnerEdge(((UniInnerEdge) baseEdge))));
                groupedVertices.add(star);
            });
            return groupedVertices.iterator();
        }
        catch (Exception e){
            return EmptyIterator.INSTANCE;
        }
    }

    private HasContainer getHasId(Predicates predicates){
        for (HasContainer hasContainer : predicates.hasContainers) {
            if (hasContainer.getKey().equals(T.id.getAccessor()))
                return hasContainer;
        }
        return null;
    }

    @Override
    public Iterator<BaseEdge> edges(Predicates predicates) {
        SelectJoinStep<Record> select = dslContext.select().from(tableName);
        HasContainer id = getHasId(predicates);
        if (id != null){
            select.where(field("EDGEID").in(((Iterable) id.getValue())));
            predicates.hasContainers.remove(id);
        }
        predicates.hasContainers.forEach(has -> select.where(JooqHelper.createCondition(has)));
        Map<Object, List<BaseVertex>> group = select.fetch(vertexMapper).stream().collect(Collectors.groupingBy(baseVertex -> baseVertex.id()));
        List<BaseVertex> groupedVertices = new ArrayList<>();
        group.values().forEach(baseVertices -> {
            UniStarVertex star = (UniStarVertex) baseVertices.get(0);
            baseVertices.forEach(baseVertex -> ((UniStarVertex) baseVertex).getInnerEdges(new Predicates()).forEach(baseEdge -> star.addInnerEdge(((UniInnerEdge) baseEdge))));
            groupedVertices.add(star);
        });
        if (id != null)
            predicates.hasContainers.add(id);
        return groupedVertices.stream().flatMap(vertex -> ((UniStarVertex) vertex).getInnerEdges(predicates).stream()).collect(Collectors.toSet()).iterator();
    }

    @Override
    public Iterator<BaseEdge> edges(Vertex[] vertices, Direction direction, String[] edgeLabels, Predicates predicates) {

        Set<BaseEdge> innerEdges = StreamSupport.stream(Arrays.asList(vertices).stream().filter(vertex1 -> vertex1 instanceof UniStarVertex)
                .map(vertex2 -> ((UniStarVertex) vertex2).getInnerEdges(direction, Arrays.asList(edgeLabels), predicates)).spliterator(), false).flatMap(Collection::stream).collect(Collectors.toSet());

        SelectJoinStep<Record> select = dslContext.select().from(tableName);
        Set<Object> ids = Stream.of(vertices).map(Element::id).collect(Collectors.toSet());
        ArrayList<HasContainer> hasContainers = new ArrayList<>();
        predicates.hasContainers.forEach(hasContainers::add);
        Set<String> controllersLabels = innerEdgeControllers.stream().map(InnerEdgeController::getEdgeLabel).collect(Collectors.toSet());
        for (String edgeLabel : edgeLabels) {
            if (controllersLabels.contains(edgeLabel))
                hasContainers.add(new HasContainer(edgeLabel, P.within(ids)));
        }
        if (edgeLabels.length == 0){
            for (String edgeLabel : controllersLabels)
                hasContainers.add(new HasContainer(edgeLabel, P.within(ids)));
        }
        HasContainer id = getHasId(predicates);
        if (id != null){
            if (id.getValue() instanceof Iterable)
                select.where(field("EDGEID").in(((Iterable) id.getValue())));
            else if (id.getValue() instanceof String)
                select.where(field("EDGEID").in(id.getValue()));
            hasContainers.remove(id);
        }
        hasContainers.forEach(has -> select.where(JooqHelper.createCondition(has)));

        Predicates p = new Predicates();
        p.hasContainers = hasContainers;
        Map<Object, List<BaseVertex>> group = select.fetch(vertexMapper).stream().collect(Collectors.groupingBy(baseVertex -> baseVertex.id()));
        List<BaseVertex> groupedVertices = new ArrayList<>();
        group.values().forEach(baseVertices -> {
            UniStarVertex star = (UniStarVertex) baseVertices.get(0);
            baseVertices.forEach(baseVertex -> ((UniStarVertex) baseVertex).getInnerEdges(new Predicates()).forEach(baseEdge -> star.addInnerEdge(((UniInnerEdge) baseEdge))));
            groupedVertices.add(star);
        });

        Set<BaseEdge> outerEdges = groupedVertices.stream()
                .flatMap(vertex -> ((UniStarVertex) vertex).getInnerEdges(direction.opposite(), Arrays.asList(edgeLabels), predicates).stream()).collect(Collectors.toSet());
        return Stream.of(innerEdges, outerEdges).flatMap(baseEdges -> baseEdges.stream()).collect(Collectors.toSet()).iterator();

    }

    @Override
    protected void addProperty(List<BaseVertex> vertices, String key, Object value) {
        super.addProperty(vertices, key, value);
        if (value instanceof List) {
            InnerEdgeController innerEdgeController1 = innerEdgeControllers.stream().filter(innerEdgeController -> innerEdgeController.getEdgeLabel().equals(key)).findFirst().get();
            List<Map<String, Object>> edges = (List<Map<String, Object>>) value;
            vertices.forEach(vertex -> edges.forEach(edge -> innerEdgeController1.parseEdge(((UniStarVertex) vertex), edge)));
        }
    }

    @Override
    public void update(BaseVertex vertex, boolean force) {
        throw new NotImplementedException();
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

    private class StarVertexMapper implements RecordMapper<Record, BaseVertex> {

        @Override
        public BaseVertex map(Record record) {
            //Change keys to lower-case. TODO: make configurable mapping
            Map<String, Object> stringObjectMap = new HashMap<>();
            record.intoMap().forEach((key, value) -> stringObjectMap.put(key.toLowerCase(), value));
            UniStarVertex star = createVertex(stringObjectMap.get("id"), tableName.toLowerCase(), stringObjectMap);
            innerEdgeControllers.forEach(innerEdgeController -> {
                    if (stringObjectMap.get(innerEdgeController.getEdgeLabel()) != null)
                        innerEdgeController.parseEdge(star, stringObjectMap);
                });
//            SelectJoinStep<Record> select = dslContext.select().from(tableName);
//            select.where(JooqHelper.createCondition(new HasContainer(T.id.getAccessor(), P.eq(stringObjectMap.get("id")))));
//            select.fetch().forEach(vertex -> {
//                Map<String, Object> vertexStringObjectMap = new HashMap<>();
//                record.intoMap().forEach((key, value) -> vertexStringObjectMap.put(key.toLowerCase(), value));
//                innerEdgeControllers.forEach(innerEdgeController -> {
//                    if (vertexStringObjectMap.get(innerEdgeController.getEdgeLabel()) != null)
//                        innerEdgeController.parseEdge(star, vertexStringObjectMap);
//                });
//            });
            return star;
        }
    }
}
