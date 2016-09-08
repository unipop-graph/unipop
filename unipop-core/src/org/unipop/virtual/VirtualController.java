package org.unipop.virtual;

import com.google.common.collect.Maps;
import org.apache.commons.lang.NotImplementedException;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.unipop.query.controller.SimpleController;
import org.unipop.query.mutation.AddEdgeQuery;
import org.unipop.query.mutation.AddVertexQuery;
import org.unipop.query.mutation.PropertyQuery;
import org.unipop.query.mutation.RemoveQuery;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.search.DeferredVertexQuery;
import org.unipop.query.search.SearchQuery;
import org.unipop.query.search.SearchVertexQuery;
import org.unipop.schema.element.ElementSchema;
import org.unipop.structure.UniEdge;
import org.unipop.structure.UniGraph;
import org.unipop.structure.UniVertex;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by sbarzilay on 9/6/16.
 */
public class VirtualController implements SimpleController {
    private final UniGraph graph;

    private Set<? extends VirtualVertexSchema> vertexSchemas = new HashSet<>();

    public VirtualController(UniGraph graph, Set<ElementSchema> schemas) {
        this.graph = graph;
        vertexSchemas = schemas.stream().filter(schema -> schema instanceof VirtualVertexSchema)
                .map(schema -> ((VirtualVertexSchema) schema)).collect(Collectors.toSet());
    }

    @Override
    public <E extends Element> void remove(RemoveQuery<E> uniQuery) {

    }

    @Override
    public Vertex addVertex(AddVertexQuery uniQuery) {
        return new UniVertex(uniQuery.getProperties(), graph);
    }

    @Override
    public <E extends Element> void property(PropertyQuery<E> uniQuery) {

    }

    @Override
    public void fetchProperties(DeferredVertexQuery query) {

    }

    @Override
    public Edge addEdge(AddEdgeQuery uniQuery) {
        return null;
    }

    @Override
    public Iterator<Edge> search(SearchVertexQuery uniQuery) {
        return EmptyIterator.instance();
    }

    private Map<String, Object> createElement(Object id, String label){
        HashMap<String, Object> element = new HashMap<>();
        element.put(T.id.getAccessor(), id);
        element.put(T.label.getAccessor(), label);
        return element;
    }

    @Override
    public <E extends Element> Iterator<E> search(SearchQuery<E> uniQuery) {
        if(uniQuery.getReturnType() != Vertex.class)
            return EmptyIterator.instance();
        PredicatesHolder predicates = uniQuery.getPredicates();
        List<? extends VirtualVertexSchema> filteredSchemas = vertexSchemas.stream()
                .filter(schema -> !schema.toPredicates(predicates).getClause().equals(PredicatesHolder.Clause.Abort)).collect(Collectors.toList());
        Optional<HasContainer> ids = predicates.getPredicates().stream().filter(has -> has.getKey().equals(T.id.getAccessor())).findFirst();
        Optional<HasContainer> labels = predicates.getPredicates().stream().filter(has -> has.getKey().equals(T.label.getAccessor())).findFirst();
        if (!ids.isPresent() || !labels.isPresent()){
            return EmptyIterator.instance();
        }
        ArrayList<Map<String, Object>> elements = new ArrayList<>();
        Object idObject = ids.get().getValue();
        Collection<Object> idsCol = idObject instanceof Collection ?
                ((Collection) idObject) : Collections.singleton(idObject);

        Object labelObject = labels.get().getValue();
        Collection<Object> labelCol = labelObject instanceof Collection ?
                ((Collection) labelObject) : Collections.singleton(labelObject);

        idsCol.forEach(id -> labelCol.forEach(label -> elements.add(createElement(id, label.toString()))));

        return (Iterator<E>) elements.stream().flatMap(fields -> filteredSchemas.stream().flatMap(schema -> Stream.of(schema.createElement(fields)))).filter(v -> v != null).iterator();
    }
}
