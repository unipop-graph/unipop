package org.unipop.jdbc.simple;

import com.google.common.collect.Iterators;
import org.apache.commons.lang.NotImplementedException;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.jooq.DSLContext;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.unipop.jdbc.controller.schemas.RowEdgeSchema;
import org.unipop.jdbc.controller.schemas.RowSchema;
import org.unipop.jdbc.controller.schemas.RowVertexSchema;
import org.unipop.query.StepDescriptor;
import org.unipop.query.controller.SimpleController;
import org.unipop.query.mutation.AddEdgeQuery;
import org.unipop.query.mutation.AddVertexQuery;
import org.unipop.query.mutation.PropertyQuery;
import org.unipop.query.mutation.RemoveQuery;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;
import org.unipop.query.search.DeferredVertexQuery;
import org.unipop.query.search.SearchQuery;
import org.unipop.query.search.SearchVertexQuery;
import org.unipop.structure.UniEdge;
import org.unipop.structure.UniGraph;

import java.sql.Connection;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by GurRo on 6/12/2016.
 *
 * @author GurRo
 * @since 6/12/2016
 */
public class RowController implements SimpleController {
    private final DSLContext dslContext;
    private final UniGraph graph;
    private final Set<? extends RowVertexSchema> vertexSchemas;
    private final Set<? extends RowEdgeSchema> edgeSchemas;

//    private final RecordMapper<Record, UniVertex> vertexMapper;

    public RowController(UniGraph graph, Connection conn, Set<RowVertexSchema> vertexSchemas, Set<RowEdgeSchema> edgeSchemas) {
        this.graph = graph;
        this.dslContext = DSL.using(conn, SQLDialect.DEFAULT);
        this.vertexSchemas = vertexSchemas;
        this.edgeSchemas = edgeSchemas;
    }

    @Override
    public <E extends Element> Iterator<E> search(SearchQuery<E> uniQuery) {
        Set<? extends RowSchema<E>> schemas = getSchemas(uniQuery.getReturnType());
        Set<PredicatesHolder> schemasPredicates = schemas.stream().map(schema ->
                schema.toPredicates(uniQuery.getPredicates())).collect(Collectors.toSet());
        PredicatesHolder schemaPredicateHolders = PredicatesHolderFactory.or(schemasPredicates);
        return search(schemaPredicateHolders, schemas, uniQuery.getLimit(), uniQuery.getStepDescriptor());
    }

    private <E extends Element> Iterator<E> search(PredicatesHolder allPredicates, Set<? extends RowSchema<E>> schemas, int limit, StepDescriptor stepDescriptor) {
        if(schemas.size() == 0 || allPredicates.isAborted()) {
            return Iterators.emptyIterator();
        }







        throw new NotImplementedException();
    }

    @Override
    public Edge addEdge(AddEdgeQuery uniQuery) {
        UniEdge edge = new UniEdge(uniQuery.getProperties(), uniQuery.getOutVertex(), uniQuery.getInVertex(), this.graph);
//        this.getDslContext().select().from()
        throw new NotImplementedException();
    }

    @Override
    public Vertex addVertex(AddVertexQuery uniQuery) {
        return null;
    }

    @Override
    public <E extends Element> void property(PropertyQuery<E> uniQuery) {

    }

    @Override
    public <E extends Element> void remove(RemoveQuery<E> uniQuery) {

    }

    @Override
    public void fetchProperties(DeferredVertexQuery query) {

    }

    @Override
    public Iterator<Edge> search(SearchVertexQuery uniQuery) {
        return null;
    }

    public DSLContext getDslContext() {
        return dslContext;
    }

    private <E extends Element> Set<RowSchema<E>> getSchemas(Class elementClass) {
        if(Vertex.class.isAssignableFrom(elementClass)) {
            return (Set<RowSchema<E>>) vertexSchemas;
        }
        else {
            return (Set<RowSchema<E>>) edgeSchemas;
        }
    }
}
