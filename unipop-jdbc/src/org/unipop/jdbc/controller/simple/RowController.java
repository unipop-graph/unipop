package org.unipop.jdbc.controller.simple;

import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.keyvalue.DefaultMapEntry;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unipop.common.util.PredicatesTranslator;
import org.unipop.jdbc.controller.simple.results.ElementMapper;
import org.unipop.jdbc.schemas.RowEdgeSchema;
import org.unipop.jdbc.schemas.RowVertexSchema;
import org.unipop.jdbc.schemas.jdbc.JdbcSchema;
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
import org.unipop.schema.element.SchemaSet;
import org.unipop.schema.reference.DeferredVertex;
import org.unipop.structure.UniEdge;
import org.unipop.structure.UniElement;
import org.unipop.structure.UniGraph;
import org.unipop.structure.UniVertex;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

/**
 * @author Gur Ronen
 * @since 6/12/2016
 */
public class RowController implements SimpleController {
    private final static Logger logger = LoggerFactory.getLogger(RowController.class);

    private final DSLContext dslContext;
    private final UniGraph graph;

    private final Set<? extends RowVertexSchema> vertexSchemas;
    private final Set<? extends RowEdgeSchema> edgeSchemas;

    private final PredicatesTranslator<Iterable<Condition>> predicatesTranslator;

    public RowController(UniGraph graph, DSLContext context, SchemaSet schemaSet, PredicatesTranslator<Iterable<Condition>> predicatesTranslator) {
        this.graph = graph;
        this.dslContext = context;

        this.vertexSchemas = schemaSet.get(RowVertexSchema.class, true);
        this.edgeSchemas = schemaSet.get(RowEdgeSchema.class, true);

        this.predicatesTranslator = predicatesTranslator;
    }

    @Override
    public <E extends Element> Iterator<E> search(SearchQuery<E> uniQuery) {
        Set<? extends JdbcSchema<E>> schemas = this.getSchemas(uniQuery.getReturnType());
        PredicatesHolder schemaPredicateHolders = this.extractPredicatesHolder(uniQuery, schemas);

        return this.search(schemaPredicateHolders, schemas, uniQuery.getLimit(), uniQuery.getStepDescriptor(), uniQuery.getPropertyKeys());
    }

    @Override
    public Edge addEdge(AddEdgeQuery uniQuery) {
        UniEdge edge = new UniEdge(uniQuery.getProperties(), uniQuery.getOutVertex(), uniQuery.getInVertex(), this.graph);
        try {
            this.insert(edgeSchemas, edge);
        } catch (IllegalArgumentException ex) {
            throw Graph.Exceptions.edgeWithIdAlreadyExists(edge.id());
        }

        return edge;
    }

    @Override
    public Vertex addVertex(AddVertexQuery uniQuery) {
        UniVertex vertex = new UniVertex(uniQuery.getProperties(), this.graph);
        try {
            this.insert(this.vertexSchemas, vertex);
        } catch (IllegalArgumentException ex) {
            throw Graph.Exceptions.vertexWithIdAlreadyExists(vertex.id());
        }

        return vertex;
    }

    @Override
    public <E extends Element> void property(PropertyQuery<E> uniQuery) {
        Set<? extends JdbcSchema<E>> schemas = this.getSchemas(uniQuery.getElement().getClass());
        this.update(schemas, uniQuery.getElement());
    }

    @Override
    public <E extends Element> void remove(RemoveQuery<E> uniQuery) {
        uniQuery.getElements().forEach(el -> {
            Set<? extends JdbcSchema<E>> schemas = this.getSchemas(el.getClass());

            for (JdbcSchema<E> schema : schemas) {
                DeleteWhereStep deleteStep = this.getDslContext().delete(table(schema.getTable()));

                Collection<Condition> conditions = this.translateElementsToConditions(schema, Collections.singletonList(el));
                deleteStep.where(conditions).execute();
            }
        });
    }

    @Override
    public void fetchProperties(DeferredVertexQuery uniQuery) {
        Set<PredicatesHolder> schemasPredicates = this.vertexSchemas.stream().map(schema ->
                schema.toPredicates(uniQuery.getVertices())).collect(Collectors.toSet());
        PredicatesHolder schemaPredicateHolders = PredicatesHolderFactory.or(schemasPredicates);

        if (schemaPredicateHolders.isEmpty()) {
            return;
        }
        Iterator<Vertex> search = this.search(schemaPredicateHolders, vertexSchemas, -1, uniQuery.getStepDescriptor(), uniQuery.getPropertyKeys());

        Map<Object, DeferredVertex> vertexMap = uniQuery.getVertices().stream().collect(Collectors.toMap(UniElement::id, Function.identity(), (a, b) -> a));
        search.forEachRemaining(newVertex -> {
            DeferredVertex deferredVertex = vertexMap.get(newVertex.id());
            if (deferredVertex != null) deferredVertex.loadProperties(newVertex);
        });
    }

    @Override
    public Iterator<Edge> search(SearchVertexQuery uniQuery) {
        Set<PredicatesHolder> schemasPredicates = edgeSchemas.stream().map(schema ->
                schema.toPredicates(uniQuery.getVertices(), uniQuery.getDirection(), uniQuery.getPredicates()))
                .collect(Collectors.toSet());
        PredicatesHolder schemaPredicateHolders = PredicatesHolderFactory.or(schemasPredicates);
        return this.search(
                schemaPredicateHolders,
                this.getSchemas(uniQuery.getReturnType()),
                uniQuery.getLimit(),
                uniQuery.getStepDescriptor(),
                uniQuery.getPropertyKeys()
        );
    }

    @SuppressWarnings("unchecked")
    private <E extends Element> Iterator<E> search(PredicatesHolder allPredicates, Set<? extends JdbcSchema<E>> schemas, int limit, StepDescriptor stepDescriptor, Set<String> propertyKeys) {
        logger.debug("executing search with parameters: allPredicates: {}, schemas: {}, limit: {}, stepDescriptor: {}, propertyKeys: {}", allPredicates, schemas, limit, stepDescriptor, propertyKeys);
        if (schemas.size() == 0 || allPredicates.isAborted()) {
            logger.warn("there are no schemas, or the predicate has been aborted, returning empty iterator. schemas: {}, allPredicates: {}", schemas, allPredicates);
            return Iterators.emptyIterator();
        }

        Iterator<Condition> conditions = this.predicatesTranslator.translate(allPredicates).iterator();
        logger.debug("translated predicates to condition iterator: {}", conditions);
        Stream<String> tables = schemas.stream().map(JdbcSchema::getTable);
        logger.debug("extracted table names from schemas, tables: {}", tables);

        int finalLimit = limit < 0 ? Integer.MAX_VALUE : limit;
        logger.debug("validated limit, initialLimit: {}, limit used in query: {}", limit, finalLimit);

        return (Iterator<E>) tables.flatMap(table -> {
            Select step = createSqlQuery(
                    propertyKeys, table)
                    .where(IteratorUtils.list(conditions))
                    .limit(finalLimit);
            logger.debug("executing SQL query, step: {}", step);

            return step.fetch()
                    .map(new ElementMapper(schemas)).stream();
        })
                .distinct()
                .iterator();
    }

    private SelectJoinStep<Record> createSqlQuery(Set<String> columnsToRetrieve, String table) {
        if (columnsToRetrieve == null) {
            return this.getDslContext().select().from(table);

        }

        return this.getDslContext()
                .select(columnsToRetrieve.stream().map(DSL::field).collect(Collectors.toList()))
                .from(table);
    }

    @SuppressWarnings("unchecked")
    private <E extends Element> Set<JdbcSchema<E>> getSchemas(Class elementClass) {
        if (Vertex.class.isAssignableFrom(elementClass)) {
            return vertexSchemas.stream().map(v -> (JdbcSchema<E>) v).collect(Collectors.toSet());
        } else {
            return edgeSchemas.stream().map(e -> (JdbcSchema<E>) e).collect(Collectors.toSet());
        }
    }

    private <E extends Element> PredicatesHolder extractPredicatesHolder(SearchQuery<E> uniQuery, Set<? extends JdbcSchema<E>> schemas) {
        Set<PredicatesHolder> schemasPredicates = schemas.stream().map(schema ->
                schema.toPredicates(uniQuery.getPredicates())).collect(Collectors.toSet());
        return PredicatesHolderFactory.or(schemasPredicates);
    }

    private <E extends Element> void insert(Set<? extends JdbcSchema<E>> schemas, E element) {
        logger.debug("executing insertion of element, schemas: {}, element: {}", schemas, element);
        for (JdbcSchema<E> schema : schemas) {
            logger.debug("executing insertion for specific schema, schema: {}", schema);
            JdbcSchema.Row row = schema.toRow(element);
            logger.debug("formed row out of schema and element, row: {}", row);

            Insert step = this.getDslContext().insertInto(table(schema.getTable()), CollectionUtils.collect(row.getFields().keySet(), DSL::field))
                    .values(row.getFields().values())
                    .onDuplicateKeyIgnore();

            logger.info("executing insertion, step: {}", step);
            int changeSetCount = step.execute();
            logger.debug("changeSet out of executed step: {}", changeSetCount);
            if (changeSetCount == 0) {
                logger.warn("change set == 0, invalid insertion. throwing IllegalArgumentException, rowId: {}", row.getId());
                throw new IllegalArgumentException("element with same key already exists:" + row.getId());
            }
        }
    }

    private <E extends Element> void update(Set<? extends JdbcSchema<E>> schemas, E element) {
        logger.debug("executing update of element, schemas: {}, element: {}", schemas, element);
        for (JdbcSchema<E> schema : schemas) {
            logger.debug("executing update for specific schema: {}", schema);
            JdbcSchema.Row row = schema.toRow(element);
            logger.debug("formed row out of schema and element, row: {}", row);

            Map<Field<?>, Object> fieldMap = Maps.newHashMap();
            row.getFields().entrySet().stream().map(this::mapSet).forEach(en -> fieldMap.put(en.getKey(), en.getValue()));
            fieldMap.remove(row.getIdField());
            logger.debug("formed fieldMap to update, fieldMap: {}", fieldMap);

            Update step = this.getDslContext().update(table(schema.getTable()))
                    .set(fieldMap)
                    .where(field(row.getIdField()).eq(row.getId()));
            logger.info("executing update statement with following parameters, step: {}, element: {}, schema: {}", step, element, schema);
            step.execute();
        }
    }

    private Map.Entry<Field<?>, Object> mapSet(Map.Entry<String, Object> entry) {
        return new DefaultMapEntry<>(field(entry.getKey()), entry.getValue());
    }

    private <E extends Element> Collection<Condition> translateElementsToConditions(JdbcSchema<E> schema, List<E> elements) {
        return StreamSupport.stream(this.predicatesTranslator.translate(
                new PredicatesHolder(
                        PredicatesHolder.Clause.Or,
                        elements.stream()
                                .map(schema::toFields)
                                .map(Map::entrySet)
                                .flatMap(m -> m.stream().map(es -> new HasContainer(es.getKey(), P.within(es.getValue()))))
                                .collect(Collectors.toList()), Collections.emptyList())).spliterator(), false)
                .collect(Collectors.toSet());

    }

    public DSLContext getDslContext() {
        return this.dslContext;
    }
}
