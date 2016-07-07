package org.unipop.jdbc.controller.simple;

import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.keyvalue.DefaultMapEntry;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.javatuples.Tuple;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unipop.common.util.PredicatesTranslator;
import org.unipop.jdbc.controller.simple.results.ElementMapper;
import org.unipop.jdbc.schemas.RowEdgeSchema;
import org.unipop.jdbc.schemas.RowVertexSchema;
import org.unipop.jdbc.schemas.jdbc.JdbcEdgeSchema;
import org.unipop.jdbc.schemas.jdbc.JdbcSchema;
import org.unipop.jdbc.schemas.jdbc.JdbcVertexSchema;
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
import org.unipop.schema.element.ElementSchema;
import org.unipop.schema.element.SchemaSet;
import org.unipop.schema.reference.DeferredVertex;
import org.unipop.structure.UniEdge;
import org.unipop.structure.UniElement;
import org.unipop.structure.UniGraph;
import org.unipop.structure.UniVertex;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
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

    private Set<? extends RowVertexSchema> vertexSchemas;
    private Set<? extends RowEdgeSchema> edgeSchemas;

    private final PredicatesTranslator<Condition> predicatesTranslator;

    public <E extends Element> RowController(UniGraph graph, DSLContext context, Set<JdbcSchema> schemaSet, PredicatesTranslator<Condition> predicatesTranslator) {
        this.graph = graph;
        this.dslContext = context;

//        this.vertexSchemas = schemaSet.get(RowVertexSchema.class, true);
//        this.edgeSchemas = schemaSet.get(RowEdgeSchema.class, true);

        extractRowSchemas(schemaSet);

        this.predicatesTranslator = predicatesTranslator;
    }

    @Override
    public <E extends Element> Iterator<E> search(SearchQuery<E> uniQuery) {
        Set<? extends JdbcSchema<E>> schemas = this.getSchemas(uniQuery.getReturnType());

        Function<JdbcSchema<E>, PredicatesHolder> toPredicatesFunction = (schema) -> schema.toPredicates(uniQuery.getPredicates());

        return this.search(toPredicatesFunction, schemas, uniQuery);
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
        logger.debug("executing removal query, RemoveQuery: {}", uniQuery);
        uniQuery.getElements().forEach(el -> {
            Set<? extends JdbcSchema<E>> schemas = this.getSchemas(el.getClass());
            logger.debug("removing element from all schemas, element: {}, schemas: {}", el, schemas);

            for (JdbcSchema<E> schema : schemas) {
                logger.debug("removing element from schema. element: {}, schema: {}", el, schema);
                DeleteWhereStep deleteStep = this.getDslContext().delete(table(schema.getTable()));

                Condition conditions = this.translateElementsToConditions(schema, Collections.singletonList(el));
                Delete step = deleteStep.where(conditions);

                logger.info("Created and executing delete step with conditions, step: {}", step);
                step.execute();
            }
        });
    }

    @Override
    public void fetchProperties(DeferredVertexQuery uniQuery) {
        Function<JdbcVertexSchema, PredicatesHolder> toPredicatesFunction = (schema) -> schema.toPredicates(uniQuery.getVertices());

        Iterator<Vertex> searchIterator = this.search(toPredicatesFunction, vertexSchemas, uniQuery);

        Map<Object, DeferredVertex> vertexMap = uniQuery.getVertices().stream().collect(Collectors.toMap(UniElement::id, Function.identity(), (a, b) -> a));
        logger.debug(
                "mapping between search results and deferred vertices, deferred vertexMap: {}, searchIterator: {}",
                vertexMap,
                searchIterator);

        searchIterator.forEachRemaining(newVertex -> {
            DeferredVertex deferredVertex = vertexMap.get(newVertex.id());
            logger.debug("mapping deferred vertex with new vertex, deferred: {}, new vertex: {}", deferredVertex, newVertex);
            if (deferredVertex != null) {
                deferredVertex.loadProperties(newVertex);
            }
        });
    }

    @Override
    public Iterator<Edge> search(SearchVertexQuery uniQuery) {
        Function<JdbcEdgeSchema, PredicatesHolder> toPredicatesFunction = (schema) -> schema.toPredicates(uniQuery.getVertices(), uniQuery.getDirection(), uniQuery.getPredicates());
        return this.search(
                toPredicatesFunction,
                this.edgeSchemas,
                uniQuery
        );
    }

    @SuppressWarnings("unchecked")
    private <E extends Element, S extends JdbcSchema<E>> Iterator<E> search(Function<S, PredicatesHolder> toSearchFunction, Set<? extends S> allSchemas, SearchQuery<E> query) {
        logger.debug("executing search with parameters: allPredicates: {}, schemas: {}, query: {}", toSearchFunction, allSchemas, query);

        Map<S, Select> schemas = allSchemas.stream()
                .map(schema -> Pair.of(schema, toSearchFunction.apply(schema)))
                .filter(pair -> pair.getRight() != null)
                .map(pair -> Pair.of(pair.getLeft(), pair.getLeft().getSearch(query, pair.getRight(), this.getDslContext())))
                .filter(pair -> pair.getRight() != null)
                .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));


        logger.debug("mapped schemas for search, schemas: {}", schemas);
        if(schemas.size() == 0) {
            logger.warn("schemas are empty, returning empty iterator");
            return EmptyIterator.instance();
        }

        Iterator<S> schemaIterator = schemas.keySet().iterator();

        return schemas.values().stream()
                .map(Select::fetch)
                .flatMap(res -> schemaIterator.next().parseResults(res, query).stream())
                .distinct().iterator();
    }

    @SuppressWarnings("unchecked")
    private <E extends Element> Set<? extends JdbcSchema<E>> getSchemas(Class elementClass) {
        if (Vertex.class.isAssignableFrom(elementClass)) {
            return vertexSchemas.stream().map(v -> (JdbcSchema<E>) v).collect(Collectors.toSet());
        } else {
            return edgeSchemas.stream().map(e -> (JdbcSchema<E>) e).collect(Collectors.toSet());
        }
    }

    private <E extends Element> void insert(Set<? extends JdbcSchema<E>> schemas, E element) {
        logger.debug("executing insertion of element, schemas: {}, element: {}", schemas, element);
        for (JdbcSchema<E> schema : schemas) {
            logger.debug("executing insertion for specific schema, schema: {}", schema);
            JdbcSchema.Row row = schema.toRow(element);
            logger.debug("formed row out of schema and element, row: {}", row);

            if (row == null) {
                logger.debug("row formed out of schema is null, continuing to next schema");
                continue;
            }

            Insert step = this.getDslContext().insertInto(table(
                    schema.getTable()),
                    CollectionUtils.collect(
                            row.getFields().keySet(),
                            DSL::field))
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

            if (row == null) {
                continue;
            }

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

    private <E extends Element> Condition translateElementsToConditions(JdbcSchema<E> schema, List<E> elements) {
        return this.predicatesTranslator.translate(
                new PredicatesHolder(
                        PredicatesHolder.Clause.Or,
                        elements.stream()
                                .map(schema::toFields)
                                .map(Map::entrySet)
                                .flatMap(m -> m.stream().map(es -> new HasContainer(es.getKey(), P.within(es.getValue()))))
                                .collect(Collectors.toList()), Collections.emptyList()));

    }

    private <E extends Element> void extractRowSchemas(Set<JdbcSchema> schemas) {
        logger.debug("extracting row schemas to element schemas, jdbcSchemas: {}", schemas);
        Set<JdbcSchema<E>> JdbcSchemas = collectSchemas(schemas);
        this.vertexSchemas = JdbcSchemas.stream().filter(schema -> schema instanceof RowVertexSchema)
                .map(schema -> ((RowVertexSchema)schema)).collect(Collectors.toSet());
        this.edgeSchemas = JdbcSchemas.stream().filter(schema -> schema instanceof RowEdgeSchema)
                .map(schema -> ((RowEdgeSchema)schema)).collect(Collectors.toSet());
        logger.info("extraced row schemas, vertexSchemas: {}, edgeSchemas: {}", this.vertexSchemas, this.edgeSchemas);
    }

    private <E extends Element> Set<JdbcSchema<E>> collectSchemas(Set<? extends ElementSchema> schemas) {
        Set<JdbcSchema<E>> rowSchemas = new HashSet<>();

        schemas.forEach(schema -> {
            if(schema instanceof JdbcSchema) {
                rowSchemas.add((JdbcSchema<E>) schema);
                Set<JdbcSchema<E>> childSchemas = collectSchemas(schema.getChildSchemas());
                rowSchemas.addAll(childSchemas);
            }
        });
        return rowSchemas;
    }

    public DSLContext getDslContext() {
        return this.dslContext;
    }

    @Override
    public String toString() {
        return "RowController{" +
                "dslContext=" + dslContext +
                ", graph=" + graph +
                ", vertexSchemas=" + vertexSchemas +
                ", edgeSchemas=" + edgeSchemas +
                ", predicatesTranslator=" + predicatesTranslator +
                '}';
    }
}
