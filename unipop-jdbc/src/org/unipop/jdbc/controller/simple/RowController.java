package org.unipop.jdbc.controller.simple;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.keyvalue.DefaultMapEntry;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.unipop.common.schema.referred.DeferredVertex;
import org.unipop.common.util.PredicatesTranslator;
import org.unipop.jdbc.controller.simple.results.ElementMapper;
import org.unipop.jdbc.schemas.RowEdgeSchema;
import org.unipop.jdbc.schemas.RowSchema;
import org.unipop.jdbc.schemas.RowVertexSchema;
import org.unipop.jdbc.utils.TableStrings;
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
import org.unipop.structure.UniElement;
import org.unipop.structure.UniGraph;
import org.unipop.structure.UniVertex;

import java.sql.Connection;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.jooq.impl.DSL.*;

/**
 * @author GurRo
 * @since 6/12/2016
 */
public class RowController implements SimpleController {
    private final DSLContext dslContext;
    private final UniGraph graph;
    private final Set<? extends RowVertexSchema> vertexSchemas;
    private final Set<? extends RowEdgeSchema> edgeSchemas;

    private final PredicatesTranslator<Iterable<Condition>> predicatesTranslator;

    public RowController(UniGraph graph, Connection conn, SQLDialect dialect, Set<RowVertexSchema> vertexSchemas, Set<RowEdgeSchema> edgeSchemas, PredicatesTranslator<Iterable<Condition>> predicatesTranslator) {
        this.graph = graph;
        this.dslContext = using(conn, dialect);

        this.vertexSchemas = vertexSchemas;
        this.edgeSchemas = edgeSchemas;

        this.predicatesTranslator = predicatesTranslator;
    }

    @Override
    public <E extends Element> Iterator<E> search(SearchQuery<E> uniQuery) {
        Set<? extends RowSchema<E>> schemas = this.getSchemas(uniQuery.getReturnType());
        PredicatesHolder schemaPredicateHolders = this.extractPredicatesHolder(uniQuery, schemas);
        return this.search(schemaPredicateHolders, schemas, uniQuery.getLimit(), uniQuery.getStepDescriptor());
    }

    @Override
    public Edge addEdge(AddEdgeQuery uniQuery) {
        UniEdge edge = new UniEdge(uniQuery.getProperties(), uniQuery.getOutVertex(), uniQuery.getInVertex(), this.graph);
        this.insert(edgeSchemas, edge);

        return edge;
    }

    @Override
    public Vertex addVertex(AddVertexQuery uniQuery) {
        UniVertex vertex = new UniVertex(uniQuery.getProperties(), this.graph);
        this.insert(vertexSchemas, vertex);

        return vertex;
    }

    @Override
    public <E extends Element> void property(PropertyQuery<E> uniQuery) {
        Set<? extends RowSchema<E>> schemas = this.getSchemas(uniQuery.getElement().getClass());
        this.insert(schemas, uniQuery.getElement());
    }

    @Override
    public <E extends Element> void remove(RemoveQuery<E> uniQuery) {
        uniQuery.getElements().forEach(el -> {
            Set<? extends RowSchema<E>> schemas = this.getSchemas(el.getClass());

            for (RowSchema<E> schema : schemas) {
                DeleteWhereStep deleteStep = this.getDslContext().delete(table(schema.getTable(el)));

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

        if(schemaPredicateHolders.isEmpty()) {
            return;
        }
        Iterator<Vertex> search = this.search(schemaPredicateHolders, vertexSchemas, -1, uniQuery.getStepDescriptor());

        Map<Object, DeferredVertex> vertexMap = uniQuery.getVertices().stream().collect(Collectors.toMap(UniElement::id, Function.identity(), (a, b) -> a));
        search.forEachRemaining(newVertex -> {
            DeferredVertex deferredVertex = vertexMap.get(newVertex.id());
            if(deferredVertex != null) deferredVertex.loadProperties(newVertex);
        });
    }

    @Override
    public Iterator<Edge> search(SearchVertexQuery uniQuery) {
        return this.search(
                uniQuery.getPredicates(),
                this.getSchemas(uniQuery.getReturnType()),
                uniQuery.getLimit(),
                uniQuery.getStepDescriptor()
        );
    }

    private <E extends Element> Iterator<E> search(PredicatesHolder allPredicates, Set<? extends RowSchema<E>> schemas, int limit, StepDescriptor stepDescriptor) {
        if (schemas.size() == 0 || allPredicates.isAborted()) {
            return Iterators.emptyIterator();
        }

        Set<Field<Object>> columnsToRetrieve = allPredicates
                .getPredicates()
                .stream()
                .map(HasContainer::getKey)
                .map(DSL::field)
                .collect(Collectors.toSet());

        Iterator<Condition> conditions = this.predicatesTranslator.translate(allPredicates).iterator();
        Iterator<String> tables = schemas.stream().map(RowSchema::getTable).iterator();
        String firstTable = tables.next();
        SelectWhereStep step = createSqlQuery(columnsToRetrieve, firstTable);

        tables.forEachRemaining(table -> step
                .unionAll(createSqlQuery(columnsToRetrieve, table)));

        conditions.forEachRemaining(step::where);

        step.limit(limit);

        return (Iterator<E>) step.fetch().map(new ElementMapper(schemas)).iterator();
    }

    private SelectJoinStep<Record> createSqlQuery(Set<Field<Object>> columnsToRetrieve, String table) {
        return this.getDslContext()
                .select(Stream.concat(
                        columnsToRetrieve.stream(),
                        Stream.of(getTableField(table)))
                        .collect(Collectors.toSet()))
                .from(table);
    }

    public DSLContext getDslContext() {
        return this.dslContext;
    }

    private Field<Object> getTableField(String tableName) {
        return field(String.format("'%s' as '%s'", tableName, TableStrings.TABLE_COLUMN_NAME));
    }

    @SuppressWarnings("unchecked")
    private <E extends Element> Set<RowSchema<E>> getSchemas(Class elementClass) {
        if (Vertex.class.isAssignableFrom(elementClass)) {
            return vertexSchemas.stream().map(v -> (RowSchema<E>) v).collect(Collectors.toSet());
        } else {
            return edgeSchemas.stream().map(e -> (RowSchema<E>) e).collect(Collectors.toSet());
        }
    }

    private <E extends Element> PredicatesHolder extractPredicatesHolder(SearchQuery<E> uniQuery, Set<? extends RowSchema<E>> schemas) {
        Set<PredicatesHolder> schemasPredicates = schemas.stream().map(schema ->
                schema.toPredicates(uniQuery.getPredicates())).collect(Collectors.toSet());
        return PredicatesHolderFactory.or(schemasPredicates);
    }

    private <E extends Element> void insert(Set<? extends RowSchema<E>> schemas, E element) {
        for (RowSchema<E> schema : schemas) {
            RowSchema.Row row = schema.toRow(element);

            int changeSetCount = this.getDslContext().insertInto(table(schema.getTable(element)), CollectionUtils.collect(row.getFields().keySet(), DSL::field))
                    .values(row.getFields().values())
                    .onDuplicateKeyIgnore().execute();

            if (changeSetCount == 0) {
                Map<Field<?>, Object> fieldMap = Maps.newHashMap();
                row.getFields().entrySet().stream().map(this::mapSet).forEach(en -> fieldMap.put(en.getKey(), en.getValue()));

                this.getDslContext().update(table(schema.getTable(element)))
                .set(fieldMap).execute();
            }
        }
    }

    private Map.Entry<Field<?>, Object> mapSet(Map.Entry<String, Object> entry) {
        return new DefaultMapEntry<>(field(entry.getKey()), entry.getValue());
    }

    private <E extends Element> Collection<Condition> translateElementsToConditions(RowSchema<E> schema, List<E> elements) {
        return StreamSupport.stream(this.predicatesTranslator.translate(
                new PredicatesHolder(
                        PredicatesHolder.Clause.Or,
                        elements.stream()
                                .map(schema::toFields)
                                .map(row -> row.entrySet())
                                .flatMap(m -> m.stream().map(es -> new HasContainer(es.getKey(), P.eq(es.getValue()))))
                                .collect(Collectors.toSet()), Collections.emptySet())).spliterator(), false).collect(Collectors.toSet());

    }
}
