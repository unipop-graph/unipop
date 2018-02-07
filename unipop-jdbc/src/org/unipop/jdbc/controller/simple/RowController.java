package org.unipop.jdbc.controller.simple;

import com.google.common.collect.Maps;
import org.apache.commons.collections4.keyvalue.DefaultMapEntry;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.jooq.*;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unipop.common.util.PredicatesTranslator;
import org.unipop.jdbc.schemas.RowEdgeSchema;
import org.unipop.jdbc.schemas.RowVertexSchema;
import org.unipop.jdbc.schemas.jdbc.JdbcEdgeSchema;
import org.unipop.jdbc.schemas.jdbc.JdbcSchema;
import org.unipop.jdbc.utils.ContextManager;
import org.unipop.jdbc.utils.TimingExecuterListener;
import org.unipop.query.UniQuery;
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
import org.unipop.schema.reference.DeferredVertex;
import org.unipop.structure.traversalfilter.TraversalFilter;
import org.unipop.structure.UniEdge;
import org.unipop.structure.UniElement;
import org.unipop.structure.UniGraph;
import org.unipop.structure.UniVertex;
import org.unipop.util.MetricsRunner;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.*;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

/**
 * @author Gur Ronen
 * @since 6/12/2016
 */
public class RowController implements SimpleController {
    protected final static Logger logger = LoggerFactory.getLogger(RowController.class);

    private final ContextManager contextManager;
    private final UniGraph graph;

    protected Set<? extends RowVertexSchema> vertexSchemas;
    protected Set<? extends RowEdgeSchema> edgeSchemas;

    protected List<Query> bulk;
    private final PredicatesTranslator<Condition> predicatesTranslator;

    private TraversalFilter traversalFilter;

    public <E extends Element> RowController(UniGraph graph, ContextManager contextManager, Set<JdbcSchema> schemaSet, PredicatesTranslator<Condition> predicatesTranslator, TraversalFilter traversalFilter) {
        this.graph = graph;
        this.contextManager = contextManager;

        extractRowSchemas(schemaSet);
        this.predicatesTranslator = predicatesTranslator;
        bulk = new ArrayList<>();

        this.traversalFilter = traversalFilter;
    }

    @Override
    public <E extends Element> Iterator<E> search(SearchQuery<E> uniQuery) {

        SelectCollector<JdbcSchema<E>, Select, E> collector = new SelectCollector<>(
                schema -> schema.getSearch(uniQuery,
                        schema.toPredicates(uniQuery.getPredicates())),
                (schema, results) -> schema.parseResults(results, uniQuery)
        );
        Set<? extends JdbcSchema<E>> schemas = this.getSchemas(uniQuery.getReturnType());

        Map<JdbcSchema<E>, Select> selects = schemas.stream()
                .filter(schema -> this.traversalFilter.filter(schema, uniQuery.getTraversal())).collect(collector);


        return this.search(uniQuery, selects, collector);
    }

    @Override
    public void fetchProperties(DeferredVertexQuery uniQuery) {
        SelectCollector<JdbcSchema<Vertex>, Select, Vertex> collector = new SelectCollector<>(
                schema -> schema.getSearch(uniQuery,
                        schema.toPredicates(uniQuery.getPredicates())),
                (schema, results) -> schema.parseResults(results, uniQuery)
        );

        Map<JdbcSchema<Vertex>, Select> selects = vertexSchemas.stream()
                .filter(schema -> this.traversalFilter.filter(schema, uniQuery.getTraversal())).collect(collector);
        Iterator<Vertex> searchIterator = this.search(uniQuery, selects, collector);

        Map<Object, DeferredVertex> vertexMap =
                uniQuery.getVertices().stream().collect(Collectors.toMap(UniElement::id, Function.identity(), (a, b) -> a));
        searchIterator.forEachRemaining(newVertex -> {
            DeferredVertex deferredVertex = vertexMap.get(newVertex.id());
            if (deferredVertex != null) {
                deferredVertex.loadProperties(newVertex);
            }
        });
    }

    @Override
    public Iterator<Edge> search(SearchVertexQuery uniQuery) {
        SelectCollector<JdbcSchema<Edge>, Select, Edge> collector = new SelectCollector<>(
                schema -> schema.getSearch(uniQuery,
                        ((JdbcEdgeSchema) schema).toPredicates(uniQuery.getVertices(), uniQuery.getDirection(), uniQuery.getPredicates())),
                (schema, results) -> schema.parseResults(results, uniQuery)
        );

        Map<JdbcSchema<Edge>, Select> selects = edgeSchemas.stream()
                .filter(schema -> this.traversalFilter.filter(schema, uniQuery.getTraversal())).collect(collector);

        return this.search(uniQuery, selects, collector);
    }

    @Override
    public Edge addEdge(AddEdgeQuery uniQuery) {
        UniEdge edge = new UniEdge(uniQuery.getProperties(), uniQuery.getOutVertex(), uniQuery.getInVertex(), this.graph);
        try {
            if (this.insert(edgeSchemas, edge)) return edge;
        } catch (DataAccessException ex) {
            throw Graph.Exceptions.edgeWithIdAlreadyExists(edge.id());
        }

        return null;
    }

    @Override
    public Vertex addVertex(AddVertexQuery uniQuery) {
        UniVertex vertex = new UniVertex(uniQuery.getProperties(), this.graph);
        try {
            if (this.insert(this.vertexSchemas, vertex)) {
                return vertex;
            }
        } catch (DataAccessException ex) {
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
                DeleteWhereStep deleteStep = DSL.delete(table(schema.getTable()));

                Condition conditions = this.translateElementsToConditions(schema, Collections.singletonList(el));
                Delete step = deleteStep.where(conditions);

                getContextManager().execute(step);
                logger.debug("removed element. element: {}, schema: {}, command: {}", el, schema, step);
            }
        });
    }

    @SuppressWarnings("unchecked")
    private <E extends Element> Set<? extends JdbcSchema<E>> getSchemas(Class elementClass) {
        if (Vertex.class.isAssignableFrom(elementClass)) {
            return vertexSchemas.stream().map(v -> (JdbcSchema<E>) v).collect(Collectors.toSet());
        } else {
            return edgeSchemas.stream().map(e -> (JdbcSchema<E>) e).collect(Collectors.toSet());
        }
    }

    private <E extends Element, S extends JdbcSchema<E>> void fillChildren(List<MutableMetrics> children, Map<S, Select> schemas) {
        List<org.javatuples.Pair<Long, Integer>> timing = TimingExecuterListener.timing.values().stream().collect(Collectors.toList());
        List<Map.Entry<S, Select>> sqls = schemas.entrySet().stream().collect(Collectors.toList());
        for (int i = 0; i < sqls.size(); i++) {
            org.javatuples.Pair<Long, Integer> timingCount = timing.get(i);
            if (i < children.size()) {
                MutableMetrics child = children.get(i);
                if (timingCount != null) {
                    child.setCount(TraversalMetrics.ELEMENT_COUNT_ID, timingCount.getValue1());
                    child.setDuration(timingCount.getValue0(), TimeUnit.NANOSECONDS);
                }
            }
            timing.remove(sqls.get(i).getValue().getSQL());
        }
    }

    @SuppressWarnings("unchecked")
    protected <E extends Element, R> Iterator<R> search(UniQuery query, Map<JdbcSchema<E>, Select> selects,
                                                        SelectCollector<JdbcSchema<E>, Select, R> collector) {

        if (bulk.size() != 0) {
            contextManager.batch(bulk);
            bulk.clear();
        }
        MetricsRunner metrics = new MetricsRunner(this, query,
                selects.keySet().stream().map(s -> ((ElementSchema) s)).collect(Collectors.toList()));

        logger.info("mapped schemas for search, schemas: {}", selects);
        if (selects.size() == 0) return EmptyIterator.instance();

        Iterator<JdbcSchema<E>> schemaIterator = selects.keySet().iterator();
        Set<R> collect = selects.values().stream()
                .map(select -> this.getContextManager().fetch(select))
                .flatMap(res -> collector.parse.apply(schemaIterator.next(), res).stream())
                .collect(Collectors.toSet());


        metrics.stop((children) -> fillChildren(children, selects));

        logger.info("results: {}", collect);
        return collect.iterator();
    }

    private <E extends Element> boolean insert(Set<? extends JdbcSchema<E>> schemas, E element) {
        for (JdbcSchema<E> schema : schemas) {
            Query query = schema.getInsertStatement(element);
            if (query == null) continue;
//            int changeSetCount = contextManager.execute(query);
//            if (logger.isDebugEnabled())
//                logger.debug("executed insertion, query: {}", contextManager.render(query));
//            if (changeSetCount == 0) {
//                logger.error("no rows changed on insertion. query: {}, element: {}", contextManager.render(query), element);
//            }
//            else {
            bulk.add(query);
            if (bulk.size() >= 1000) {
                contextManager.batch(bulk);
                bulk.clear();
            }
            return true;
//            }
        }
        return false;
    }

    private <E extends Element> void update(Set<? extends JdbcSchema<E>> schemas, E element) {
        for (JdbcSchema<E> schema : schemas) {
            JdbcSchema.Row row = schema.toRow(element);

            if (row == null) continue;

            Map<Field<?>, Object> fieldMap = Maps.newHashMap();
            row.getFields().entrySet().stream().map(this::mapSet).forEach(en -> fieldMap.put(en.getKey(), en.getValue()));

            Update step = DSL.update(table(schema.getTable()))
                    .set(fieldMap).where(field(schema.getFieldByPropertyKey(T.id.getAccessor())).eq(row.getId()));

//            this.getContextManager().execute(step);
            logger.info("executed update statement with following parameters, step: {}, element: {}, schema: {}", this.getContextManager().render(step), element, schema);
//            contextManager.execute("commit;");
            bulk.add(step);
            if (bulk.size() >= 1000) {
                contextManager.batch(bulk);
                bulk.clear();
            }
        }
    }

    private Map.Entry<Field<?>, Object> mapSet(Map.Entry<String, Object> entry) {
        return new DefaultMapEntry<>(field(entry.getKey()), entry.getValue());
    }

    private <E extends Element> Condition translateElementsToConditions(JdbcSchema<E> schema, List<E> elements) {
        return this.predicatesTranslator.translate(
                new PredicatesHolder(
                        PredicatesHolder.Clause.And,
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
                .map(schema -> ((RowVertexSchema) schema)).collect(Collectors.toSet());
        this.edgeSchemas = JdbcSchemas.stream().filter(schema -> schema instanceof RowEdgeSchema)
                .map(schema -> ((RowEdgeSchema) schema)).collect(Collectors.toSet());
        logger.info("extraced row schemas, vertexSchemas: {}, edgeSchemas: {}", this.vertexSchemas, this.edgeSchemas);
    }

    private <E extends Element> Set<JdbcSchema<E>> collectSchemas(Set<? extends ElementSchema> schemas) {
        Set<JdbcSchema<E>> rowSchemas = new HashSet<>();

        schemas.forEach(schema -> {
            if (schema instanceof JdbcSchema) {
                rowSchemas.add((JdbcSchema<E>) schema);
                Set<JdbcSchema<E>> childSchemas = collectSchemas(schema.getChildSchemas());
                rowSchemas.addAll(childSchemas);
            }
        });
        return rowSchemas;
    }

    public ContextManager getContextManager() {
        return this.contextManager;
    }

    @Override
    public String toString() {
        return "RowController{" +
                "contextManager=" + contextManager +
                ", graph=" + graph +
                ", vertexSchemas=" + vertexSchemas +
                ", edgeSchemas=" + edgeSchemas +
                ", predicatesTranslator=" + predicatesTranslator +
                '}';
    }

    public class SelectCollector<K, V, R> implements Collector<K, Map<K, V>, Map<K, V>> {

        private final Function<? super K, ? extends V> valueMapper;

        private final BiFunction<? super K, List<Map<String, Object>>, ? extends Collection<R>> parse;

        public SelectCollector(Function<? super K, ? extends V> valueMapper, BiFunction<? super K, List<Map<String, Object>>, Collection<R>> parse) {
            this.valueMapper = valueMapper;
            this.parse = parse;
        }

        @Override
        public Supplier<Map<K, V>> supplier() {
            return HashMap<K, V>::new;
        }

        @Override
        public BiConsumer<Map<K, V>, K> accumulator() {
            return (map, t) -> {
                V value = valueMapper.apply(t);
                if (value != null) map.put(t, value);
            };
        }

        @Override
        public BinaryOperator<Map<K, V>> combiner() {
            return (map1, map2) -> {
                map1.putAll(map2);
                return map1;
            };
        }

        @Override
        public Function finisher() {
            return m -> m;
        }

        @Override
        public Set<Characteristics> characteristics() {
            return EnumSet.of(Collector.Characteristics.IDENTITY_FINISH);
        }
    }
}
