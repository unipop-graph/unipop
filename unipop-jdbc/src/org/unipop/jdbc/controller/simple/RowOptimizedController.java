package org.unipop.jdbc.controller.simple;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.javatuples.Pair;
import org.jooq.Condition;
import org.jooq.DSLContext;
import org.jooq.Select;
import org.unipop.common.util.PredicatesTranslator;
import org.unipop.jdbc.schemas.jdbc.JdbcEdgeSchema;
import org.unipop.jdbc.schemas.jdbc.JdbcSchema;
import org.unipop.query.aggregation.LocalQuery;
import org.unipop.structure.UniGraph;
import org.unipop.util.ConversionUtils;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Created by sbarzilay on 9/18/16.
 */
public class RowOptimizedController extends RowController implements LocalQuery.LocalController{

    public <E extends Element> RowOptimizedController(UniGraph graph, DSLContext context, Set<JdbcSchema> schemaSet, PredicatesTranslator<Condition> predicatesTranslator) {
        super(graph, context, schemaSet, predicatesTranslator);
    }

    @Override
    public <S extends Element> Iterator<Pair<String, S>> local(LocalQuery<S> query) {
        SelectCollector<JdbcSchema<Edge>, Select, org.javatuples.Pair<String, Element>> collector = new SelectCollector<>(
                (schema) -> ((JdbcEdgeSchema) schema).getLocal(query, dslContext),
                (schema, results) -> ((JdbcEdgeSchema) schema).parseLocal(results, query)
        );

        Map<JdbcSchema<Edge>, Select> selects = edgeSchemas.stream().collect(collector);

        Iterator<org.javatuples.Pair<String, Element>> search = search(query, selects, collector);
        return ConversionUtils.asStream(search).map(o -> ((org.javatuples.Pair<String, S>)o)).iterator();
    }
}
