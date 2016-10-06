package org.unipop.jdbc.schemas;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.jooq.*;
import org.jooq.impl.DSL;
import org.json.JSONObject;
import org.unipop.jdbc.schemas.jdbc.JdbcEdgeSchema;
import org.unipop.query.aggregation.LocalQuery;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.predicates.PredicatesHolderFactory;
import org.unipop.query.search.SearchVertexQuery;
import org.unipop.schema.element.VertexSchema;
import org.unipop.schema.property.AbstractPropertyContainer;
import org.unipop.schema.property.PropertySchema;
import org.unipop.structure.UniGraph;

import java.util.*;
import java.util.stream.Collectors;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.groupingSets;

/**
 * Created by sbarzilay on 9/15/16.
 */
public abstract class AbstractJdbcEdgeSchema extends AbstractRowSchema<Edge> implements JdbcEdgeSchema{
    public AbstractJdbcEdgeSchema(JSONObject configuration, UniGraph graph) {
        super(configuration, graph);
    }

    @Override
    public Select getLocal(LocalQuery query, DSLContext dsl) {
        SearchVertexQuery searchQuery = (SearchVertexQuery) query.getSearchQuery();
        PredicatesHolder edgePredicates = this.toPredicates(searchQuery.getPredicates());
        PredicatesHolder vertexPredicates = this.getVertexPredicates(searchQuery.getVertices(), searchQuery.getDirection());
        if (edgePredicates.isAborted() || vertexPredicates.isAborted()) return null;
        PredicatesHolder predicatesHolder = PredicatesHolderFactory.and(edgePredicates, vertexPredicates);
         if (predicatesHolder.isAborted()) return null;
        // TODO: create select
        Set<String> outId = getOutVertexSchema().getPropertySchema(T.id.getAccessor()).toFields(Collections.emptySet());
        Set<String> id = getPropertySchema(T.id.getAccessor()).toFields(Collections.emptySet());
        SelectGroupByStep select = ((SelectGroupByStep) createSelect(searchQuery, predicatesHolder, dsl, (Field) DSL.rank().over(DSL.partitionBy(field(outId.iterator().next())).orderBy(field(id.iterator().next()))).as("r1")));

        Set<String> fields = searchQuery.getPropertyKeys();
        if (fields == null)
            fields = this.getPropertySchemas().stream().map(PropertySchema::getKey).collect(Collectors.toSet());
        Set<String> props = this.toFields(fields);
        int limit = searchQuery.getLimit();
        int finalLimit = limit == -1 ? Integer.MAX_VALUE : limit + 1;
        SelectConditionStep<Record> selectf = dsl.select(props.stream().filter(p -> p!=null).map(DSL::field).collect(Collectors.toList())).from(select).where(field("r1").lt(finalLimit));

        return selectf;
    }

    protected PredicatesHolder getVertexPredicates(List<Vertex> vertices, Direction direction) {
        PredicatesHolder outPredicates = this.getOutVertexSchema().toPredicates(vertices);
        PredicatesHolder inPredicates = this.getInVertexSchema().toPredicates(vertices);
        if(direction.equals(Direction.OUT)) return outPredicates;
        if(direction.equals(Direction.IN)) return inPredicates;
        return PredicatesHolderFactory.or(inPredicates, outPredicates);
    }
}
