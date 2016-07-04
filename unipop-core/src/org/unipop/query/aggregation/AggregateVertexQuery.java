package org.unipop.query.aggregation;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.unipop.process.group.traversal.SemanticKeyTraversal;
import org.unipop.process.group.traversal.SemanticReducerTraversal;
import org.unipop.process.group.traversal.SemanticValuesTraversal;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.StepDescriptor;
import org.unipop.query.controller.UniQueryController;
import org.unipop.query.VertexQuery;

import java.util.List;

public class AggregateVertexQuery extends AggregateQuery implements VertexQuery {

    private final List<Vertex> vertices;
    private final Direction direction;

    public AggregateVertexQuery(List<Vertex> vertices,
                                Direction direction,
                                PredicatesHolder predicates,
                                SemanticKeyTraversal key,
                                SemanticValuesTraversal values,
                                SemanticReducerTraversal reduce,
                                StepDescriptor stepDescriptor) {
        super(predicates, key, values, reduce, stepDescriptor);
        this.vertices = vertices;
        this.direction = direction;
    }

    @Override
    public List<Vertex> getVertices() {
        return vertices;
    }

    @Override
    public Direction getDirection() {
        return direction;
    }

    public interface AggregateVertexController extends UniQueryController {
        void query(AggregateVertexQuery uniQuery);
    }

    @Override
    public String toString() {
        return "AggregateVertexQuery{" +
                "vertices=" + vertices +
                ", direction=" + direction +
                '}';
    }
}
