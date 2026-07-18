package org.unipop.schema.catalog;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.unipop.schema.element.ElementSchema;

import java.util.Objects;

/**
 * One resolved hop in a {@link PathPlan}: edge schema + direction + target vertex schema.
 */
public final class PathHop {
    private final ElementSchema edgeSchema;
    private final Direction direction;
    private final ElementSchema targetVertexSchema;

    public PathHop(ElementSchema edgeSchema, Direction direction, ElementSchema targetVertexSchema) {
        this.edgeSchema = edgeSchema;
        this.direction = direction;
        this.targetVertexSchema = targetVertexSchema;
    }

    public ElementSchema getEdgeSchema() {
        return edgeSchema;
    }

    public Direction getDirection() {
        return direction;
    }

    public ElementSchema getTargetVertexSchema() {
        return targetVertexSchema;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PathHop)) return false;
        PathHop pathHop = (PathHop) o;
        return Objects.equals(edgeSchema, pathHop.edgeSchema)
                && direction == pathHop.direction
                && Objects.equals(targetVertexSchema, pathHop.targetVertexSchema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(edgeSchema, direction, targetVertexSchema);
    }

    @Override
    public String toString() {
        return "PathHop{" + direction + ", edge=" + edgeSchema + ", target=" + targetVertexSchema + '}';
    }
}
