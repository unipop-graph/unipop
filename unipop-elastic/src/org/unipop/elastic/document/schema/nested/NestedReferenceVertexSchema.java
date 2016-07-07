package org.unipop.elastic.document.schema.nested;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.json.JSONObject;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.schema.reference.ReferenceVertexSchema;
import org.unipop.structure.UniGraph;

import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by sbarzilay on 7/7/16.
 */
public class NestedReferenceVertexSchema extends ReferenceVertexSchema{
    private String path;
    public NestedReferenceVertexSchema(JSONObject properties, String path, UniGraph graph) {
        super(properties, graph);
        this.path = path;
    }

    @Override
    public Set<String> toFields(Set<String> propertyKeys) {
        return super.toFields(propertyKeys).stream().map(key -> path + "." + key).collect(Collectors.toSet());
    }

    @Override
    public PredicatesHolder toPredicates(PredicatesHolder predicatesHolder) {
        return super.toPredicates(predicatesHolder).map(has -> new HasContainer(path + "." + has.getKey(), has.getPredicate()));
    }
}
