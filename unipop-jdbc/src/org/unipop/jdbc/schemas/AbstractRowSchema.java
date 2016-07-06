package org.unipop.jdbc.schemas;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.json.JSONObject;
import org.unipop.jdbc.schemas.jdbc.JdbcSchema;
import org.unipop.query.predicates.PredicateQuery;
import org.unipop.schema.element.AbstractElementSchema;
import org.unipop.structure.UniGraph;

import java.util.List;
import java.util.Map;

/**
 * @author Gur Ronen
 * @since 3/7/2016
 */
public abstract class AbstractRowSchema<E extends Element> extends AbstractElementSchema<E> implements JdbcSchema<E> {
    private final String table;

    public AbstractRowSchema(JSONObject configuration, UniGraph graph) {
        super(configuration, graph);
        this.table = configuration.optString("table");
    }

    @Override
    public String getTable() {
        return this.table;
    }

    @Override
    public Object getId(E element) {
        return element.id();
    }

    @Override
    public List<E> parseResults(String result, PredicateQuery query) {
        return null;
    }

    @Override
    public String toString() {
        return "AbstractRowSchema{" +
                "table='" + table + '\'' +
                "} " + super.toString();
    }
}
