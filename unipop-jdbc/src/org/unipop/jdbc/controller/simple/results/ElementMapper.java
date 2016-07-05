package org.unipop.jdbc.controller.simple.results;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.jooq.Record;
import org.jooq.RecordMapper;
import org.unipop.jdbc.schemas.jdbc.JdbcSchema;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * @author GurRo
 * @since 6/20/2016
 */
public class ElementMapper<E extends Element> implements RecordMapper<Record, Element> {
    private final Set<JdbcSchema<E>> rowSchemas;

    public ElementMapper(Set<JdbcSchema<E>> rowSchemas) {
        this.rowSchemas = rowSchemas;
    }

    @Override
    public Element map(Record record) {
        Map<String, Object> dataMap = record.intoMap();
        return rowSchemas.stream().flatMap(schema -> schema.fromFields(dataMap).stream().filter(Objects::nonNull)).findFirst().get();
    }
}
