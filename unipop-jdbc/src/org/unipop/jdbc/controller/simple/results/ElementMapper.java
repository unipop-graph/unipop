package org.unipop.jdbc.controller.simple.results;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.jooq.Record;
import org.jooq.RecordMapper;
import org.unipop.jdbc.schemas.jdbc.JdbcSchema;

import java.util.Map;
import java.util.Objects;

/**
 * @author Gur Ronen
 * @since 6/20/2016
 */
@Deprecated
public class ElementMapper<E extends Element> implements RecordMapper<Record, Element> {
    private final JdbcSchema<E> rowSchema;

    public ElementMapper(JdbcSchema<E> rowSchema) {
        this.rowSchema = rowSchema;
    }

    @Override
    public Element map(Record record) {
        Map<String, Object> dataMap = record.intoMap();
        return rowSchema.fromFields(dataMap).stream().findFirst().get();
    }
}
