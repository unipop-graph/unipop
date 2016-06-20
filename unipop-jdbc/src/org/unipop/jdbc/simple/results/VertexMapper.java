package org.unipop.jdbc.simple.results;

import org.apache.commons.lang.NotImplementedException;
import org.apache.tinkerpop.gremlin.structure.T;
import org.jooq.Record;
import org.jooq.RecordMapper;
import org.unipop.jdbc.utils.TableStrings;
import org.unipop.structure.UniGraph;
import org.unipop.structure.UniVertex;

import java.util.Map;

/**
 * @author GurRo
 * @since 6/20/2016
 */
public class VertexMapper implements RecordMapper<Record, UniVertex> {

    private final UniGraph graph;
    private final String idKey;

    private final String tableName;

    public VertexMapper(UniGraph graph) {
        this(graph, TableStrings.TABLE_COLUMN_NAME, TableStrings.DEFAULT_ID_KEY);
    }

    public VertexMapper(UniGraph graph, String tableName, String idKey) {
        this.graph = graph;
        this.tableName = tableName;

        this.idKey = idKey;
    }

    @Override
    public UniVertex map(Record record) {
        Map<String, Object> dataMap = record.intoMap();

        dataMap.put(T.id.getAccessor(), dataMap.remove(this.idKey));
        dataMap.put(T.label.getAccessor(), dataMap.remove(this.tableName));

        UniVertex vertex = new UniVertex(dataMap, this.graph);
        return vertex;
    }
}
