package org.unipop.jdbc.helpers;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.T;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.SelectJoinStep;
import org.unipop.helpers.LazzyGetter;
import org.unipop.jdbc.utils.JooqHelper;
import org.unipop.structure.BaseVertex;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by sbarzilay on 28/01/16.
 */
public class SqlLazyGetter implements LazzyGetter {

    private static final int MAX_LAZY_GET = 50;
    private DSLContext context;
    private boolean executed = false;
    private Map<GetKey, List<BaseVertex>> keyToVertices;

    public SqlLazyGetter(DSLContext context){
        this.context = context;
        keyToVertices = new HashMap<>();
    }

    @Override
    public Boolean canRegister() {
        return !executed && keyToVertices.keySet().size() < MAX_LAZY_GET;
    }

    @Override
    public void register(BaseVertex v, String label, String indexName) {
        if(executed) System.out.println("This SqlLazyGetter has already been executed.");

        GetKey key = new GetKey(v.id(), label, indexName);

        List<BaseVertex> vertices = keyToVertices.get(key);

        if (vertices == null) {
            vertices = new ArrayList();
            keyToVertices.put(key, vertices);
        }
        vertices.add(v);
    }

    @Override
    public void execute() {
        if (executed) return;
        executed = true;
        Set<String> tables = keyToVertices.keySet().stream().map(getKey -> getKey.tableName).collect(Collectors.toSet());
        tables.forEach(table -> {
            Set<Object> ids = new HashSet<>();
            Set<String> labels = new HashSet<>();
            keyToVertices.entrySet().stream().filter(getKeyListEntry -> getKeyListEntry.getKey().tableName.equals(table))
                    .map(Map.Entry::getKey).forEach(getKey -> {
                ids.add(getKey.id);
                labels.add(getKey.label);
            });
            SelectJoinStep<Record> select = context.select().from(table);
            List<HasContainer> hasContainers = new ArrayList<>();
            hasContainers.add(new HasContainer(T.id.getAccessor(), P.within(ids)));
//            hasContainers.add(new HasContainer(T.label.getAccessor(), P.within(labels)));
            hasContainers.forEach(has-> select.where(JooqHelper.createCondition(has)));
            select.fetch().forEach(record -> {
                Map<String, Object> stringObjectMap = new HashMap<>();
                record.intoMap().forEach((key, value) -> stringObjectMap.put(key.toLowerCase(), value));
                GetKey getKey = new GetKey(stringObjectMap.get("id"), table.toLowerCase(), table);
                List<BaseVertex> baseVertices = keyToVertices.get(getKey);
                keyToVertices.get(getKey).forEach(baseVertex -> baseVertex.applyLazyFields(table, stringObjectMap));
            });
        });
        keyToVertices = null;
        context = null;
    }

    private class GetKey {
        private final String id;
        private final String label;
        private final String tableName;

        public GetKey(Object id, String label, String tableName) {

            this.id = id.toString();
            this.label = label;
            this.tableName = tableName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            GetKey getKey = (GetKey) o;

            if (!id.equals(getKey.id)) return false;
            if (label != null && getKey.label != null && !label.equals(getKey.label)) return false;
            return true;
        }

        @Override
        public int hashCode() {
            int result = id.hashCode();
//            result = 31 * result + tableName.hashCode();
            return result;
        }
    }
}
