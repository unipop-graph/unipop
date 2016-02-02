package org.unipop.elastic.controller.template.helpers;

import org.elasticsearch.client.Client;
import org.unipop.elastic.helpers.TimingAccessor;
import org.unipop.helpers.LazzyGetter;
import org.unipop.structure.BaseVertex;

import java.util.HashMap;
import java.util.List;

/**
 * Created by sbarzilay on 02/02/16.
 */
public class TemplateLazyGetter implements LazzyGetter {
    private static final int MAX_LAZY_GET = 50;
    private Client client;
    private TimingAccessor timing;
    private boolean executed = false;
    private HashMap<GetKey, List<BaseVertex>> keyToVertices = new HashMap();

    @Override
    public Boolean canRegister() {
        return null;
    }

    @Override
    public void register(BaseVertex v, String label, String indexName) {

    }

    @Override
    public void execute() {

    }

    private class GetKey {
        private final String id;
        private final String type;
        private final String indexName;

        public GetKey(Object id, String type, String indexName) {

            this.id = id.toString();
            this.type = type;
            this.indexName = indexName;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            GetKey getKey = (GetKey) o;

            if (!id.equals(getKey.id)) return false;
            if (type != null && getKey.type != null && !type.equals(getKey.type)) return false;
            return true;
        }

        @Override
        public int hashCode() {
            int result = id.hashCode();
//            result = 31 * result + indexName.hashCode();
            return result;
        }
    }
}
