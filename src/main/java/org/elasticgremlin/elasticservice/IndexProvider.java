package org.elasticgremlin.elasticservice;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.*;
import org.elasticsearch.client.Client;

import java.io.IOException;
import java.util.*;


public interface IndexProvider {
    void init(Client client, Configuration configuration) throws IOException;

    default void close(){};

    IndexResult getIndex(Element element);
    IndexResult getIndex(String label, Object idValue, ElasticService.ElementType elementType);
    MultiIndexResult getIndex(List<HasContainer> hasContainerList, ElasticService.ElementType elementType);

    String[] getIndicesForClearGraph();

    public class IndexResult {
        private final String index;
        private final String routing;

        public IndexResult(String index, String routing) {
            this.index = index;
            this.routing = routing;
        }

        public String getIndex() {
            return index;
        }

        public String getRouting() {
            return routing;
        }
    }

    public class MultiIndexResult {
        private final String[] index;
        private final String routing;

        public MultiIndexResult(String[] index, String routing) {
            this.index = index;
            this.routing = routing;
        }

        public String[] getIndex() {
            return index;
        }

        public String getRouting() {
            return routing;
        }
    }
}
