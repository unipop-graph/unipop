package com.tinkerpop.gremlin.elastic.elasticservice;

import com.tinkerpop.gremlin.process.graph.util.HasContainer;
import com.tinkerpop.gremlin.structure.*;
import org.apache.commons.configuration.Configuration;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.FilterBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public interface SchemaProvider {
    void init(Client client, Configuration configuration) throws IOException;

    default void close(){};

    default Result getIndex(Element element){
        ArrayList keyValues = new ArrayList();
        element.iterators().propertyIterator().forEachRemaining(property->{
            keyValues.add(property.key());
            keyValues.add(property.value());
        });
        ElasticService.ElementType type = element.getClass().isAssignableFrom(Vertex.class) ? ElasticService.ElementType.vertex : ElasticService.ElementType.edge;

        return getIndex(element.label(), element.id(), type, keyValues.toArray());
    }

    Result getIndex(String label, Object idValue, ElasticService.ElementType elementType, Object[] keyValues);
    Result getIndex(List<HasContainer> hasContainerList, ElasticService.ElementType elementType);

    String[] getIndicesForClearGraph();

    public class Result {
        private final String index;
        private final String routing;

        public Result(String index, String routing) {
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
}
