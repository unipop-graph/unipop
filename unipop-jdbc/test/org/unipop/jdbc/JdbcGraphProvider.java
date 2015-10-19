package org.unipop.jdbc;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.unipop.structure.*;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class JdbcGraphProvider extends AbstractGraphProvider {



    private static final Set<Class> IMPLEMENTATION = new HashSet<Class>() {{
        add(BaseEdge.class);
        add(BaseElement.class);
        add(UniGraph.class);
        add(BaseProperty.class);
        add(BaseVertex.class);
        add(BaseVertexProperty.class);
    }};


    @Override
    public Map<String, Object> getBaseConfiguration(String graphName, Class<?> test, String testMethodName, LoadGraphWith.GraphData loadGraphWith) {
        return null;
    }

    @Override
    public void clear(Graph graph, Configuration configuration) throws Exception {

    }

    @Override
    public Set<Class> getImplementations() {
        return null;
    }
}
