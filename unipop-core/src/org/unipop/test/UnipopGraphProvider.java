package org.unipop.test;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.unipop.structure.*;

import java.lang.annotation.*;
import java.util.*;

public abstract class UnipopGraphProvider extends AbstractGraphProvider {

    private static final Set<Class> IMPLEMENTATION = new HashSet<Class>() {{
        add(UniGraph.class);
        add(UniElement.class);
        add(UniVertex.class);
        add(UniEdge.class);
        add(UniProperty.class);
        add(UniVertexProperty.class);
    }};

    @Override
    public Map<String, Object> getBaseConfiguration(String graphName, Class<?> test, String testMethodName, LoadGraphWith.GraphData loadGraphWith) {
        return new HashMap<String, Object>() {{
            put(Graph.GRAPH, UniGraph.class.getName());
            put("graphName",graphName.toLowerCase());

        }};
    }

    @Override
    public void clear(final Graph g, final Configuration configuration) throws Exception {
        if (g != null) g.close();
    }

    @Override
    public Set<Class> getImplementations() {
        return IMPLEMENTATION;
    }

    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    @Inherited
    public @interface OptIn {

        String UnipopStructureSuite = "org.unipop.test.UnipopStructureSuite";
        String UnipopProcessSuite = "org.unipop.test.UnipopProcessSuite";

        String value();
    }
}
