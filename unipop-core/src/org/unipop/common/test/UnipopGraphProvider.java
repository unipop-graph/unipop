package org.unipop.common.test;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.unipop.structure.*;

import java.util.*;

public class UnipopGraphProvider extends AbstractGraphProvider {

    private static final Set<Class> IMPLEMENTATION = new HashSet<Class>() {{
        add(UniEdge.class);
        add(UniElement.class);
        add(UniGraph.class);
        add(UniProperty.class);
        add(UniVertex.class);
        add(UniVertexProperty.class);
    }};

    private List<TestInitializer> initializers;

    public UnipopGraphProvider(List<TestInitializer> initializers) throws Exception {
        this.initializers = initializers;
    }

    @Override
    public Map<String, Object> getBaseConfiguration(String graphName, Class<?> test, String testMethodName, LoadGraphWith.GraphData loadGraphWith) {
        return new HashMap<String, Object>() {{
            put(Graph.GRAPH, UniGraph.class.getName());
            put("graphName",graphName.toLowerCase());
        }};
    }

    @Override
    public void clear(final Graph g, final Configuration configuration) throws Exception {
        initializers.forEach(initializer -> initializer.clear(g, configuration));
        if (g != null) g.close();
    }

    @Override
    public Set<Class> getImplementations() {
        return IMPLEMENTATION;
    }

    @Override
    public Object convertId(Object id, Class<? extends Element> c) {
        return id.toString();
    }
}
