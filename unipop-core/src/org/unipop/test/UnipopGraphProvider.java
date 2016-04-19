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
        add(UniEdge.class);
        add(UniElement.class);
        add(UniGraph.class);
        add(UniProperty.class);
        add(UniVertex.class);
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


    /**
     * Defines the test suite that the implementer has decided to support and represents publicly as "passing".
     * Marking the {@link Graph} instance with this class allows that particular test suite to run.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    @Inherited
    public @interface OptIn {

        public static String UnipopStructureSuite = "org.unipop.test.UnipopStructureSuite";
        public static String UnipopProcessSuite = "org.unipop.test.UnipopProcessSuite";

        /**
         * The test suite class to opt in to.
         */
        public String value();
    }
}
