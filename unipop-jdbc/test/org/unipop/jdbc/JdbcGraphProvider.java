package org.unipop.jdbc;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.NotImplementedException;
import org.apache.tinkerpop.gremlin.AbstractGraphProvider;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.Map;
import java.util.Set;

/**
 * @author GurRo
 * @since 6/20/2016
 */
public class JdbcGraphProvider extends AbstractGraphProvider {
    @Override
    public Map<String, Object> getBaseConfiguration(String graphName, Class<?> test, String testMethodName, LoadGraphWith.GraphData loadGraphWith) {
        throw new NotImplementedException();
    }

    @Override
    public void clear(Graph graph, Configuration configuration) throws Exception {
        throw new NotImplementedException();
    }

    @Override
    public Set<Class> getImplementations() {
        throw new NotImplementedException();
    }
}
