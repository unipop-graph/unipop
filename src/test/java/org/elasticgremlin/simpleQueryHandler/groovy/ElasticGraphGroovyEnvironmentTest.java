package org.elasticgremlin.simpleQueryHandler.groovy;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.elasticgremlin.ElasticGraphGraphProvider;
import org.elasticgremlin.structure.ElasticGraph;
import org.apache.tinkerpop.gremlin.groovy.GroovyEnvironmentSuite;
import org.apache.tinkerpop.gremlin.groovy.loaders.SugarLoader;
import org.junit.runner.RunWith;

@RunWith(GroovyEnvironmentSuite.class)
@GraphProviderClass(provider = ElasticGraphGraphProvider.class, graph = ElasticGraph.class)
public class ElasticGraphGroovyEnvironmentTest {
    static {
        SugarLoader.load();
    }
}