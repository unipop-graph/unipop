package org.apache.tinkerpop.gremlin.elastic.structure.groovy;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.elastic.ElasticGraphGraphProvider;
import org.apache.tinkerpop.gremlin.elastic.structure.ElasticGraph;
import org.apache.tinkerpop.gremlin.groovy.GroovyEnvironmentIntegrateSuite;
import org.apache.tinkerpop.gremlin.groovy.loaders.SugarLoader;
import org.junit.runner.RunWith;

@RunWith(GroovyEnvironmentIntegrateSuite.class)
@GraphProviderClass(provider = ElasticGraphGraphProvider.class, graph = ElasticGraph.class)
public class ElasticGraphGroovyEnvironmentIntegrateTest {
    static {
        SugarLoader.load();
    }
}