package org.apache.tinkerpop.gremlin.elastic.structure.groovy;

import org.apache.tinkerpop.gremlin.elastic.ElasticGraphGraphProvider;
import org.apache.tinkerpop.gremlin.elastic.structure.ElasticGraph;
import org.apache.tinkerpop.gremlin.groovy.GroovyEnvironmentSuite;
import org.apache.tinkerpop.gremlin.groovy.loaders.SugarLoader;
import org.apache.tinkerpop.gremlin.process.ProcessStandardSuite;
import org.junit.runner.RunWith;

@RunWith(GroovyEnvironmentSuite.class)
@ProcessStandardSuite.GraphProviderClass(provider = ElasticGraphGraphProvider.class, graph = ElasticGraph.class)
public class ElasticGraphGroovyEnvironmentTest {
    static {
        SugarLoader.load();
    }
}