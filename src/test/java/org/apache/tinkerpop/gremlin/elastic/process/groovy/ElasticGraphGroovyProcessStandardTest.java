package org.apache.tinkerpop.gremlin.elastic.process.groovy;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.elastic.ElasticGraphGraphProvider;
import org.apache.tinkerpop.gremlin.elastic.structure.ElasticGraph;
import org.apache.tinkerpop.gremlin.groovy.loaders.SugarLoader;
import org.apache.tinkerpop.gremlin.process.GroovyProcessStandardSuite;
import org.junit.runner.RunWith;

@RunWith(GroovyProcessStandardSuite.class)
@GraphProviderClass(provider = ElasticGraphGraphProvider.class, graph = ElasticGraph.class)
public class ElasticGraphGroovyProcessStandardTest {
    static {
        SugarLoader.load();
    }
}
