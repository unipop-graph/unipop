package org.elasticgremlin.starQueryHandler.groovy;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.groovy.loaders.SugarLoader;
import org.apache.tinkerpop.gremlin.process.GroovyProcessStandardSuite;
import org.elasticgremlin.starQueryHandler.ModernGraphGraphProvider;
import org.elasticgremlin.structure.ElasticGraph;
import org.junit.runner.RunWith;

@RunWith(GroovyProcessStandardSuite.class)
@GraphProviderClass(provider = ModernGraphGraphProvider.class, graph = ElasticGraph.class)
public class ModernGraphGroovyProcessStandardTest {
    static {
        SugarLoader.load();
    }
}
