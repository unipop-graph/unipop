package org.elasticgremlin.starQueryHandler.java;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.groovy.GroovyEnvironmentSuite;
import org.apache.tinkerpop.gremlin.groovy.loaders.SugarLoader;
import org.elasticgremlin.starQueryHandler.ModernStarGraphGraphProvider;
import org.elasticgremlin.structure.ElasticGraph;
import org.junit.runner.RunWith;

@RunWith(GroovyEnvironmentSuite.class)
@GraphProviderClass(provider = ModernStarGraphGraphProvider.class, graph = ElasticGraph.class)
public class ModernStarGraphGroovyEnvironmentTest {
    static {
        SugarLoader.load();
    }
}
