package org.unipop.starQueryHandler.java;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.groovy.GroovyEnvironmentSuite;
import org.apache.tinkerpop.gremlin.groovy.loaders.SugarLoader;
import org.unipop.starQueryHandler.ModernGraphGraphProvider;
import org.unipop.structure.UniGraph;
import org.junit.runner.RunWith;

@RunWith(GroovyEnvironmentSuite.class)
@GraphProviderClass(provider = ModernGraphGraphProvider.class, graph = UniGraph.class)
public class ModernGraphGroovyEnvironmentTest {
    static {
        SugarLoader.load();
    }
}
