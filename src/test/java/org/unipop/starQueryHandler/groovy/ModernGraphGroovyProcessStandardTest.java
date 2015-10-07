package org.unipop.starQueryHandler.groovy;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.groovy.loaders.SugarLoader;
import org.apache.tinkerpop.gremlin.process.GroovyProcessStandardSuite;
import org.unipop.starQueryHandler.ModernGraphGraphProvider;
import org.unipop.structure.UniGraph;
import org.junit.runner.RunWith;

@RunWith(GroovyProcessStandardSuite.class)
@GraphProviderClass(provider = ModernGraphGraphProvider.class, graph = UniGraph.class)
public class ModernGraphGroovyProcessStandardTest {
    static {
        SugarLoader.load();
    }
}
