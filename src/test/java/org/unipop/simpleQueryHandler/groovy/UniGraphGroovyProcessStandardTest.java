package org.unipop.simpleQueryHandler.groovy;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.unipop.UniGraphGraphProvider;
import org.unipop.structure.UniGraph;
import org.apache.tinkerpop.gremlin.groovy.loaders.SugarLoader;
import org.apache.tinkerpop.gremlin.process.GroovyProcessStandardSuite;
import org.junit.runner.RunWith;

@RunWith(GroovyProcessStandardSuite.class)
@GraphProviderClass(provider = UniGraphGraphProvider.class, graph = UniGraph.class)
public class UniGraphGroovyProcessStandardTest {
    static {
        SugarLoader.load();
    }
}
