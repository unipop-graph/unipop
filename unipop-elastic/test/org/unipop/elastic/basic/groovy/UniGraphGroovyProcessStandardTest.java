package org.unipop.elastic.basic.groovy;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.unipop.elastic.basic.BasicGraphProvider;
import org.unipop.structure.UniGraph;
import org.apache.tinkerpop.gremlin.groovy.loaders.SugarLoader;
import org.apache.tinkerpop.gremlin.process.GroovyProcessStandardSuite;
import org.junit.runner.RunWith;

@RunWith(GroovyProcessStandardSuite.class)
@GraphProviderClass(provider = BasicGraphProvider.class, graph = UniGraph.class)
public class UniGraphGroovyProcessStandardTest {
    static {
        SugarLoader.load();
    }
}
