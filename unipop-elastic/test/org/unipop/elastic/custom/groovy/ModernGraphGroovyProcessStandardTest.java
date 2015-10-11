package org.unipop.elastic.custom.groovy;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.groovy.loaders.SugarLoader;
import org.apache.tinkerpop.gremlin.process.GroovyProcessStandardSuite;
import org.unipop.elastic.custom.CustomGraphProvider;
import org.unipop.structure.UniGraph;
import org.junit.runner.RunWith;

@RunWith(GroovyProcessStandardSuite.class)
@GraphProviderClass(provider = CustomGraphProvider.class, graph = UniGraph.class)
public class ModernGraphGroovyProcessStandardTest {
    static {
        SugarLoader.load();
    }
}
