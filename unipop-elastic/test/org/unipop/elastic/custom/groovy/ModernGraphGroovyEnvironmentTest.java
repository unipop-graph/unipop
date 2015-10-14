package org.unipop.elastic.custom.groovy;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.groovy.GroovyEnvironmentSuite;
import org.apache.tinkerpop.gremlin.groovy.loaders.SugarLoader;
import org.unipop.elastic.custom.CustomGraphProvider;
import org.junit.runner.RunWith;
import org.unipop.structure.UniGraph;

@RunWith(GroovyEnvironmentSuite.class)
@GraphProviderClass(provider = CustomGraphProvider.class, graph = UniGraph.class)
public class ModernGraphGroovyEnvironmentTest {
    static {
        SugarLoader.load();
    }
}
