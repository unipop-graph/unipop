package org.unipop.elastic.basic.groovy;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.unipop.elastic.basic.BasicGraphProvider;
import org.unipop.structure.UniGraph;
import org.apache.tinkerpop.gremlin.groovy.GroovyEnvironmentSuite;
import org.apache.tinkerpop.gremlin.groovy.loaders.SugarLoader;
import org.junit.runner.RunWith;

@RunWith(GroovyEnvironmentSuite.class)
@GraphProviderClass(provider = BasicGraphProvider.class, graph = UniGraph.class)
public class UniGraphGroovyEnvironmentTest {
    static {
        SugarLoader.load();
    }
}