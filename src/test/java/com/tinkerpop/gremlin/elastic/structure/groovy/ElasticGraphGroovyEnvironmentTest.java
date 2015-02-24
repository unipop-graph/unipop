package com.tinkerpop.gremlin.elastic.structure.groovy;

import com.tinkerpop.gremlin.elastic.ElasticGraphGraphProvider;
import com.tinkerpop.gremlin.elastic.structure.ElasticGraph;
import com.tinkerpop.gremlin.groovy.GroovyEnvironmentSuite;
import com.tinkerpop.gremlin.groovy.loaders.SugarLoader;
import com.tinkerpop.gremlin.process.ProcessStandardSuite;
import org.junit.runner.RunWith;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(GroovyEnvironmentSuite.class)
@ProcessStandardSuite.GraphProviderClass(provider = ElasticGraphGraphProvider.class, graph = ElasticGraph.class)
public class ElasticGraphGroovyEnvironmentTest {
    static {
        SugarLoader.load();
    }
}