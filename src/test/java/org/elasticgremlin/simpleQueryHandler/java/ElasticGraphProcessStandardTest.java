package org.elasticgremlin.simpleQueryHandler.java;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.elasticgremlin.ElasticGraphGraphProvider;
import org.elasticgremlin.structure.ElasticGraph;
import org.apache.tinkerpop.gremlin.process.ProcessStandardSuite;
import org.junit.runner.RunWith;

@RunWith(ProcessStandardSuite.class)
@GraphProviderClass(provider = ElasticGraphGraphProvider.class, graph = ElasticGraph.class)
public class ElasticGraphProcessStandardTest {
}