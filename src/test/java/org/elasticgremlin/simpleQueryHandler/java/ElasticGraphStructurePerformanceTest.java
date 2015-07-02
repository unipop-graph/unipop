package org.elasticgremlin.simpleQueryHandler.java;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.structure.StructurePerformanceSuite;
import org.elasticgremlin.ElasticGraphGraphProvider;
import org.elasticgremlin.structure.ElasticGraph;
import org.junit.runner.RunWith;

@RunWith(StructurePerformanceSuite.class)
@GraphProviderClass(provider = ElasticGraphGraphProvider.class, graph = ElasticGraph.class)
public class ElasticGraphStructurePerformanceTest {

}