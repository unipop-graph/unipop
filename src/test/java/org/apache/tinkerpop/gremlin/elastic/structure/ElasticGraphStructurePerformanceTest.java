package org.apache.tinkerpop.gremlin.elastic.structure;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.elastic.ElasticGraphGraphProvider;
import org.apache.tinkerpop.gremlin.structure.StructurePerformanceSuite;
import org.junit.runner.RunWith;

@RunWith(StructurePerformanceSuite.class)
@GraphProviderClass(provider = ElasticGraphGraphProvider.class, graph = ElasticGraph.class)
public class ElasticGraphStructurePerformanceTest {

}