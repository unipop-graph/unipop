package org.elasticgremlin.starQueryHandler.java;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.structure.StructurePerformanceSuite;
import org.elasticgremlin.starQueryHandler.ModernGraphGraphProvider;
import org.elasticgremlin.structure.ElasticGraph;
import org.junit.runner.RunWith;

@RunWith(StructurePerformanceSuite.class)
@GraphProviderClass(provider = ModernGraphGraphProvider.class, graph = ElasticGraph.class)
public class ModernGraphStructurePerformanceTest {

}
