package org.elasticgremlin.structure;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.structure.StructurePerformanceSuite;
import org.elasticgremlin.ModernStarGraphGraphProvider;
import org.elasticgremlin.testimpl.ModernStarGraph;
import org.junit.runner.RunWith;

@RunWith(StructurePerformanceSuite.class)
@GraphProviderClass(provider = ModernStarGraphGraphProvider.class, graph = ModernStarGraph.class)
public class ModernStarGraphStructurePerformanceTest {

}
