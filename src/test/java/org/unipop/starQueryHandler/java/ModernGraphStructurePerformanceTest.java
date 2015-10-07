package org.unipop.starQueryHandler.java;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.structure.StructurePerformanceSuite;
import org.unipop.starQueryHandler.ModernGraphGraphProvider;
import org.unipop.structure.UniGraph;
import org.junit.runner.RunWith;

@RunWith(StructurePerformanceSuite.class)
@GraphProviderClass(provider = ModernGraphGraphProvider.class, graph = UniGraph.class)
public class ModernGraphStructurePerformanceTest {

}
