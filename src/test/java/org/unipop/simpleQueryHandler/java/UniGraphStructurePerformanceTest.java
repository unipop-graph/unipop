package org.unipop.simpleQueryHandler.java;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.structure.StructurePerformanceSuite;
import org.unipop.UniGraphGraphProvider;
import org.unipop.structure.UniGraph;
import org.junit.runner.RunWith;

@RunWith(StructurePerformanceSuite.class)
@GraphProviderClass(provider = UniGraphGraphProvider.class, graph = UniGraph.class)
public class UniGraphStructurePerformanceTest {

}