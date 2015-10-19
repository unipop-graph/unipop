package org.unipop.elastic.basic.java;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.structure.StructurePerformanceSuite;
import org.junit.runner.RunWith;
import org.unipop.elastic.basic.BasicGraphProvider;
import org.unipop.structure.UniGraph;

@RunWith(StructurePerformanceSuite.class)
@GraphProviderClass(provider = BasicGraphProvider.class, graph = UniGraph.class)
public class UniGraphStructurePerformanceTest {

}