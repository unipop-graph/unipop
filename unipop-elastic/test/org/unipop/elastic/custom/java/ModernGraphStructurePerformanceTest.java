package org.unipop.elastic.custom.java;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.structure.StructurePerformanceSuite;
import org.unipop.elastic.custom.CustomGraphProvider;
import org.junit.runner.RunWith;
import org.unipop.structure.UniGraph;

@RunWith(StructurePerformanceSuite.class)
@GraphProviderClass(provider = CustomGraphProvider.class, graph = UniGraph.class)
public class ModernGraphStructurePerformanceTest {

}
