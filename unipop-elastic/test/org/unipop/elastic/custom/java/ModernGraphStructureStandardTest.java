package org.unipop.elastic.custom.java;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.structure.StructureStandardSuite;
import org.unipop.elastic.custom.CustomGraphProvider;
import org.junit.runner.RunWith;
import org.unipop.structure.UniGraph;

@RunWith(StructureStandardSuite.class)
@GraphProviderClass(provider = CustomGraphProvider.class, graph = UniGraph.class)
public class ModernGraphStructureStandardTest {

}
