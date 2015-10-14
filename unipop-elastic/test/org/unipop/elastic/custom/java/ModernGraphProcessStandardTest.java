package org.unipop.elastic.custom.java;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.process.ProcessStandardSuite;
import org.unipop.elastic.custom.CustomGraphProvider;
import org.unipop.structure.UniGraph;
import org.junit.runner.RunWith;

@RunWith(ProcessStandardSuite.class)
@GraphProviderClass(provider = CustomGraphProvider.class, graph = UniGraph.class)
public class ModernGraphProcessStandardTest {
}
