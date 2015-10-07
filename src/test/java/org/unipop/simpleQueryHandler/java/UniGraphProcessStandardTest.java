package org.unipop.simpleQueryHandler.java;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.unipop.UniGraphGraphProvider;
import org.unipop.structure.UniGraph;
import org.apache.tinkerpop.gremlin.process.ProcessStandardSuite;
import org.junit.runner.RunWith;

@RunWith(ProcessStandardSuite.class)
@GraphProviderClass(provider = UniGraphGraphProvider.class, graph = UniGraph.class)
public class UniGraphProcessStandardTest {
}