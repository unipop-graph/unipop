package org.unipop.elastic.basic.java;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.unipop.elastic.basic.BasicGraphProvider;
import org.unipop.structure.UniGraph;
import org.apache.tinkerpop.gremlin.process.ProcessStandardSuite;
import org.junit.runner.RunWith;

@RunWith(ProcessStandardSuite.class)
@GraphProviderClass(provider = BasicGraphProvider.class, graph = UniGraph.class)
public class UniGraphProcessStandardTest {
}