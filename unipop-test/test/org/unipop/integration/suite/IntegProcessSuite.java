package org.unipop.integration.suite;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.junit.runner.RunWith;
import org.unipop.integration.IntegGraphProvider;
import org.unipop.structure.UniGraph;
import org.unipop.test.UnipopProcessSuite;

@RunWith(ExtendedProcessSuite.class)
@GraphProviderClass(provider = IntegGraphProvider.class, graph = UniGraph.class)
public class IntegProcessSuite {
}