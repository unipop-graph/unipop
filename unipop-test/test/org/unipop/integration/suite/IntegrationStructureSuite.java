package org.unipop.integration.suite;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.structure.StructureStandardSuite;
import org.junit.runner.RunWith;
import org.unipop.integration.IntegrationGraphProvider;
import org.unipop.structure.UniGraph;

@RunWith(StructureStandardSuite.class)
@GraphProviderClass(provider = IntegrationGraphProvider.class, graph = UniGraph.class)
public class IntegrationStructureSuite {
}
