package org.unipop.integration.suite;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.junit.runner.RunWith;
import org.unipop.integration.IntegrationGraphProvider;
import org.unipop.structure.UniGraph;
import org.unipop.test.UnipopStructureSuite;

@RunWith(UnipopStructureSuite.class)
@GraphProviderClass(provider = IntegrationGraphProvider.class, graph = UniGraph.class)
public class IntegStructureSuite {
}
