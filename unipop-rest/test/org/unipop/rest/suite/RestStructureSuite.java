package org.unipop.rest.suite;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.junit.runner.RunWith;
import org.unipop.structure.UniGraph;
import org.unipop.test.UnipopStructureSuite;
import test.RestGraphProvider;

@RunWith(UnipopStructureSuite.class)
@GraphProviderClass(provider = RestGraphProvider.class, graph = UniGraph.class)
public class RestStructureSuite {
}
