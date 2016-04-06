package org.unipop.elastic.suite;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.structure.StructureStandardSuite;
import org.junit.runner.RunWith;
import org.unipop.common.test.UnipopGraphProvider;
import org.unipop.structure.UniGraph;

@RunWith(StructureStandardSuite.class)
@GraphProviderClass(provider = UnipopGraphProvider.class, graph = UniGraph.class)
public class ElasticStructureSuite {
}
