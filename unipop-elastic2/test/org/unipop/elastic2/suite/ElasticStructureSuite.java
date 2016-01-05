package org.unipop.elastic2.suite;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.structure.StructureStandardSuite;
import org.junit.runner.RunWith;
import org.unipop.elastic2.ElasticGraphProvider;
import org.unipop.structure.UniGraph;

@RunWith(StructureStandardSuite.class)
@GraphProviderClass(provider = ElasticGraphProvider.class, graph = UniGraph.class)
public class ElasticStructureSuite {
}
