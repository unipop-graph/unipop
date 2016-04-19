package org.unipop.elastic.suite;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.structure.StructureStandardSuite;
import org.junit.runner.RunWith;
import org.unipop.elastic.ElasticGraphProvider;
import org.unipop.test.UnipopGraphProvider;
import org.unipop.structure.UniGraph;
import org.unipop.test.UnipopStructureSuite;

@RunWith(UnipopStructureSuite.class)
@GraphProviderClass(provider = ElasticGraphProvider.class, graph = UniGraph.class)
public class ElasticStructureSuite {
}
