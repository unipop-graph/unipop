package org.unipop.elastic.suite;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.process.ProcessStandardSuite;
import org.junit.runner.RunWith;
import org.unipop.common.test.UnipopGraphProvider;
import org.unipop.structure.UniGraph;

@RunWith(ProcessStandardSuite.class)
@GraphProviderClass(provider = UnipopGraphProvider.class, graph = UniGraph.class)
public class ElasticProcessSuite {
}