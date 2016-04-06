package org.unipop.elastic.suite;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.junit.runner.RunWith;
import org.unipop.common.test.UnipopGraphProvider;
import org.unipop.elastic.tests.CustomTestSuite;
import org.unipop.structure.UniGraph;

@RunWith(CustomTestSuite.class)
@GraphProviderClass(provider = UnipopGraphProvider.class, graph = UniGraph.class)
public class ElasticCustomSuite {
}