package org.unipop.rest.suite;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.junit.runner.RunWith;
import org.unipop.structure.UniGraph;
import org.unipop.test.UnipopProcessSuite;
import test.ElasticGraphProvider;
import test.RestGraphProvider;

@RunWith(UnipopProcessSuite.class)
@GraphProviderClass(provider = RestGraphProvider.class, graph = UniGraph.class)
public class RestProcessSuite {
}