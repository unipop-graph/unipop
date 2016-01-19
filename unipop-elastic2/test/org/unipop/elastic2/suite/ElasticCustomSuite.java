package org.unipop.elastic2.suite;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.junit.runner.RunWith;
import org.unipop.elastic2.ElasticGraphProvider;
import org.unipop.elastic2.schema.misc.CustomTestSuite;
import org.unipop.structure.UniGraph;

/**
 * Created by Roman on 11/11/2015.
 */
@RunWith(CustomTestSuite.class)
@GraphProviderClass(provider = ElasticGraphProvider.class, graph = UniGraph.class)
public class ElasticCustomSuite {
}