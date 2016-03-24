package org.unipop.elastic.suite;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.junit.runner.RunWith;
import org.unipop.elastic.ElasticGraphProvider;
import org.unipop.elastic.schema.ElementSchema.misc.CustomTestSuite;
import org.unipop.structure.UniGraph;

/**
 * Created by Roman on 11/11/2015.
 */
@RunWith(CustomTestSuite.class)
@GraphProviderClass(provider = ElasticGraphProvider.class, graph = UniGraph.class)
public class ElasticCustomSuite {
}