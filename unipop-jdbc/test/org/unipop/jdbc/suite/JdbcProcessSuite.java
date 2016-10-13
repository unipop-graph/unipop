package org.unipop.jdbc.suite;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.junit.runner.RunWith;
import org.unipop.jdbc.JdbcSourceProvider;
import test.JdbcGraphProvider;
import org.unipop.structure.UniGraph;
import org.unipop.test.UnipopProcessSuite;
import test.JdbcOptimizedGraphProvider;

/**
 * @author GurRo
 * @since 6/20/2016
 */
@RunWith(UnipopProcessSuite.class)
@GraphProviderClass(provider = JdbcOptimizedGraphProvider.class, graph = UniGraph.class)
public class JdbcProcessSuite {
}
