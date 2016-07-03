package org.unipop.jdbc.suite;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.junit.runner.RunWith;
import org.unipop.jdbc.JdbcGraphProvider;
import org.unipop.structure.UniGraph;
import org.unipop.test.UnipopProcessSuite;

/**
 * @author GurRo
 * @since 6/20/2016
 */
@RunWith(UnipopProcessSuite.class)
@GraphProviderClass(provider = JdbcGraphProvider.class, graph = UniGraph.class)
public class JdbcProcessSuite {
}
