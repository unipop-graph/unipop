package org.unipop.jdbc.suite;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.junit.runner.RunWith;
import org.unipop.common.test.UnipopStructureSuite;
import org.unipop.jdbc.JdbcGraphProvider;
import org.unipop.structure.UniGraph;

/**
 * @author Gur Ronen
 * @since 6/20/2016
 */
@RunWith(UnipopStructureSuite.class)
@GraphProviderClass(provider = JdbcGraphProvider.class, graph = UniGraph.class)
public class JdbcStructureSuite {
}
