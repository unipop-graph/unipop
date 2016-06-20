package org.unipop.jdbc.suite;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.junit.runner.RunWith;
import org.unipop.common.test.UnipopGraphProvider;
import org.unipop.common.test.UnipopProcessSuite;
import org.unipop.jdbc.JdbcGraphProvider;
import org.unipop.structure.UniGraph;

/**
 * @author GurRo
 * @since 6/20/2016
 */
@RunWith(UnipopProcessSuite.class)
@GraphProviderClass(provider = , graph = UniGraph.class)
public class JdbcProcessSuite extends UnipopGraphProvider {

    public JdbcGraphProvider() {

    }

}
