package com.tinkerpop.gremlin.elastic.structure;

import com.tinkerpop.gremlin.elastic.ElasticGraphGraphProvider;
import com.tinkerpop.gremlin.structure.StructureStandardSuite;
import org.elasticsearch.node.Node;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;


/**
 * Executes the Standard Gremlin Structure Test Suite using TinkerGraph.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
@RunWith(StructureStandardSuite.class)
@StructureStandardSuite.GraphProviderClass(provider = ElasticGraphGraphProvider.class, graph = ElasticGraph.class)
public class ElasticGraphStructureStandardTest {
    static Node node;

    @BeforeClass
    public static void setUp() throws Exception {
        //node = NodeBuilder.nodeBuilder().clusterName("elasticsearch").local(true).node();
        //System.out.print(node.toString());
    }

    @AfterClass
    public static void destroy() {
        //node.close();
    }

}
