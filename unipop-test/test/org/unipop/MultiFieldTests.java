package org.unipop;

import org.apache.tinkerpop.gremlin.AbstractGremlinTest;
import org.apache.tinkerpop.gremlin.GraphManager;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.junit.Before;
import org.junit.Test;
import org.unipop.elastic.test.ElasticGraphProvider;

/**
 * Created by sbarzilay on 7/7/16.
 */
public class MultiFieldTests extends AbstractGremlinTest{
    public MultiFieldTests() throws Exception {
        GraphManager.setGraphProvider(new ElasticGraphProvider());
    }

    @LoadGraphWith
    @Test
    public void name() throws Exception {

    }
}
