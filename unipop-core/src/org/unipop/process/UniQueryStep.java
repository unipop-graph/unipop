package org.unipop.process;

import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.unipop.query.UniQuery;
import org.unipop.query.controller.UniQueryController;
import org.unipop.query.search.SearchQuery;

import java.util.List;


/**
 * Created by sbarzilay on 9/4/16.
 */
public interface UniQueryStep<S> {
    UniQuery getQuery(List<Traverser.Admin<S>> traversers);
    boolean hasControllers();
    void addControllers(List<UniQueryController> controllers);
}
