package org.unipop.process.predicate;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;

import java.util.List;

public interface ReceivesHasContainers<S, E> extends Step<S, E> {
    void addHasContainer(HasContainer hasContainer);
    List<HasContainer> getHasContainers();
    void setLimit(int limit);
}
