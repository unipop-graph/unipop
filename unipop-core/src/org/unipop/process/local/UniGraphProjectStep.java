package org.unipop.process.local;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.ByModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ProjectStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.RequirementsStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.javatuples.Pair;
import org.javatuples.Tuple;
import org.unipop.process.UniBulkStep;
import org.unipop.process.traverser.UniGraphTraverserStep;
import org.unipop.process.vertex.UniGraphVertexStep;
import org.unipop.query.aggregation.LocalQuery;
import org.unipop.query.search.SearchVertexQuery;
import org.unipop.structure.UniElement;
import org.unipop.structure.UniGraph;
import org.unipop.structure.UniVertex;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Created by sbarzilay on 9/28/16.
 */
public class UniGraphProjectStep<S, E> extends UniLocalBulkStep<S, E, Map<String, E>> {
    private String[] keys;

    public UniGraphProjectStep(Traversal.Admin traversal, UniGraph graph, String[] keys,
                               List<LocalQuery.LocalController> controllers,
                               List<SearchVertexQuery.SearchVertexController> nonLocalControllers,
                               List<Traversal.Admin<?,?>> traversals) {
        super(traversal, graph, controllers, nonLocalControllers);
        traversals.forEach(t -> locals.add(createLocalStep((Traversal.Admin<S, E>) t)));
        this.keys = keys;
    }

    @Override
    protected Iterator<Traverser.Admin<Map<String, E>>> process(List<Traverser.Admin<S>> traversers) {
        Map<UniElement, Map<String, E>> maps = new HashMap<>();
        for (int i = 0; i < this.locals.size(); i++) {
            UniGraphLocalStep<S, E> project = locals.get(i);
            project.reset();
            traversers.forEach(project::addStart);
            List<Pair<UniElement, E>> projectResult = new ArrayList<>();
            project.forEachRemaining(a -> projectResult.add(Pair.with(a.getSideEffects().get("prev"), a.get())));
            for (int j = 0; j < projectResult.size(); j++) {
                Object key = projectResult.get(j).getValue0();
                if (key instanceof List)
                    key = ((List) key).get(((List)key).size() -1);
                Map<String, E> map = maps.containsKey(key) ? maps.get(key) : new HashMap<>();
                E value = projectResult.get(j).getValue1();
                if (value instanceof Collection)
                    if (((Collection) value).isEmpty())
                        continue;
                map.put(keys[i], value);
                if (key instanceof Traverser)
                    key = ((Traverser) key).get();
                maps.put((UniElement)key , map);
            }
        }
        List<Traverser.Admin<Map<String, E>>> results = new ArrayList<>();
        for (int i = 0; i < traversers.size(); i++) {
            results.add(traversers.get(i).split(maps.get(traversers.get(i).get()), this));
        }
        return results.iterator();
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, Arrays.asList(this.keys), this.locals);
    }
}



