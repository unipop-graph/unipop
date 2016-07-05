package org.unipop.process.where;

import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.unipop.process.traverser.UniGraphTraverserStep;

import java.util.*;

/**
 * Created by sbarzilay on 5/2/16.
 */
public class UniGraphWhereTraversalStep<S extends Element> extends AbstractStep<S, S> implements TraversalParent {
    protected Traversal.Admin<S, S> whereTraversal;
    protected Iterator<Traverser.Admin<S>> results;
    protected List<Traverser.Admin<S>> originals;
    protected boolean had = false;

    public String toString() {
        return StringFactory.stepString(this, this.whereTraversal);
    }

    @Override
    public List<Traversal.Admin<S, S>> getGlobalChildren() {
        return Arrays.asList(whereTraversal);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        Set<TraverserRequirement> reqs = new HashSet<>();
        whereTraversal.asAdmin().getTraverserRequirements().forEach(reqs::add);
        reqs.add(TraverserRequirement.SIDE_EFFECTS);
        reqs.add(TraverserRequirement.PATH);
        return reqs;
    }

    public UniGraphWhereTraversalStep(Traversal.Admin traversal, Traversal<S, S> whereTraversal) {
        super(traversal);
        this.whereTraversal = whereTraversal.asAdmin();
        whereTraversal.asAdmin().addStep(new UniGraphTraverserStep(whereTraversal.asAdmin()));
        originals = new ArrayList<>();
        results = EmptyIterator.instance();
    }

    @Override
    protected Traverser.Admin<S> processNextStart() throws NoSuchElementException {
        if (this.starts.hasNext()) {
            this.starts.forEachRemaining(start -> {
                start.setSideEffects(new DefaultTraversalSideEffects() {{
                    register("_whereStep", () -> start, (sAdmin, sAdmin2) -> sAdmin);
                }});
                whereTraversal.asAdmin().addStart(start);
                originals.add(start);
                if (whereTraversal.asAdmin().getStartStep() instanceof UniGraphWhereStartStep) {
                    ((UniGraphWhereStartStep) whereTraversal.asAdmin().getStartStep()).addOriginal(start);
                }
            });
            had = true;
        }
        if (had && !results.hasNext()) {
            HashSet<Traverser.Admin<S>> resultsList = new HashSet<>();
            while (whereTraversal.hasNext()) {
                had = false;
                B_O_S_SE_SL_Traverser next = (B_O_S_SE_SL_Traverser) whereTraversal.next();
                Object whereStep = next.getSideEffects().get("_whereStep");
                if (whereStep instanceof Traverser)
                    resultsList.add((Traverser.Admin<S>) whereStep);
                else {
                    results = ((ArrayList<Traverser.Admin<S>>) whereStep).iterator();
                }
            }
            if (resultsList.size() > 0)
                results = resultsList.iterator();
        }
        return results.next();
    }

    public static class UniGraphWhereStartStep<S> extends AbstractStep implements Scoping {

        protected String selectKey;
        protected List<Traverser.Admin<S>> originals;

        public UniGraphWhereStartStep(Traversal.Admin traversal, String selectKey) {
            super(traversal);
            this.selectKey = selectKey;
            originals = new ArrayList<>();
        }

        @Override
        protected Traverser.Admin processNextStart() throws NoSuchElementException {
            if (null == selectKey)
                return starts.next();
            Object scopeValue = getScopeValue(Pop.last, selectKey, starts.next());
            Traverser.Admin<S> orig = null;
            List<Traverser.Admin<S>> origMaps = new ArrayList<>();
            for (Traverser.Admin<S> original : originals) {
                if (original.get() instanceof Map) {
                    Map<String, S> origMap = (Map<String, S>) original.get();
                    if (origMap.get(selectKey).equals(scopeValue)) {
                        orig = original;
                        origMaps.add(original);
                    }
                } else {
                    if (original.get().equals(scopeValue))
                        return original;
                }
            }
            if (orig != null) {
                Traverser.Admin split = orig.asAdmin().split(((Map<String, S>) origMaps.get(0).get()).get(selectKey), this);
                split.setSideEffects(new DefaultTraversalSideEffects() {{
                    register("_whereStep", () -> origMaps, (sAdmin, sAdmin2) -> sAdmin);
                }});
                return split;
            }
            throw FastNoSuchElementException.instance();
        }

        @Override
        public Set<String> getScopeKeys() {
            return Collections.singleton(selectKey);
        }

        public void addOriginal(Traverser.Admin<S> original) {
            originals.add(original);
        }
    }

    public static class UniGraphWhereEndStep<S> extends AbstractStep<S, S> implements Scoping {

        protected String selectKey;
        List<Traverser.Admin<S>> results;
        Iterator<Traverser.Admin<S>> resultsIter = EmptyIterator.instance();

        public UniGraphWhereEndStep(Traversal.Admin traversal, String selectKey) {
            super(traversal);
            this.selectKey = selectKey;
            results = new ArrayList<>();
        }

        @Override
        protected Traverser.Admin<S> processNextStart() throws NoSuchElementException {
            if (resultsIter instanceof EmptyIterator) {
                while (starts.hasNext()) {
                    Traverser.Admin<S> next = starts.next();
                    B_O_S_SE_SL_Traverser traverser = (B_O_S_SE_SL_Traverser) next;
                    if (traverser.getSideEffects().get("_whereStep") instanceof Traverser) {
                        results.add(traverser.getSideEffects().get("_whereStep"));
                    }
                    ArrayList<Traverser.Admin<S>> whereStep = traverser.getSideEffects().get("_whereStep");
                    for (Traverser.Admin<S> stringSMap : whereStep) {
                        if (((Map<String, S>) stringSMap.get()).get(selectKey).equals(next.get())) {
                            results.add(stringSMap);
                        }
                    }
                }
                resultsIter = results.iterator();
            }
            if (resultsIter.hasNext())
                return resultsIter.next();
            throw FastNoSuchElementException.instance();
        }

        @Override
        public Set<String> getScopeKeys() {
            return Collections.singleton(selectKey);
        }
    }
}
