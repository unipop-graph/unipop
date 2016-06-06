package org.unipop.process;

import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Scoping;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_LP_O_P_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_LP_O_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;

import java.util.*;

/**
 * Created by sbarzilay on 5/2/16.
 */
public class UniGraphWhereTraversalStep<S extends Element> extends AbstractStep<S, S> implements TraversalParent {
    protected Traversal<S, S> whereTraversal;
    protected Iterator<Traverser<S>> results;
    protected List<Traverser<S>> originals;

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
        this.whereTraversal = whereTraversal;
        whereTraversal.asAdmin().addStep(new UniGraphTraverserStep(whereTraversal.asAdmin()));
        originals = new ArrayList<>();
        results = EmptyIterator.instance();
    }

    @Override
    protected Traverser<S> processNextStart() throws NoSuchElementException {
        if (this.starts.hasNext()) {
            this.starts.forEachRemaining(start -> {
                start.setSideEffects(new DefaultTraversalSideEffects() {{
                    set("_whereStep", start);
                }});
                whereTraversal.asAdmin().addStart(start);
                originals.add(start);
            });
            if(whereTraversal.asAdmin().getStartStep() instanceof UniGraphWhereStartStep){
                ((UniGraphWhereStartStep) whereTraversal.asAdmin().getStartStep()).addOriginals(originals);
            }
        }
        if (results instanceof EmptyIterator) {
            List<Traverser<S>> resultsList = new ArrayList<>();
            while (whereTraversal.hasNext()) {
                B_O_S_SE_SL_Traverser next = (B_O_S_SE_SL_Traverser) whereTraversal.next();
                Object whereStep = next.getSideEffects().get("_whereStep").get();
                if (whereStep instanceof Traverser)
                    resultsList.add((Traverser<S>) whereStep);
                else {
                    results = ((ArrayList<Traverser<S>>) whereStep).iterator();
                }
            }
            if (resultsList.size() > 0)
                results = resultsList.iterator();
        }
        Traverser<S> next = results.next();
//        System.out.println(next);
        return next;
    }

    public static class UniGraphWhereStartStep<S> extends AbstractStep implements Scoping {

        protected String selectKey;
        protected List<Traverser<S>> originals;

        public UniGraphWhereStartStep(Traversal.Admin traversal, String selectKey) {
            super(traversal);
            this.selectKey = selectKey;
        }

        @Override
        protected Traverser processNextStart() throws NoSuchElementException {
            Object scopeValue = getScopeValue(Pop.last, selectKey, starts.next());
            Traverser<S> orig = null;
            List<Traverser<S>> origMaps = new ArrayList<>();
            for (Traverser<S> original : originals) {
                if (original.get() instanceof Map) {
                    Map<String, S> origMap = (Map<String, S>) original.get();
                    if (origMap.get(selectKey).equals(scopeValue)) {
                        orig = original;
                        origMaps.add(original);
                    }
                }
                else {
                    if(original.get().equals(scopeValue))
                        return original;
                }
            }
            if (orig != null){
                Traverser.Admin split = orig.asAdmin().split(((Map<String, S>) origMaps.get(0).get()).get(selectKey), this);
                split.setSideEffects(new DefaultTraversalSideEffects() {{
                    set("_whereStep", origMaps);
                }});
                return split;
            }
            throw FastNoSuchElementException.instance();
        }

        @Override
        public Set<String> getScopeKeys() {
            return Collections.singleton(selectKey);
        }

        public void addOriginals(List<Traverser<S>> originals) {
            this.originals = originals;
        }
    }

    public static class UniGraphWhereEndStep<S> extends AbstractStep<S,S> implements Scoping{

        protected String selectKey;

        public UniGraphWhereEndStep(Traversal.Admin traversal, String selectKey) {
            super(traversal);
            this.selectKey = selectKey;
        }

        @Override
        protected Traverser<S> processNextStart() throws NoSuchElementException {
            Traverser.Admin<S> next = starts.next();
            B_O_S_SE_SL_Traverser traverser = (B_O_S_SE_SL_Traverser) next;
            ArrayList<Traverser<S>> whereStep = (ArrayList<Traverser<S>>) traverser.getSideEffects().get("_whereStep").get();
            for (Traverser<S> stringSMap : whereStep) {
                if (((Map<String, S>) stringSMap.get()).get(selectKey).equals(next.get())){
                    return stringSMap;
                }
            }
            throw FastNoSuchElementException.instance();
        }

        @Override
        public Set<String> getScopeKeys() {
            return Collections.singleton(selectKey);
        }
    }
}
