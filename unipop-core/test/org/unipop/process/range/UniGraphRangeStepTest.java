package org.unipop.process.range;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ScalarMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal;
import org.junit.Test;
import org.unipop.process.Offsettable;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class UniGraphRangeStepTest {

    // A trivial source step that reports a fixed pushedOffset.
    // TP 3.8.1 note: MapStep here is a bare marker class (no map()/processNextStart());
    // those live on ScalarMapStep, which extends MapStep. Extend that instead.
    static class FakeSource extends ScalarMapStep<Object, Object> implements Offsettable {
        private final int pushed;
        FakeSource(Traversal.Admin t, int pushed) { super(t); this.pushed = pushed; }
        @Override protected Object map(Traverser.Admin<Object> t) { return t.get(); }
        @Override public void setOffset(int low) {}
        @Override public int getPushedOffset() { return pushed; }
    }

    private List<Object> run(long low, long high, int pushed, int count) {
        DefaultTraversal t = new DefaultTraversal();
        FakeSource source = new FakeSource(t, pushed);
        UniGraphRangeStep<Object> step = new UniGraphRangeStep<>(t, low, high, source);
        for (int i = 0; i < count; i++) {
            step.addStart(new B_O_Traverser<>((Object) Integer.valueOf(i), 1L).asAdmin());
        }
        List<Object> out = new ArrayList<>();
        // Unwrap the traverser's value: Traverser.Admin.equals() only matches other Traversers,
        // so comparing raw next() results against plain Integers would always fail List.equals().
        while (step.hasNext()) out.add(step.next().get());
        return out;
    }

    @Test public void noPushAppliesFullRange() {
        // range(2,4) over [0..9], pushed=0 -> elements 2,3
        assertEquals(java.util.Arrays.asList(2, 3), run(2, 4, 0, 10));
    }

    @Test public void fullPushPassesThrough() {
        // DB already did OFFSET 2 LIMIT 2, so upstream is the 2-row window [x,y]; pushed=2,
        // residual range(0,2) -> pass through both.
        assertEquals(java.util.Arrays.asList(0, 1), run(2, 4, 2, 2));
    }

    @Test public void unboundedHighSkipsLowTakesRest() {
        // skip(3): range(3,-1), pushed=0 -> 3,4,5,6,7,8,9
        assertEquals(java.util.Arrays.asList(3, 4, 5, 6, 7, 8, 9), run(3, -1, 0, 10));
    }

    @Test public void emptyUpstreamEmptyOut() {
        assertEquals(java.util.Collections.emptyList(), run(2, 4, 0, 0));
    }
}
