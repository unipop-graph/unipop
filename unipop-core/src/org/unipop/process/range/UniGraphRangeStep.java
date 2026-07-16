package org.unipop.process.range;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.unipop.process.Offsettable;

import java.util.NoSuchElementException;

/**
 * Schema-aware replacement for a global RangeGlobalStep that follows a UniGraph search step.
 * Applies the RESIDUAL range: range(low - pushed, high - pushed), where `pushed` is how much of the
 * offset the provider already applied as SQL OFFSET (read from the source step lazily, once the first
 * upstream element arrives -- by then the source's search has run and set pushedOffset). When the
 * provider pushed nothing (fan-out, federated, non-JDBC, offset not applicable) pushed==0 and this is
 * exactly a native range(low, high) in memory.
 */
public final class UniGraphRangeStep<S> extends AbstractStep<S, S> {

    private final long low;
    private final long high;   // -1 == unbounded
    private final Step<?, ?> source;

    private boolean resolved = false;
    private long effLow;
    private long effHigh;      // -1 == unbounded
    private long counter = 0L;

    public UniGraphRangeStep(Traversal.Admin traversal, long low, long high, Step<?, ?> source) {
        super(traversal);
        this.low = low;
        this.high = high;
        this.source = source;
    }

    private void resolve() {
        int pushed = (source instanceof Offsettable) ? ((Offsettable) source).getPushedOffset() : 0;
        this.effLow = Math.max(0L, low - pushed);
        this.effHigh = (high == -1L) ? -1L : Math.max(0L, high - pushed);
        this.resolved = true;
    }

    @Override
    protected Traverser.Admin<S> processNextStart() throws NoSuchElementException {
        while (true) {
            // Pulling the first start triggers the upstream search, which sets pushedOffset.
            final Traverser.Admin<S> start = this.starts.next();  // throws FastNoSuchElement when drained
            if (!resolved) resolve();

            if (effHigh != -1L && counter >= effHigh) {
                throw FastNoSuchElementException.instance();      // window closed -> stop pulling
            }

            final long bulk = start.bulk();
            // number of items in [counter, counter+bulk) that fall within [effLow, effHigh)
            final long from = counter;
            final long to = counter + bulk;
            counter = to;

            final long windowStart = effLow;
            final long windowEnd = (effHigh == -1L) ? Long.MAX_VALUE : effHigh;
            final long emitFrom = Math.max(from, windowStart);
            final long emitTo = Math.min(to, windowEnd);
            if (emitTo > emitFrom) {
                start.setBulk(emitTo - emitFrom);
                return start;
            }
            // else: entirely before the window -> skip, pull next
        }
    }

    @Override
    public void reset() {
        super.reset();
        this.resolved = false;
        this.counter = 0L;
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, low, high);
    }
}
