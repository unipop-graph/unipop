package org.unipop.process;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Created by sbarzilay on 7/6/16.
 */
public class BulkIterator<S> implements Iterator<List<S>> {

    private Iterator<S> original;
    private final int maxBulk;
    private int bulk;
    private int multiplier;
    private List<S> current;

    public BulkIterator(final int maxBulk, int startBulk, int multiplier, Iterator<S> original) {
        this.original = original;
        this.maxBulk = maxBulk;
        this.bulk = startBulk;
        this.multiplier = multiplier;
    }

    @Override
    public boolean hasNext() {
        if (current != null) return true;
        if (!original.hasNext()) return false;
        current = new ArrayList<>(bulk);
        while (current.size() < bulk && original.hasNext())
            current.add(original.next());
        bulk = Math.min(bulk * multiplier, maxBulk);
        return true;
    }

    @Override
    public List<S> next() {
        if (!hasNext()) throw new NoSuchElementException();
        List<S> batch = current;
        current = null;
        return batch;
    }
}
