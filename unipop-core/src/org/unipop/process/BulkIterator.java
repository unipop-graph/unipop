package org.unipop.process;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

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
        if (!original.hasNext())
            return false;
        else {
            List<S> list = new ArrayList<>(bulk);
            for (int i = 0; i < bulk; i++) {
                if (original.hasNext())
                    list.add(original.next());
            }
            bulk = Math.min(bulk * multiplier, maxBulk);
            current = list;
            return current.size() > 0;
        }
    }

    @Override
    public List<S> next() {
        return this.current;
    }
}
