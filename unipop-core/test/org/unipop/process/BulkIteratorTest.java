package org.unipop.process;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BulkIteratorTest {

    @Test
    public void repeatedHasNextKeepsTheSameBatch() {
        BulkIterator<Integer> batches = new BulkIterator<>(2, 2, 2, Arrays.asList(1, 2, 3).iterator());
        assertTrue(batches.hasNext());
        assertTrue(batches.hasNext());
        assertEquals(Arrays.asList(1, 2), batches.next());
        assertEquals(Collections.singletonList(3), batches.next());
        assertFalse(batches.hasNext());
    }
}
