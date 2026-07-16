package org.unipop.process;

/** A search step that can receive a range low-bound (offset) and report how much of it the
 *  provider pushed to the backend (0 if none). See UniGraphRangeStep. */
public interface Offsettable {
    void setOffset(int low);
    int getPushedOffset();
}
