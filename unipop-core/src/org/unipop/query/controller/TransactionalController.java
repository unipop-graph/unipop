package org.unipop.query.controller;

/**
 * A controller whose writes can be scoped to a transaction on its own source.
 *
 * Only a controller over a source with real transactions may implement this. A source without them
 * must not, so that {@link org.unipop.structure.UniGraph#tx()} can refuse outright instead of
 * handing back a transaction that would silently fail to be atomic.
 */
public interface TransactionalController extends UniQueryController {

    /** Pin the resources this controller needs for a transaction and suspend auto-commit. */
    void begin();

    /** Make every write issued since {@link #begin()} durable and release the pinned resources. */
    void commit();

    /** Undo every write issued since {@link #begin()} and release the pinned resources. */
    void rollback();
}
