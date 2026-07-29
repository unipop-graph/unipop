package org.unipop.structure;

import org.apache.tinkerpop.gremlin.structure.util.AbstractThreadLocalTransaction;
import org.unipop.query.controller.TransactionalController;

import java.util.List;

/**
 * The handle gremlin-server drives; the atomicity itself belongs to the source.
 *
 * Thread-local is the right shape here rather than an accident of the base class: on both server
 * paths one request is opened, iterated, serialized and committed on a single thread —
 * TraversalOpProcessor runs beforeProcessing/handleIterator/onTraversalSuccess inside one task
 * submitted to the Gremlin executor, and SingleTaskSession.run() pins sessionThread the same way.
 *
 * Threaded transactions stay unsupported: AbstractTransaction.createThreadedTx() already throws.
 */
public class UniTransaction extends AbstractThreadLocalTransaction {

    /**
     * The controllers this thread opened on, held for the transaction's duration. Resolved per-open
     * rather than once, because a config reload swaps the controllers — and their connection pool — out
     * from under us; committing on the set we began with is the only coherent choice.
     */
    private final ThreadLocal<List<TransactionalController>> participants = new ThreadLocal<>();
    private final UniGraph graph;

    UniTransaction(final UniGraph graph) {
        super(graph);
        this.graph = graph;
    }

    @Override
    protected void doOpen() {
        List<TransactionalController> current = graph.transactionalControllers();
        current.forEach(TransactionalController::begin);
        participants.set(current);
    }

    @Override
    protected void doCommit() {
        // cleared even if the source throws: the controller has released its connection either way, so
        // leaving this open would strand the next request on this thread with a transaction it cannot use
        try {
            participants.get().forEach(TransactionalController::commit);
        } finally {
            participants.remove();
        }
    }

    @Override
    protected void doRollback() {
        try {
            participants.get().forEach(TransactionalController::rollback);
        } finally {
            participants.remove();
        }
    }

    @Override
    public boolean isOpen() {
        return participants.get() != null;
    }
}
