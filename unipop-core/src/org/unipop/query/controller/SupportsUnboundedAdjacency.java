package org.unipop.query.controller;

/**
 * Marker for a {@link org.unipop.query.search.SearchVertexQuery.SearchVertexController} that can honour
 * an unbounded-source SearchVertexQuery ({@code isAllSources()}) — i.e. run the adjacency as a direct
 * edge query with no source-id bound (single JOIN when possible, edge-label-only scan otherwise).
 *
 * <p>UnboundedVertexAdjacencyStrategy uses the single-JOIN start step only when at least one search
 * controller advertises this; otherwise it keeps the backend-agnostic {@code g.E().<adjacency>()}
 * rewrite, so a controller that does not implement the allSources contract is never handed one.
 */
public interface SupportsUnboundedAdjacency {
}
