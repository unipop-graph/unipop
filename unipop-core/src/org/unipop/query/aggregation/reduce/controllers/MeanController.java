package org.unipop.query.aggregation.reduce.controllers;

import org.apache.tinkerpop.gremlin.process.traversal.step.map.MeanGlobalStep;
import org.unipop.query.aggregation.reduce.ReduceQuery;
import org.unipop.query.controller.UniQueryController;

/**
 * @author Gur Ronen
 * @since 6/29/2016
 */
public interface MeanController extends UniQueryController {
    MeanGlobalStep.MeanNumber mean(ReduceQuery reduceQuery);
}
