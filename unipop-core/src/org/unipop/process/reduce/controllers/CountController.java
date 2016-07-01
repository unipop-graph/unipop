package org.unipop.process.reduce.controllers;

import org.unipop.query.aggregation.reduce.ReduceQuery;
import org.unipop.query.controller.UniQueryController;

/**
 * @author Gur Ronen
 * @since 6/29/2016
 */
public interface CountController extends UniQueryController {
    long count(ReduceQuery reduceQuery);
}
