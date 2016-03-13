package org.unipop.structure;

import org.unipop.controller.InnerEdgeController;
import org.unipop.controller.VertexController;
import org.unipop.controllerprovider.ControllerManager;

import java.util.Map;
import java.util.Set;

/**
 * Created by sbarzilay on 3/10/16.
 */
public class UniDelayedStarVertex extends UniStarVertex{
    public UniDelayedStarVertex(Object id, String label, ControllerManager manager, UniGraph graph, Set<InnerEdgeController>innerEdgeControllers) {
        super(id, label, null, manager, graph, innerEdgeControllers);
    }
}
