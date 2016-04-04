package org.unipop.process.properties;

import org.unipop.controller.VertexController;
import org.unipop.structure.DeferredVertex;

import java.util.Iterator;

public interface DeferredVertexController extends VertexController {
    boolean loadProperties(Iterator<DeferredVertex> elements);
}
