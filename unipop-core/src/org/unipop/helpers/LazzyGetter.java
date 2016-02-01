package org.unipop.helpers;

import org.unipop.structure.BaseVertex;

/**
 * Created by sbarzilay on 28/01/16.
 */
public interface LazzyGetter {
    Boolean canRegister();
    void register(BaseVertex v, String label, String indexName);
    void execute();
}
