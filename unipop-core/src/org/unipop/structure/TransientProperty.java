package org.unipop.structure;

/**
 * Created by sbarzilay on 3/11/16.
 */
public class TransientProperty<V> extends BaseVertexProperty<V>{
    public TransientProperty(BaseVertex vertex, String key, V value) {
        super(vertex, key, value);
    }
}
