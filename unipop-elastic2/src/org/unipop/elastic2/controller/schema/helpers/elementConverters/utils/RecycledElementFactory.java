package org.unipop.elastic2.controller.schema.helpers.elementConverters.utils;

import java.util.List;

/**
 * Created by Roman on 3/25/2015.
 */
public class RecycledElementFactory<TElementWrapper extends ElementWrapper<TWrapped>, TWrapped>
        implements ElementFactory<TWrapped, TElementWrapper>{
    //region Constructor
    public RecycledElementFactory(List<TElementWrapper> elementWrappers) {
        this.elementWrappers = elementWrappers;
    }
    //endregion

    //reguib ElementFactory Implementation
    @Override
    public TElementWrapper getElement(TWrapped input) {
        try {
            return (TElementWrapper) this.elementWrappers.get(current).wrap(input);
        } finally {
            current++;
            if (current == this.elementWrappers.size()) {
                current = 0;
            }
        }
    }
    //endregion

    //region Fields
    private List<TElementWrapper> elementWrappers;
    private int current = 0;
    //endregion
}
