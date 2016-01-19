package org.unipop.elastic2.controller.schema.helpers.elementConverters.utils;

/**
 * Created by Roman on 3/25/2015.
 */
public interface Wrapper<TWrapper, TWrapped> {
    public TWrapper wrap(TWrapped wrapped);
    public TWrapped unwrap();
}
