package org.unipop.elastic2.controller.schema.helpers.queryAppenders;


/**
 * Created by Roman on 3/28/2015.
 */
public interface QueryAppender<T> {
    public boolean canAppend(T input);
    public boolean append(T input);
}
