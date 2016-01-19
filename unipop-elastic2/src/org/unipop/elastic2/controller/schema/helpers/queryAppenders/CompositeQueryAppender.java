package org.unipop.elastic2.controller.schema.helpers.queryAppenders;

import java.util.Arrays;

/**
 * Created by Roman on 3/28/2015.
 */
public class CompositeQueryAppender<TInput> implements QueryAppender<TInput> {
    public enum Mode {
        First,
        All
    }

    //region Constructor
    public CompositeQueryAppender(Mode mode, QueryAppender<TInput>... queryAppenders) {
        this.mode = mode;
        this.queryAppenders = Arrays.asList(queryAppenders);
    }
    //endregion

    //region QueryAppender Implementation
    @Override
    public boolean canAppend(TInput input) {
        for(QueryAppender<TInput> queryAppender : this.queryAppenders) {
            if (queryAppender.canAppend(input)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public boolean append(TInput input) {
        switch (this.mode) {
            case First:
                for(QueryAppender<TInput> queryAppender : this.queryAppenders) {
                    if (queryAppender.canAppend(input)) {
                        return queryAppender.append(input);
                    }
                }
                break;

            case All:
                boolean appendedSuccesfully = false;
                for(QueryAppender<TInput> queryAppender : this.queryAppenders) {
                    if (queryAppender.canAppend(input)) {
                        appendedSuccesfully = queryAppender.append(input) || appendedSuccesfully;
                    }
                }
                return appendedSuccesfully;
        }

        return false;
    }
    //endregion

    //region Fields
    private Iterable<QueryAppender<TInput>> queryAppenders;
    private Mode mode;
    //endregion
}
