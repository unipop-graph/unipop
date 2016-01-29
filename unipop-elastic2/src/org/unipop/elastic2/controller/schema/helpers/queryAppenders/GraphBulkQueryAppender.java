package org.unipop.elastic2.controller.schema.helpers.queryAppenders;

import java.util.Map;

/**
 * Created by Gilad on 14/10/2015.
 */
public class GraphBulkQueryAppender implements QueryAppender<GraphBulkInput> {
    public enum TypeMode {
        cartesian,
        match
    }

    //region Constructor
    public GraphBulkQueryAppender(TypeMode typeMode,
                                  QueryAppender<GraphTypeBulkInput> graphTypeBulkInputQueryAppender) {
        this.typeMode = typeMode;
        this.graphTypeBulkQueryAppender = graphTypeBulkInputQueryAppender;
    }
    //endregion

    //region QueryAppender Implementation
    @Override
    public boolean canAppend(GraphBulkInput input) {
        return true;
    }

    @Override
    public boolean append(GraphBulkInput input) {
        boolean appendedSuccesfully = false;
        for(String typeToQuery : input.getTypesToQuery()) {
            for (Map.Entry<String, Iterable<String>> entry : input.getTypeElementIds().entrySet()) {

                if (this.typeMode == TypeMode.match) {
                    if (!typeToQuery.equals(entry.getKey())) {
                        continue;
                    }
                }

                GraphTypeBulkInput graphTypeBulkInput = new GraphTypeBulkInput(
                        input.getQueryBuilder(),
                        entry.getValue(),
                        entry.getKey(),
                        typeToQuery
                );

                if (this.graphTypeBulkQueryAppender.canAppend(graphTypeBulkInput)) {
                    appendedSuccesfully = this.graphTypeBulkQueryAppender.append(graphTypeBulkInput) || appendedSuccesfully;
                }
            }
        }

        return appendedSuccesfully;
    }
    //endregion

    //region Fields
    private QueryAppender<GraphTypeBulkInput> graphTypeBulkQueryAppender;
    private TypeMode typeMode;
    //endregion
}
