package org.unipop.elastic2.controller.schema.helpers.queryAppenders;

import org.unipop.elastic2.controller.schema.helpers.QueryBuilder;

/**
 * Created by Gilad on 14/10/2015.
 */
public class GraphTypeBulkInput {
    //region Constructor
    public GraphTypeBulkInput(
            QueryBuilder queryBuilder,
            Iterable<String> elementIds,
            String elementType,
            String typeToQuery) {
        this.queryBuilder = queryBuilder;
        this.elementIds = elementIds;
        this.elementType = elementType;
        this.typeToQuery = typeToQuery;
    }
    //endregion

    //region GraphQueryAppenderBase.Input Implementation
    public Iterable<String> getElementIds() {
        return this.elementIds;
    }

    public String getElementType() {
        return this.elementType;
    }

    public String getTypeToQuery() {
        return this.typeToQuery;
    }

    public QueryBuilder getQueryBuilder() {
        return queryBuilder;
    }
    //endregion

    //region Fields
    private Iterable<String> elementIds;
    private String elementType;
    private String typeToQuery;

    private QueryBuilder queryBuilder;
    //endregion
}
