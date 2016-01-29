package org.unipop.elastic2.controller.schema.helpers.queryAppenders;

import org.unipop.elastic2.controller.schema.helpers.QueryBuilder;

import java.util.Map;

/**
 * Created by Gilad on 14/10/2015.
 */
public class GraphBulkInput {
    //Constructor
    public GraphBulkInput(
            Map<String, Iterable<String>> typeElementIds,
            Iterable<String> typesToQuery,
            QueryBuilder queryBuilder) {
        this.typeElementIds = typeElementIds;
        this.typesToQuery = typesToQuery;
        this.queryBuilder = queryBuilder;
    }
    //endregion

    //region Properties
    public Map<String, Iterable<String>> getTypeElementIds() {
        return this.typeElementIds;
    }

    public Iterable<String> getTypesToQuery() {
        return this.typesToQuery;
    }

    public QueryBuilder getQueryBuilder() {
        return this.queryBuilder;
    }
    //endregion

    //region Fields
    private Map<String, Iterable<String>> typeElementIds;
    private Iterable<String> typesToQuery;

    private QueryBuilder queryBuilder;
    //endregion
}
