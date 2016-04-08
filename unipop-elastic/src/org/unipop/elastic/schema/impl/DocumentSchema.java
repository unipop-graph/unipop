package org.unipop.elastic.schema.impl;

import org.apache.commons.cli.MissingArgumentException;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.elasticsearch.search.SearchHit;
import org.javatuples.Pair;
import org.unipop.common.schema.BasicSchema;
import org.unipop.common.property.PropertySchema;
import org.unipop.query.UniQuery;
import org.unipop.elastic.schema.ElasticElementSchema;
import org.unipop.elastic.schema.Filter;
import org.unipop.query.search.SearchQuery;
import org.unipop.structure.UniGraph;

import java.util.Iterator;
import java.util.Map;

public abstract class DocumentSchema<E extends Element> extends BasicSchema<E>  implements ElasticElementSchema<E> {
    protected final String index;
    protected final String type;

    public DocumentSchema(HierarchicalConfiguration configuration, UniGraph graph) throws MissingArgumentException {
        super(configuration, graph);
        this.index = configuration.getString("index", "all");
        this.type = configuration.getString("_type", null);
    }

    @Override
    public String getIndex() {
        return this.index;
    }

    @Override
    public E fromFields(SearchHit hit) {
        Map<String, Object> fields = hit.getSource();
        fields.put("~schema", this);
        fields.put("~index", this.getIndex());
        if(this.type != null) fields.put("~type", hit.getType());

        return fromFields(fields);
    }

    @Override
    public <E extends Element>  Filter getFilter(SearchQuery<E> uniQuery) {
        DocumentFilter documentFilter = new DocumentFilter(this.index);
        for(HasContainer hasContainer : uniQuery.getPredicates()) {
            String key = hasContainer.getKey();
            Object value = hasContainer.getValue();
            P predicate = hasContainer.getPredicate();
            PropertySchema propertySchema = properties.get(key);
            if(propertySchema != null) {
                if(!propertySchema.test(predicate))
                    return null; //static properties can rule out the schema
                Iterator<Pair<String, Object>> fields = propertySchema.toFields(value);
                documentFilter.add(fields, predicate);
            }
        }
        return documentFilter;
    }
}
