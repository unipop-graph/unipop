package org.unipop.elastic.controllerprovider.schema;

import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.elasticsearch.search.SearchHit;
import org.unipop.controller.ElementController;
import org.unipop.controller.Predicates;
import org.unipop.elastic.schema.ElasticElementSchema;
import org.unipop.elastic.schema.Filter;
import org.unipop.schema.impl.BasicSchema;
import org.unipop.schema.impl.PropertySchema;
import org.unipop.structure.UniGraph;

import java.util.Map;

public abstract class DocumentSchema<E extends Element, C extends ElementController<E>> extends BasicSchema implements ElasticElementSchema<E, C> {
    protected final String index;
    protected final String type;
    protected final UniGraph graph;


    public DocumentSchema(HierarchicalConfiguration configuration, UniGraph graph) {
        super(configuration);
        this.graph = graph;
        this.index = configuration.getString("index");
        this.type = configuration.getString("_type", null);
    }

    @Override
    public String getIndex() {
        return this.index;
    }

    @Override
    public E createElement(SearchHit hit, C controller) {
        Map<String, Object> source = hit.getSource();
        Object id = getId(source);
        String label = getLabel(source);
        Map<String, Object> properties = getProperties(source);
        properties.put("~schema", this);
        properties.put("~index", this.index);
        if(this.type != null) properties.put("~type", this.type);

        return createElement(id, label, properties, controller);
    }

    protected abstract E createElement(Object id, String label, Map<String, Object> properties, C controller);

    @Override
    public Map<String, Object> toFields(E element) {
        return toFields(element.id(), element.label(), element.properties());
    }


    @Override
    public Filter getFilter(Predicates<E> predicates) {
        DocumentFilter documentFilter = new DocumentFilter(this.index);
        for(HasContainer hasContainer : predicates.getHasContainers()) {
            String key = hasContainer.getKey();
            Object value = hasContainer.getValue();
            P<?> predicate = hasContainer.getPredicate();
            PropertySchema propertySchema = properties.get(key);
            if(propertySchema != null) {
                if(!propertySchema.test(value, predicate)) return null; //static properties can rule out the schema
                Map<String, Object> fields = propertySchema.toFields(value);
                documentFilter.add(fields, predicate);
            }
            else documentFilter.add(key, value, predicate);
        }
        return documentFilter;
    }
}
