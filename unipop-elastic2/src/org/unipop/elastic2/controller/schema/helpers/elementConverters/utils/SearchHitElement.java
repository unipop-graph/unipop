package org.unipop.elastic2.controller.schema.helpers.elementConverters.utils;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.elasticsearch.search.SearchHit;
import org.unipop.structure.UniGraph;

import java.util.*;

/**
 * Created by Roman on 3/25/2015.
 */
public class SearchHitElement implements ElementWrapper<SearchHit> {
    //region Constructor
    public SearchHitElement(SearchHit searchHit, UniGraph graph) {
        this.searchHit = searchHit;
        this.graph = graph;
    }
    //endregion

    //region Wrapper Implementation
    @Override
    public Element wrap(SearchHit searchHit) {
        this.searchHit = searchHit;
        return this;
    }

    @Override
    public SearchHit unwrap() {
        return this.searchHit;
    }
    //endregion

    //region Element Implementation
    @Override
    public Object id() {
        return this.searchHit.getId();
    }

    @Override
    public String label() {
        return this.searchHit.getType();
    }

    @Override
    public Graph graph() {
        return this.graph;
    }

    @Override
    public Set<String> keys() {
        if (searchHit.getSource() != null) {
            return searchHit.getSource().keySet();
        }

        return Collections.emptySet();
    }

    @Override
    public <V> Property<V> property(String key) {
        final Element thisElement = this;
        return new Property<V>() {
            @Override
            public String key() {
                return key;
            }

            @Override
            public V value() throws NoSuchElementException {
                if (searchHit.getSource() != null) {
                    return (V) searchHit.getSource().get(key);
                }

                return null;
            }

            @Override
            public boolean isPresent() {
                if (searchHit.getSource() != null) {
                    return searchHit.getSource().containsKey(key);
                }

                return false;
            }

            @Override
            public Element element() {
                return thisElement;
            }

            @Override
            public void remove() {
                throw Exceptions.propertyRemovalNotSupported();
            }
        };
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        throw Element.Exceptions.propertyAdditionNotSupported();
    }

    @Override
    public <V> V value(String key) throws NoSuchElementException {
        if (this.searchHit.getSource() != null) {
            return (V)this.searchHit.getSource().get(key);
        }

        return null;
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <V> Iterator<? extends Property<V>> properties(String... strings) {
        Iterable<String> propKeys = strings.length > 0 ? Arrays.asList(strings) :
                this.searchHit.getSource() != null ? this.searchHit.getSource().keySet() :
                        Collections.emptyList();

        ArrayList<Property<V>> properties = new ArrayList<>();
        for(String propKey : propKeys) {
            properties.add(this.<V>property(propKey));
        }

        return properties.iterator();
    }
    //endregion

    //region Fields
    private final Graph graph;
    private SearchHit searchHit;
    //endregion
}
