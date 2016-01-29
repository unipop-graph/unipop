package org.unipop.elastic2.controller.schema.helpers.elementConverters;

import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by Roman on 3/21/2015.
 */
public class CompositeElementConverter implements ElementConverter<Element, Element> {
    public enum Mode {
        First,
        All
    }

    //region Constructor
    public CompositeElementConverter(Mode mode, ElementConverter<Element, Element>... converters) {
        this.mode = mode;
        this.converters = Arrays.asList(converters);
    }
    //endregion

    //region SearchHitElementConverter Implementation
    @Override
    public boolean canConvert(Element element) {
        for(ElementConverter<Element, Element> converter : converters) {
            if (converter.canConvert(element)) {
                return true;
            }
        }

        return false;
    }

    @Override
    public Iterable<Element> convert(Element element) {
        switch (this.mode) {
            case First:
                for(ElementConverter<Element, Element> converter : converters) {
                    if (converter.canConvert(element)) {
                        return converter.convert(element);
                    }
                }
                return new ArrayList<>();

            case All:
                ArrayList<Element> elements = new ArrayList<>();
                for(ElementConverter<Element, Element> converter : converters) {
                    if (converter.canConvert(element)) {
                        for(Element convertedElement : converter.convert(element)) {
                            elements.add(convertedElement);
                        }
                    }
                }
                return elements;
        }

        return new ArrayList<>();
    }

    //endregion

    //region Fields
    private Iterable<ElementConverter<Element, Element>> converters;
    private Mode mode;
    //endregion
}
