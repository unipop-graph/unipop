package org.unipop.schema.property.type;

/**
 * Created by sbarzilay on 5/06/17.
 */
public class DefaultType implements PropertyType {
    @Override
    public String getType() {
        return "DEFAULT";
    }

    @Override
    public Object convertToType(Object object) {
        return object;
    }
}
