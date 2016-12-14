package org.unipop.schema.property.type;


/**
 * Created by sbarzilay on 14/12/16.
 */
public class DoubleType implements PropertyType {
    @Override
    public String getType() {
        return "DOUBLE";
    }

    @Override
    public Object convertToType(Object object) {
        return Double.parseDouble(object.toString());
    }
}
