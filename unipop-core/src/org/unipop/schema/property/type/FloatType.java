package org.unipop.schema.property.type;

/**
 * Created by sbarzilay on 14/12/16.
 */
public class FloatType implements PropertyType {
    @Override
    public String getType() {
        return "FLOAT";
    }

    @Override
    public Object convertToType(Object object) {
        return Float.parseFloat(object.toString());
    }
}
