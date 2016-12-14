package org.unipop.schema.property.type;

/**
 * Created by sbarzilay on 14/12/16.
 */
public class IntType implements PropertyType {
    @Override
    public String getType() {
        return "INT";
    }

    @Override
    public Object convertToType(Object object) {
        if (object instanceof Number)
            return ((Number) object).intValue();
        return Integer.parseInt(object.toString());
    }
}
