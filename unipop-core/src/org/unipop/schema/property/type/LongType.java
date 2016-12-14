package org.unipop.schema.property.type;

/**
 * Created by sbarzilay on 14/12/16.
 */
public class LongType implements PropertyType {
    @Override
    public String getType() {
        return "LONG";
    }

    @Override
    public Object convertToType(Object object) {
        if (object instanceof Number)
            return ((Number) object).longValue();
        return Long.parseLong(object.toString());
    }
}
