package org.unipop.schema.property;

import com.google.common.collect.Sets;

import java.util.Collections;
import java.util.Set;

/**
 * Created by sbarzilay on 8/18/16.
 */

public class PropertyType {
    public static final String string = "STRING";
    public static final String number = "NUMBER";
    public static final String date = "DATE";
    public static final String coalesce = "COALESCE";
    public static final String multi = "MULTI";
    public static final String concat = "CONCAT";
    public static final String dynamic = "DYNAMIC";

    public static Set<String> getTypes(){
        return Sets.newHashSet(string, number, date, coalesce, multi, concat, dynamic);
    }
}
