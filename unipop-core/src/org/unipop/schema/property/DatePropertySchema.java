package org.unipop.schema.property;

import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;

/**
 * Created by sbarzilay on 8/2/16.
 */
public interface DatePropertySchema {
    DateFormat getSourceDateFormat();

    DateFormat getDisplayDateFormat();

    default Date fromSource(String date) {
        try {
            return getSourceDateFormat().parse(date);
        } catch (ParseException e) {
            throw new RuntimeException(String.format("couldn't parse date:{0} using:{1}", date, getSourceDateFormat()));
        }
    }

    default Date fromDisplay(String date) {
        try {
            return getDisplayDateFormat().parse(date);
        } catch (ParseException e) {
            throw new RuntimeException(String.format("couldn't parse date:{0} using:{1}", date, getDisplayDateFormat()));
        }
    }

    default String toDisplay(Date date) {
        return getDisplayDateFormat().format(date);
    }

    default String toSource(Date date) {
        return getSourceDateFormat().format(date);
    }
}
