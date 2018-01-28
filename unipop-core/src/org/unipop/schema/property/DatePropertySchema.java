package org.unipop.schema.property;

import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;

/**
 * A property schema of a date
 * Created by sbarzilay on 8/2/16.
 */
public interface DatePropertySchema {
    /**
     * Returns source date format
     * @return Source date format
     */
    DateFormat getSourceDateFormat();

    /**
     * Returns display date format
     * @return Display date format
     */
    DateFormat getDisplayDateFormat();

    /**
     * Converts a string into a date with the source date format
     * @param date A string representing a date
     * @return Date
     */
    default Date fromSource(String date) {
        try {
            return getSourceDateFormat().parse(date);
        } catch (ParseException e) {
            throw new RuntimeException(String.format("couldn't parse date:{0} using:{1}", date, getSourceDateFormat()));
        }
    }

    /**
     * Converts a string into a date with the display date format
     * @param date A string representing a date
     * @return Date
     */
    default Date fromDisplay(String date) {
        try {
            return getDisplayDateFormat().parse(date);
        } catch (ParseException e) {
            throw new RuntimeException(String.format("couldn't parse date:{0} using:{1}", date, getDisplayDateFormat()));
        }
    }

    /**
     * Converts a date into a string with the display date format
     * @param date A date
     * @return A string representing a date
     */
    default String toDisplay(Date date) {
        return getDisplayDateFormat().format(date);
    }

    /**
     * Converts a date into a string with the source date format
     * @param date A date
     * @return A string representing a date
     */
    default String toSource(Date date) {
        return getSourceDateFormat().format(date);
    }
}
