package org.unipop.util;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.text.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by sbarzilay on 8/19/16.
 */
public class MultiDateFormat extends DateFormat {
    private List<SimpleDateFormat> formats;

    public MultiDateFormat(String format, String... formats) {
        this.formats = new ArrayList<>();
        this.formats.add(new SimpleDateFormat(format));
        for (String f : formats) {
            this.formats.add(new SimpleDateFormat(f));
        }
    }

    @Override
    public StringBuffer format(Date date, StringBuffer toAppendTo, FieldPosition fieldPosition) {
        return this.formats.get(0).format(date, toAppendTo, fieldPosition);
    }

    @Override
    public Date parse(String source) throws ParseException {
        for (SimpleDateFormat format : formats) {
            try {
                return format.parse(source);
            }
            catch (ParseException ignored){}
        }
        throw new ParseException(source, 0);
    }

    @Override
    public Date parse(String source, ParsePosition pos) {
        throw new NotImplementedException();
    }
}
