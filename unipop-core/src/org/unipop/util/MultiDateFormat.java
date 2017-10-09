package org.unipop.util;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.text.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by sbarzilay on 8/19/16.
 */
public class MultiDateFormat extends DateFormat {
    private List<SimpleDateFormat> formats;

    public MultiDateFormat(String format, Collection<String> formats) {
        this.formats = new ArrayList<>();
        this.formats.add(new SimpleDateFormat(format));
        this.formats.addAll(formats.stream().map(SimpleDateFormat::new).collect(Collectors.toList()));
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