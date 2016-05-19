package org.unipop.common.util;

import org.json.JSONArray;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class ConversionUtils {

    public static <T> Stream<T> asStream(Iterator<T> sourceIterator) {
        return asStream(sourceIterator, false);
    }

    public static <T> Stream<T> asStream(Iterator<T> sourceIterator, boolean parallel) {
        Iterable<T> iterable = () -> sourceIterator;
        return StreamSupport.stream(iterable.spliterator(), parallel);
    }

    public static <T> Set<T> toSet(JSONArray jsonArray) {
        HashSet<T> hashSet = new HashSet<>(jsonArray.length());
        for(int i = 0; i < jsonArray.length(); i++)
            hashSet.add((T) jsonArray.get(i));
        return hashSet;
    }
}