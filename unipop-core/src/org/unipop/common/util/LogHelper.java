package org.unipop.common.util;

import java.util.Collection;
import java.util.stream.Collectors;

/**
 * @author Gur Ronen
 * @since 6/26/2016.
 */
public final class LogHelper {
    public static String formatCollection(Collection<?> coll) {
        return coll.stream().map(Object::toString).collect(Collectors.joining(", ", "{ ", " }"));
    }
}
