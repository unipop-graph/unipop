package org.unipop.process.properties;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by sbarzilay on 6/8/16.
 */
public interface PropertyFetcher {
    void addPropertyKey(String key);
    void fetchAllKeys();
    Set<String> getPropertyKeys();
}
