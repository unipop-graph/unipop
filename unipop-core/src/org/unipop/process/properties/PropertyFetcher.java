package org.unipop.process.properties;

import java.util.Set;

/**
 * Created by sbarzilay on 6/8/16.
 */
public interface PropertyFetcher {
    default void addPropertyKey(String key){
        if (getPropertyKeys() == null)
            return;
        this.getPropertyKeys().add(key);
    }
    void fetchAllKeys();
    Set<String> getPropertyKeys();
}
