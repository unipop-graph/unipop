package org.unipop.process.properties;

import java.util.Set;

public interface PropertyFetcher {
    void addPropertyKey(String key);
    void fetchAllKeys();
    Set<String> getKeys();
}
