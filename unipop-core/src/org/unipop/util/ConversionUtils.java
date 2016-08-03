package org.unipop.util;

import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.*;
import java.util.function.BiFunction;
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

    public static <T> Set<T> toSet(JSONObject config, String key) {
        JSONArray objects = config.optJSONArray(key);
        if (objects != null) {
            HashSet<T> hashSet = new HashSet<>(objects.length());
            for (int i = 0; i < objects.length(); i++)
                hashSet.add((T) objects.get(i));
            return hashSet;
        }
        Object opt = config.opt(key);
        if (opt != null) return Collections.singleton((T) opt);
        return Collections.EMPTY_SET;
    }

    public static Map<String, Object> asMap(Object[] keyValues){
        Map<String, Object> map = new HashMap<>();
        if (keyValues != null) {
            ElementHelper.legalPropertyKeyValueArray(keyValues);
            for (int i = 0; i < keyValues.length; i = i + 2) {
                String key = keyValues[i].toString();
                Object value = keyValues[i + 1];
                ElementHelper.validateProperty(key,value);
                if (map.containsKey(key)){
                    Object o = map.get(key);
                    List list = o instanceof List ? ((List) o) : new ArrayList<Object>(){{add(o);}};
                    list.add(value);
                    value = list;
                }
                map.put(key, value);
            }
        }
        return map;
    }

    public static List<JSONObject> getList(JSONObject json, String key) {
        JSONArray objectsArray = json.optJSONArray(key);
        if(objectsArray == null) return Collections.emptyList();

        List<JSONObject> objects = new ArrayList<>();
        for(int i = 0; i < objectsArray.length(); i++){
            JSONObject config = objectsArray.getJSONObject(i);
            objects.add(config);
        }
        return objects;
    }

    public static List<String> toStringList(JSONObject config, String key) {
        JSONArray addressesConfiguration = config.optJSONArray(key);
        if (addressesConfiguration != null) {
            List<String> addresses = new ArrayList<>();
            for (int i = 0; i < addressesConfiguration.length(); i++) {
                String address = addressesConfiguration.getString(i);
                addresses.add(address);
            }
            return addresses;
        }
        else
            return Collections.singletonList(config.getString(key));
    }

    public static <K, V> Map<K, V> merge(List<Map<K, V>> maps, BiFunction<? super V, ? super V, ? extends V> mergeFunc, Boolean ignoreNull) {
        Map<K, V> newMap = new HashMap<>(maps.size());
        for(Map<K, V> current : maps) {
            if(current == null) {
                if (!ignoreNull) return null; //a null results indicates to cancel the merge.
                continue;
            }
            current.forEach((fieldKey, fieldValue) -> newMap.merge(fieldKey, fieldValue, mergeFunc));
        }
        return newMap;
    }
}