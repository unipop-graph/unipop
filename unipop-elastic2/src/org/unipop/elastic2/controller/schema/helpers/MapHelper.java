package org.unipop.elastic2.controller.schema.helpers;

import java.util.*;

/**
 * Created by Gilad on 14/10/2015.
 */
public class MapHelper {
    //region Public Methods
    static public <T> T value(Map<String, Object> map, String key) {
        List<T> values = values(map, key);
        if (values.size() == 0) {
            return null;
        } else {
            return values.get(0);
        }
    }

    static public <T> List<T> values(Map<String, Object> map, String key) {
        T val = (T)map.get(key);
        if (val != null) {
            if (List.class.isAssignableFrom(val.getClass())) {
                return (List) val;
            } else {
                return Arrays.asList(val);
            }
        }

        if (key.indexOf(".") > 0) {
            String[] path = key.split("\\.");
            return values(map, path, 0);
        }

        return Collections.emptyList();
    }

    static public boolean containsKey(Map<String, Object> map, String key) {
        boolean mapContainsKey = map.containsKey(key);
        if (mapContainsKey) {
            return true;
        }

        if (key.indexOf(".") > 0) {
            String[] path = key.split("\\.");
            return containsKey(map, path, 0);
        }

        return false;
    }
    //endregion


    //region Private Methods
    static private <T> List<T> values(Map<String, Object> map, String[] path, int pathIndex) {
        Object mapValue = null;
        for (int index = pathIndex; index < path.length; index++) {
            mapValue = map.get(path[index]);
            if (mapValue == null) {
                return Collections.emptyList();
            }

            if (Map.class.isAssignableFrom(mapValue.getClass())) {
                map = (Map<String, Object>) mapValue;
            } else {
                if (List.class.isAssignableFrom(mapValue.getClass())) {
                    List list = (List) mapValue;
                    if (list.size() > 0 && Map.class.isAssignableFrom(list.get(0).getClass())) {
                        final int pathIndexRec = index + 1;

                        List<T> resultList = new ArrayList<>();
                        for(Object o : list) {
                            resultList.addAll(values((Map<String, Object>) o, path, pathIndexRec));
                        }
                        return resultList;
                    } else {
                        if (index == path.length - 1) {
                            return list;
                        }
                    }
                }

                if (index < path.length - 1) {
                    return Collections.emptyList();
                }
            }
        }

        return Arrays.asList((T) mapValue);
    }

    static private boolean containsKey(Map<String, Object> map, String[] path, int pathIndex) {
        Object mapValue = null;
        for (int index = pathIndex; index < path.length; index++) {
            mapValue = map.get(path[index]);
            if (mapValue == null) {
                return false;
            }

            if (Map.class.isAssignableFrom(mapValue.getClass())) {
                map = (Map<String, Object>) mapValue;
            } else {
                if (List.class.isAssignableFrom(mapValue.getClass())) {
                    for (Object obj : (List) mapValue) {
                        if (Map.class.isAssignableFrom(obj.getClass())) {
                            if (containsKey((Map<String, Object>) obj, path, index + 1)) {
                                return true;
                            }
                        }
                    }
                }

                if (index < path.length - 1) {
                    return false;
                }
            }
        }

        return true;
    }
    //endregion
}
