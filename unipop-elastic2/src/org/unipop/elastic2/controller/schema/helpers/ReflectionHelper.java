package org.unipop.elastic2.controller.schema.helpers;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * Created by Gilad on 12/10/2015.
 */
public class ReflectionHelper {
    public static <T> T createNew(String className, Object... parameters) throws Exception{
        int indexOf$ = className.indexOf("$");

        ArrayList<Object> newParamaters = new ArrayList<>(Arrays.asList(parameters));
        if (indexOf$ > 0) {
            String outerClassName = className.substring(0, indexOf$);
            newParamaters.add(0, Class.forName(outerClassName).getConstructor().newInstance());
        }

        ArrayList<Class> parameterTypes = new ArrayList<>();
        for(Object param : newParamaters) {
            parameterTypes.add(param.getClass());
        }

        return (T)(Class.forName(className).getConstructor(parameterTypes.stream().toArray(Class[]::new))
                .newInstance(newParamaters.stream().toArray(Object[]::new)));
    }
}
