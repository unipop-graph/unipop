package org.unipop.util;

import org.json.JSONArray;
import org.json.JSONObject;

import java.util.function.Function;

/**
 * Interpolates {@code ${VAR}} / {@code ${VAR:-default}} placeholders in provider config JSON,
 * resolving from environment variables. Applied once when a config is loaded, so every provider
 * benefits. A config with no placeholders is returned unchanged.
 */
public final class ConfigInterpolator {

    private ConfigInterpolator() {
    }

    /** Interpolate all string values in {@code config} (in place) using OS environment variables. */
    public static void interpolate(JSONObject config, String sourceName) {
        interpolate(config, System::getenv, sourceName);
    }

    static void interpolate(JSONObject config, Function<String, String> resolver, String sourceName) {
        for (String key : config.keySet()) {
            Object value = config.get(key);
            if (value instanceof String) {
                config.put(key, interpolateString((String) value, resolver, sourceName));
            } else if (value instanceof JSONObject) {
                interpolate((JSONObject) value, resolver, sourceName);
            } else if (value instanceof JSONArray) {
                interpolate((JSONArray) value, resolver, sourceName);
            }
        }
    }

    private static void interpolate(JSONArray array, Function<String, String> resolver, String sourceName) {
        for (int i = 0; i < array.length(); i++) {
            Object value = array.get(i);
            if (value instanceof String) {
                array.put(i, interpolateString((String) value, resolver, sourceName));
            } else if (value instanceof JSONObject) {
                interpolate((JSONObject) value, resolver, sourceName);
            } else if (value instanceof JSONArray) {
                interpolate((JSONArray) value, resolver, sourceName);
            }
        }
    }

    static String interpolateString(String value, Function<String, String> resolver, String sourceName) {
        if (value.indexOf("${") < 0) {
            return value;                                   // fast path: no placeholder
        }
        StringBuilder out = new StringBuilder();
        int i = 0;
        while (i < value.length()) {
            int open = value.indexOf("${", i);
            if (open < 0) {
                out.append(value, i, value.length());
                break;
            }
            int close = value.indexOf('}', open + 2);
            if (close < 0) {
                out.append(value, i, value.length());       // no closing brace -> rest is literal
                break;
            }
            out.append(value, i, open);                     // literal text before "${"
            String body = value.substring(open + 2, close);
            String raw = value.substring(open, close + 1);  // "${...}" as written
            out.append(resolve(body, resolver, sourceName, raw));
            i = close + 1;
        }
        return out.toString();
    }

    private static String resolve(String body, Function<String, String> resolver, String sourceName, String raw) {
        int sep = body.indexOf(":-");
        String name = sep >= 0 ? body.substring(0, sep) : body;
        String def = sep >= 0 ? body.substring(sep + 2) : null;
        if (!isValidName(name)) {
            return raw;                                     // not a well-formed placeholder -> literal
        }
        String resolved = resolver.apply(name);
        if (resolved != null) {
            return resolved;
        }
        if (def != null) {
            return def;
        }
        throw new IllegalStateException("Unresolved environment variable ${" + name + "} in config "
                + sourceName + " (set it, or provide a default via ${" + name + ":-...})");
    }

    private static boolean isValidName(String name) {
        if (name.isEmpty()) {
            return false;
        }
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            boolean ok = (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_'
                    || (i > 0 && c >= '0' && c <= '9');
            if (!ok) {
                return false;
            }
        }
        return true;
    }
}
