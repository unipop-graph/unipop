package org.unipop.util;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.junit.Assert.*;

public class ConfigInterpolatorTest {

    /** Map-backed resolver (OS env vars can't be set inside a running JVM). */
    private static Function<String, String> env(String... kv) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2) m.put(kv[i], kv[i + 1]);
        return m::get;
    }

    @Test public void substitutesSetVar() {
        assertEquals("alice",
                ConfigInterpolator.interpolateString("${DB_USER}", env("DB_USER", "alice"), "cfg"));
    }

    @Test public void usesDefaultWhenUnset() {
        assertEquals("postgres",
                ConfigInterpolator.interpolateString("${DB_USER:-postgres}", env(), "cfg"));
    }

    @Test public void setVarBeatsDefault() {
        assertEquals("alice",
                ConfigInterpolator.interpolateString("${DB_USER:-postgres}", env("DB_USER", "alice"), "cfg"));
    }

    @Test public void emptyDefaultAllowed() {
        assertEquals("", ConfigInterpolator.interpolateString("${DB_USER:-}", env(), "cfg"));
    }

    @Test public void missingVarNoDefaultThrows() {
        try {
            ConfigInterpolator.interpolateString("${DB_PASSWORD}", env(), "myconfig.json");
            fail("expected IllegalStateException");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("DB_PASSWORD"));
            assertTrue(e.getMessage().contains("myconfig.json"));
        }
    }

    @Test public void multiplePlaceholdersInOneString() {
        String url = "jdbc:postgresql://${DB_HOST:-localhost}:${DB_PORT}/graph";
        assertEquals("jdbc:postgresql://localhost:5432/graph",
                ConfigInterpolator.interpolateString(url, env("DB_PORT", "5432"), "cfg"));
    }

    @Test public void malformedPlaceholderLeftLiteral() {
        assertEquals("${}", ConfigInterpolator.interpolateString("${}", env(), "cfg"));
        assertEquals("${1BAD}", ConfigInterpolator.interpolateString("${1BAD}", env(), "cfg"));
        assertEquals("${UNCLOSED", ConfigInterpolator.interpolateString("${UNCLOSED", env(), "cfg"));
    }

    @Test public void noPlaceholderPassthrough() {
        assertEquals("plain", ConfigInterpolator.interpolateString("plain", env(), "cfg"));
    }

    @Test public void walksNestedObjectsAndArrays() {
        JSONObject cfg = new JSONObject()
                .put("user", "${DB_USER:-postgres}")
                .put("port", 5432)
                .put("address", new JSONArray().put("jdbc://${DB_HOST:-localhost}:5432/g"))
                .put("nested", new JSONObject().put("password", "${DB_PW:-secret}"));
        ConfigInterpolator.interpolate(cfg, env(), "cfg");
        assertEquals("postgres", cfg.getString("user"));
        assertEquals(5432, cfg.getInt("port"));            // non-string untouched
        assertEquals("jdbc://localhost:5432/g", cfg.getJSONArray("address").getString(0));
        assertEquals("secret", cfg.getJSONObject("nested").getString("password"));
    }

    @Test public void publicOverloadUsesEnvWithDefault() {
        // Exercises the System.getenv path via a default so it needs no real env var.
        JSONObject cfg = new JSONObject().put("user", "${DEFINITELY_UNSET_VAR_XYZ:-fallback}");
        ConfigInterpolator.interpolate(cfg, "cfg");
        assertEquals("fallback", cfg.getString("user"));
    }
}
