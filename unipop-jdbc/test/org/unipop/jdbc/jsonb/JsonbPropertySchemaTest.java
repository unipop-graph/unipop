package org.unipop.jdbc.jsonb;

import org.junit.Test;
import org.unipop.jdbc.schemas.property.JsonbPropertySchema;

import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.*;

public class JsonbPropertySchemaTest {

    @Test
    public void toPropertiesExposesWholeDictAndDottedKeys() {
        JsonbPropertySchema schema = new JsonbPropertySchema("data");
        Map<String, Object> props = schema.toProperties(
                Collections.singletonMap("data",
                        "{\"status\":\"active\",\"address\":{\"city\":\"NYC\"}}"));

        assertTrue(props.get("data") instanceof Map);
        @SuppressWarnings("unchecked")
        Map<String, Object> whole = (Map<String, Object>) props.get("data");
        assertEquals("active", whole.get("status"));
        assertTrue(whole.get("address") instanceof Map);
        assertEquals("active", props.get("data.status"));
        @SuppressWarnings("unchecked")
        Map<String, Object> address = (Map<String, Object>) props.get("data.address");
        assertEquals("NYC", address.get("city"));
    }

    @Test
    public void toPropertiesEmptyForNonObjectJson() {
        JsonbPropertySchema schema = new JsonbPropertySchema("data");
        assertTrue(schema.toProperties(Collections.singletonMap("data", "[1,2]")).isEmpty());
        assertTrue(schema.toProperties(Collections.singletonMap("data", "\"x\"")).isEmpty());
    }

    @Test
    public void toPropertiesEmptyWhenColumnAbsent() {
        JsonbPropertySchema schema = new JsonbPropertySchema("data");
        assertTrue(schema.toProperties(Collections.emptyMap()).isEmpty());
    }
}
