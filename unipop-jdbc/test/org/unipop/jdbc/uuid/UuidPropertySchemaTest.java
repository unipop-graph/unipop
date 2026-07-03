package org.unipop.jdbc.uuid;

import org.json.JSONObject;
import org.junit.BeforeClass;
import org.junit.Test;
import org.unipop.jdbc.schemas.property.UuidPropertySchema;
import org.unipop.schema.property.PropertySchema;
import org.unipop.schema.property.type.DateType;
import org.unipop.schema.property.type.DoubleType;
import org.unipop.schema.property.type.FloatType;
import org.unipop.schema.property.type.IntType;
import org.unipop.schema.property.type.LongType;
import org.unipop.schema.property.type.NumberType;
import org.unipop.schema.property.type.TextType;
import org.unipop.util.PropertyTypeFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.*;

public class UuidPropertySchemaTest {

    private static final String S = "f47ac10b-58cc-4372-a567-0f02b2f3d479";

    @BeforeClass
    public static void initPropertyTypes() {
        // FieldPropertySchema's constructor resolves its PropertyType from PropertyTypeFactory, which
        // UniGraph populates at graph bootstrap. Replicate that init so the builder works standalone.
        PropertyTypeFactory.init(Arrays.asList(
                TextType.class.getCanonicalName(),
                DateType.class.getCanonicalName(),
                NumberType.class.getCanonicalName(),
                DoubleType.class.getCanonicalName(),
                FloatType.class.getCanonicalName(),
                IntType.class.getCanonicalName(),
                LongType.class.getCanonicalName()));
    }

    @Test public void toFieldsCoercesStringToUuid() {
        UuidPropertySchema schema = new UuidPropertySchema("ref", "ref", true);
        Map<String, Object> fields = schema.toFields(Collections.singletonMap("ref", S));
        assertEquals(UUID.fromString(S), fields.get("ref"));
        assertTrue(fields.get("ref") instanceof UUID);
    }

    @Test public void toFieldsPassesThroughUuid() {
        UuidPropertySchema schema = new UuidPropertySchema("ref", "ref", true);
        UUID u = UUID.fromString(S);
        Map<String, Object> fields = schema.toFields(Collections.singletonMap("ref", u));
        assertSame(u, fields.get("ref"));
    }

    @Test public void toFieldsEmptyWhenAbsentAndNullable() {
        UuidPropertySchema schema = new UuidPropertySchema("ref", "ref", true);
        assertTrue(schema.toFields(Collections.emptyMap()).isEmpty());
    }

    @Test public void toPropertiesCoercesStringToUuid() {
        // simulates a fetch that returned the uuid column as a String
        UuidPropertySchema schema = new UuidPropertySchema("ref", "ref", true);
        Map<String, Object> props = schema.toProperties(Collections.singletonMap("ref", S));
        assertEquals(UUID.fromString(S), props.get("ref"));
        assertTrue(props.get("ref") instanceof UUID);
    }

    @Test public void toPropertiesPassesThroughUuid() {
        UuidPropertySchema schema = new UuidPropertySchema("ref", "ref", true);
        UUID u = UUID.fromString(S);
        Map<String, Object> props = schema.toProperties(Collections.singletonMap("ref", u));
        assertSame(u, props.get("ref"));
    }

    @Test public void builderMatchesUuidType() {
        PropertySchema s = new UuidPropertySchema.Builder()
                .build("ref", new JSONObject().put("type", "uuid"), null);
        assertNotNull(s);
        assertTrue(s instanceof UuidPropertySchema);
        assertEquals("ref", ((UuidPropertySchema) s).getUuidColumn());
    }

    @Test public void builderHonorsFieldAlias() {
        PropertySchema s = new UuidPropertySchema.Builder()
                .build("ref", new JSONObject().put("type", "uuid").put("field", "@ref_col"), null);
        assertEquals("ref_col", ((UuidPropertySchema) s).getUuidColumn());
    }

    @Test public void builderIgnoresOtherTypesAndNonObjects() {
        assertNull(new UuidPropertySchema.Builder().build("x", new JSONObject().put("type", "enum"), null));
        assertNull(new UuidPropertySchema.Builder().build("x", "plainstring", null));
    }
}
