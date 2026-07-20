package org.unipop.jdbc.enums;

import org.json.JSONObject;
import org.junit.BeforeClass;
import org.junit.Test;
import org.unipop.jdbc.schemas.property.EnumPropertySchema;
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
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

/**
 * Unit tests for {@link EnumPropertySchema.Builder} — no database needed. Locks in the config-claim
 * and enum-type-name validation behavior carried over from the old AbstractRowSchema parsing.
 */
public class EnumPropertySchemaTest {

    private final EnumPropertySchema.Builder builder = new EnumPropertySchema.Builder();

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

    @Test
    public void buildsWithValidEnumType() {
        PropertySchema schema = builder.build("status",
                new JSONObject().put("type", "enum").put("enumType", "mood"), null);
        assertNotNull(schema);
        assertTrue(schema instanceof EnumPropertySchema);
        EnumPropertySchema enumSchema = (EnumPropertySchema) schema;
        assertEquals("status", enumSchema.getKey());
        assertEquals("status", enumSchema.getEnumColumn());
        assertEquals("mood", enumSchema.getEnumType());
    }

    @Test
    public void buildsWithoutEnumType() {
        PropertySchema schema = builder.build("status", new JSONObject().put("type", "enum"), null);
        assertNotNull(schema);
        assertNull(((EnumPropertySchema) schema).getEnumType());
    }

    @Test
    public void stripsLeadingAtFromFieldOverride() {
        PropertySchema schema = builder.build("status",
                new JSONObject().put("type", "enum").put("field", "@mood_col"), null);
        assertEquals("mood_col", ((EnumPropertySchema) schema).getEnumColumn());
        assertEquals("status", ((EnumPropertySchema) schema).getKey());
    }

    @Test(expected = IllegalArgumentException.class)
    public void rejectsInvalidEnumTypeName() {
        builder.build("status", new JSONObject().put("type", "enum").put("enumType", "bad name!"), null);
    }

    @Test
    public void returnsNullForNonEnumConfig() {
        assertNull(builder.build("status", new JSONObject().put("type", "jsonb"), null));
        assertNull(builder.build("status", new JSONObject().put("field", "status"), null));
        assertNull(builder.build("status", "@status", null));
    }

    @Test
    public void valuesFormClosedKnownDomain() {
        PropertySchema schema = builder.build("label",
                new JSONObject()
                        .put("type", "enum")
                        .put("field", "label")
                        .put("enumType", "element_label")
                        .put("values", new org.json.JSONArray().put("person").put("software")),
                null);
        EnumPropertySchema enumSchema = (EnumPropertySchema) schema;
        Set<Object> known = enumSchema.knownValues();
        assertEquals(new HashSet<>(Arrays.asList("person", "software")), known);
        assertFalse(known.isEmpty());
    }

    @Test
    public void includeAcceptedAsValuesAlias() {
        PropertySchema schema = builder.build("label",
                new JSONObject()
                        .put("type", "enum")
                        .put("include", new org.json.JSONArray().put("host")),
                null);
        assertEquals(Collections.singleton("host"), ((EnumPropertySchema) schema).knownValues());
    }

    @Test
    public void bareEnumHasEmptyKnownValues() {
        PropertySchema schema = builder.build("status", new JSONObject().put("type", "enum"), null);
        assertTrue(((EnumPropertySchema) schema).knownValues().isEmpty());
    }
}
