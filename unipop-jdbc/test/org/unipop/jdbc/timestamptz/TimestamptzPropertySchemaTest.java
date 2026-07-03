package org.unipop.jdbc.timestamptz;

import org.json.JSONObject;
import org.junit.BeforeClass;
import org.junit.Test;
import org.unipop.jdbc.schemas.property.TimestamptzPropertySchema;
import org.unipop.schema.property.PropertySchema;
import org.unipop.schema.property.type.DateType;
import org.unipop.schema.property.type.DoubleType;
import org.unipop.schema.property.type.FloatType;
import org.unipop.schema.property.type.IntType;
import org.unipop.schema.property.type.LongType;
import org.unipop.schema.property.type.NumberType;
import org.unipop.schema.property.type.TextType;
import org.unipop.util.PropertyTypeFactory;

import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Map;

import static org.junit.Assert.*;

public class TimestamptzPropertySchemaTest {

    private static final OffsetDateTime ODT = OffsetDateTime.parse("2020-08-02T00:00:00Z");

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

    @Test public void coerceOffsetDateTimePassthrough() {
        assertSame(ODT, TimestamptzPropertySchema.toOffsetDateTime(ODT));
    }

    @Test public void coerceInstant() {
        assertEquals(ODT, TimestamptzPropertySchema.toOffsetDateTime(ODT.toInstant()));
    }

    @Test public void coerceUtilDate() {
        assertEquals(ODT, TimestamptzPropertySchema.toOffsetDateTime(Date.from(ODT.toInstant())));
    }

    @Test public void coerceSqlTimestamp() {
        assertEquals(ODT, TimestamptzPropertySchema.toOffsetDateTime(java.sql.Timestamp.from(ODT.toInstant())));
    }

    @Test public void coerceIsoStringWithOffset() {
        assertEquals(ODT, TimestamptzPropertySchema.toOffsetDateTime("2020-08-02T00:00:00Z"));
    }

    @Test public void coerceIsoStringWithoutOffsetAssumedUtc() {
        assertEquals(ODT, TimestamptzPropertySchema.toOffsetDateTime("2020-08-02T00:00:00"));
    }

    @Test(expected = IllegalArgumentException.class) public void coerceBareLongRejected() {
        TimestamptzPropertySchema.toOffsetDateTime(1596326400000L);
    }

    @Test(expected = IllegalArgumentException.class) public void coerceNonParseableStringThrows() {
        TimestamptzPropertySchema.toOffsetDateTime("not-a-date");
    }

    @Test public void toFieldsCoercesStringToOffsetDateTime() {
        TimestamptzPropertySchema s = new TimestamptzPropertySchema("at", "at", true);
        Map<String, Object> f = s.toFields(Collections.singletonMap("at", "2020-08-02T00:00:00Z"));
        assertEquals(ODT, f.get("at"));
        assertTrue(f.get("at") instanceof OffsetDateTime);
    }

    @Test public void toPropertiesCoercesTimestampToOffsetDateTime() {
        TimestamptzPropertySchema s = new TimestamptzPropertySchema("at", "at", true);
        Map<String, Object> p = s.toProperties(Collections.singletonMap("at", java.sql.Timestamp.from(ODT.toInstant())));
        assertEquals(ODT, p.get("at"));
        assertTrue(p.get("at") instanceof OffsetDateTime);
    }

    @Test public void toFieldsEmptyWhenAbsentAndNullable() {
        TimestamptzPropertySchema s = new TimestamptzPropertySchema("at", "at", true);
        assertTrue(s.toFields(Collections.emptyMap()).isEmpty());
    }

    @Test public void builderMatches() {
        PropertySchema s = new TimestamptzPropertySchema.Builder()
                .build("at", new JSONObject().put("type", "timestamptz"), null);
        assertNotNull(s);
        assertTrue(s instanceof TimestamptzPropertySchema);
        assertEquals("at", ((TimestamptzPropertySchema) s).getTimestamptzColumn());
    }

    @Test public void builderHonorsFieldAlias() {
        PropertySchema s = new TimestamptzPropertySchema.Builder()
                .build("at", new JSONObject().put("type", "timestamptz").put("field", "@at_col"), null);
        assertEquals("at_col", ((TimestamptzPropertySchema) s).getTimestamptzColumn());
    }

    @Test public void builderIgnoresOtherTypesAndNonObjects() {
        assertNull(new TimestamptzPropertySchema.Builder().build("x", new JSONObject().put("type", "uuid"), null));
        assertNull(new TimestamptzPropertySchema.Builder().build("x", "plainstring", null));
    }
}
