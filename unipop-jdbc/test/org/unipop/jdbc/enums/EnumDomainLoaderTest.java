package org.unipop.jdbc.enums;

import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.unipop.jdbc.schemas.RowVertexSchema;
import org.unipop.jdbc.schemas.property.EnumPropertySchema;
import org.unipop.jdbc.suite.EmbeddedPostgresServer;
import org.unipop.jdbc.utils.ContextManager;
import org.unipop.jdbc.utils.EnumDomainLoader;
import org.unipop.schema.property.PropertySchema;
import org.unipop.schema.property.type.DateType;
import org.unipop.schema.property.type.DoubleType;
import org.unipop.schema.property.type.FloatType;
import org.unipop.schema.property.type.IntType;
import org.unipop.schema.property.type.LongType;
import org.unipop.schema.property.type.NumberType;
import org.unipop.schema.property.type.TextType;
import org.unipop.util.PropertySchemaFactory;
import org.unipop.util.PropertyTypeFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Verifies Postgres catalog introspection fills {@link EnumPropertySchema#knownValues()}.
 */
public class EnumDomainLoaderTest {

    private static ContextManager contextManager;

    @BeforeClass
    public static void setUp() throws Exception {
        PropertyTypeFactory.init(Arrays.asList(
                TextType.class.getCanonicalName(),
                DateType.class.getCanonicalName(),
                NumberType.class.getCanonicalName(),
                DoubleType.class.getCanonicalName(),
                FloatType.class.getCanonicalName(),
                IntType.class.getCanonicalName(),
                LongType.class.getCanonicalName()));
        PropertySchemaFactory.build(
                Collections.singletonList(new EnumPropertySchema.Builder()),
                Collections.emptyList());

        EmbeddedPostgresServer.ensureStarted();
        Class.forName("org.postgresql.Driver");
        try (Connection c = DriverManager.getConnection(EmbeddedPostgresServer.URL, EmbeddedPostgresServer.USER, "");
             Statement s = c.createStatement()) {
            s.execute("DROP TABLE IF EXISTS enum_domain_host");
            s.execute("DROP TYPE IF EXISTS host_kind");
            s.execute("CREATE TYPE host_kind AS ENUM ('physical','virtual','container')");
            s.execute("CREATE TABLE enum_domain_host (id varchar(100) primary key, label host_kind, name varchar(100))");
        }

        JSONObject conf = new JSONObject()
                .put("driver", "org.postgresql.Driver")
                .put("sqlDialect", "POSTGRES")
                .put("address", new org.json.JSONArray().put(EmbeddedPostgresServer.URL))
                .put("user", EmbeddedPostgresServer.USER)
                .put("password", "");
        contextManager = new ContextManager(conf);
    }

    @AfterClass
    public static void tearDown() {
        if (contextManager != null) contextManager.close();
    }

    @Test
    public void loadsMembersByEnumTypeName() {
        Set<Object> labels = EnumDomainLoader.loadByTypeName(contextManager, "host_kind");
        assertEquals(new HashSet<>(Arrays.asList("physical", "virtual", "container")), labels);
    }

    @Test
    public void loadsMembersByTableColumn() {
        Set<Object> labels = EnumDomainLoader.loadByTableColumn(contextManager, "enum_domain_host", "label");
        assertEquals(new HashSet<>(Arrays.asList("physical", "virtual", "container")), labels);
    }

    @Test
    public void hydrateFillsSchemaWithoutConfigValues() {
        // label is enum with type name only — no values/include in config
        JSONObject vertexJson = new JSONObject()
                .put("table", "enum_domain_host")
                .put("id", "@id")
                .put("label", new JSONObject().put("type", "enum").put("field", "label").put("enumType", "host_kind"))
                .put("properties", new JSONObject().put("name", "@name"))
                .put("dynamicProperties", false);

        RowVertexSchema schema = new RowVertexSchema(vertexJson, null);
        EnumPropertySchema label = findLabelEnum(schema);
        assertTrue("precondition: domain empty before hydrate", label.knownValues().isEmpty());

        EnumDomainLoader.hydrate(contextManager, Collections.singleton(schema));

        assertEquals(new HashSet<>(Arrays.asList("physical", "virtual", "container")),
                label.knownValues());
    }

    @Test
    public void hydrateUsesTableColumnWhenEnumTypeOmitted() {
        JSONObject vertexJson = new JSONObject()
                .put("table", "enum_domain_host")
                .put("id", "@id")
                .put("label", new JSONObject().put("type", "enum").put("field", "label"))
                .put("properties", new JSONObject())
                .put("dynamicProperties", false);

        RowVertexSchema schema = new RowVertexSchema(vertexJson, null);
        EnumPropertySchema label = findLabelEnum(schema);
        EnumDomainLoader.hydrate(contextManager, Collections.singleton(schema));
        assertEquals(new HashSet<>(Arrays.asList("physical", "virtual", "container")),
                label.knownValues());
    }

    @Test
    public void configValuesWinOverDatabase() {
        JSONObject vertexJson = new JSONObject()
                .put("table", "enum_domain_host")
                .put("id", "@id")
                .put("label", new JSONObject()
                        .put("type", "enum")
                        .put("field", "label")
                        .put("enumType", "host_kind")
                        .put("values", new org.json.JSONArray().put("only_config")))
                .put("properties", new JSONObject())
                .put("dynamicProperties", false);

        RowVertexSchema schema = new RowVertexSchema(vertexJson, null);
        EnumPropertySchema label = findLabelEnum(schema);
        EnumDomainLoader.hydrate(contextManager, Collections.singleton(schema));
        assertEquals(Collections.singleton("only_config"), label.knownValues());
    }

    /** Label property key is T.label.getAccessor() → "~label". */
    private static EnumPropertySchema findLabelEnum(RowVertexSchema schema) {
        for (PropertySchema p : schema.getPropertySchemas()) {
            if (p instanceof EnumPropertySchema) {
                return (EnumPropertySchema) p;
            }
        }
        throw new AssertionError("no EnumPropertySchema on schema; props="
                + schema.getPropertySchemas().stream().map(PropertySchema::getKey).toList());
    }
}
