package org.unipop.schema.catalog;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.json.JSONObject;
import org.junit.BeforeClass;
import org.junit.Test;
import org.unipop.schema.element.AbstractElementSchema;
import org.unipop.schema.element.EdgeSchema;
import org.unipop.schema.element.ElementSchema;
import org.unipop.schema.element.VertexSchema;
import org.unipop.schema.property.type.DateType;
import org.unipop.schema.property.type.DoubleType;
import org.unipop.schema.property.type.FloatType;
import org.unipop.schema.property.type.IntType;
import org.unipop.schema.property.type.LongType;
import org.unipop.schema.property.type.NumberType;
import org.unipop.schema.property.type.TextType;
import org.unipop.schema.reference.ReferenceVertexSchema;
import org.unipop.util.PropertySchemaFactory;
import org.unipop.util.PropertyTypeFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SchemaCatalogTest {

    @BeforeClass
    public static void initPropertyTypes() {
        PropertyTypeFactory.init(Arrays.asList(
                TextType.class.getCanonicalName(),
                DateType.class.getCanonicalName(),
                NumberType.class.getCanonicalName(),
                DoubleType.class.getCanonicalName(),
                FloatType.class.getCanonicalName(),
                IntType.class.getCanonicalName(),
                LongType.class.getCanonicalName()));
        PropertySchemaFactory.build(Collections.emptyList(), Collections.emptyList());
    }

    @Test
    public void closedLabelsIndexVertexAndEdgeTypes() {
        FakeVertex host = vertex("host", "status", "name");
        FakeVertex person = vertex("person", "status", "name");
        FakeEdge owns = edge("owns", ref("@src_id", "person"), ref("@dst_id", "host"), true);

        SchemaCatalog catalog = catalogOf(host, person, owns);

        assertEquals(1, catalog.vertexSchemas(Collections.singleton("host")).size());
        assertTrue(catalog.vertexSchemas(Collections.singleton("host")).contains(host));
        assertEquals(1, catalog.vertexSchemas(Collections.singleton("person")).size());
        assertTrue(catalog.edgeSchemas(Collections.singleton("owns")).contains(owns));
        assertTrue(catalog.canJoin(owns));
    }

    @Test
    public void openLabelTypesAlwaysIncluded() {
        FakeVertex openV = openLabelVertex("status");
        FakeVertex host = vertex("host", "name");
        SchemaCatalog catalog = catalogOf(openV, host);

        Set<ElementSchema> forHost = catalog.vertexSchemas(Collections.singleton("host"));
        assertTrue(forHost.contains(host));
        assertTrue("open-label vtype must not be pruned", forHost.contains(openV));
    }

    @Test
    public void includeListOnLabelIsClosedDomain() {
        // Field label with include — same pattern as jdbc inneredge modern config.
        FakeVertex personOnly = fieldLabelVertex(Collections.singleton("person"), "name");
        FakeVertex softwareOnly = fieldLabelVertex(Collections.singleton("software"), "lang");
        SchemaCatalog catalog = catalogOf(personOnly, softwareOnly);

        assertEquals(Collections.singleton(personOnly), catalog.vertexSchemas(Collections.singleton("person")));
        assertEquals(Collections.singleton(softwareOnly), catalog.vertexSchemas(Collections.singleton("software")));
        assertFalse(catalog.vertexSchemas(Collections.singleton("person")).contains(softwareOnly));
    }

    @Test
    public void knownValuesOnLabelClosesJoinTargets() {
        FakeVertex person = fieldLabelVertex(Collections.singleton("person"), "name");
        FakeVertex host = fieldLabelVertex(Collections.singleton("host"), "status");
        FakeEdge owns = edge("owns",
                new ReferenceVertexSchema(endpointJsonWithInclude("@src", Collections.singleton("person")), null),
                new ReferenceVertexSchema(endpointJsonWithInclude("@dst", Collections.singleton("host")), null),
                true);

        SchemaCatalog catalog = catalogOf(person, host, owns);
        assertEquals(Collections.singleton(host), catalog.joinTargets(owns, Direction.OUT, Collections.emptySet()));
        assertEquals(Collections.singleton(person), catalog.joinTargets(owns, Direction.IN, Collections.emptySet()));
    }

    @Test
    public void joinTargetsUseClosedEndpointLinks() {
        FakeVertex host = vertex("host", "status", "name");
        FakeVertex person = vertex("person", "status", "name");
        FakeEdge owns = edge("owns",
                new ReferenceVertexSchema(endpointJson("@src_id", "person"), null),
                new ReferenceVertexSchema(endpointJson("@dst_id", "host"), null),
                true);

        SchemaCatalog catalog = catalogOf(host, person, owns);

        Set<ElementSchema> outTargets = catalog.joinTargets(owns, Direction.OUT, Collections.emptySet());
        assertEquals(Collections.singleton(host), outTargets);

        Set<ElementSchema> inTargets = catalog.joinTargets(owns, Direction.IN, Collections.emptySet());
        assertEquals(Collections.singleton(person), inTargets);

        Set<ElementSchema> outHostOnly = catalog.joinTargets(owns, Direction.OUT, Collections.singleton("host"));
        assertEquals(Collections.singleton(host), outHostOnly);

        Set<ElementSchema> outPersonFilter = catalog.joinTargets(owns, Direction.OUT, Collections.singleton("person"));
        assertTrue("target label filter should drop non-matching closed vtypes", outPersonFilter.isEmpty());
    }

    @Test
    public void joinTargetsIncludeSameSourceOpenLabelVtype() {
        FakeVertex host = vertex("host", "status", "name");
        FakeVertex person = vertex("person", "status", "name");
        FakeVertex anything = openLabelVertex("status"); // open-label vtype, same source
        FakeEdge owns = edge("owns",
                new ReferenceVertexSchema(endpointJson("@src_id", "person"), null),
                new ReferenceVertexSchema(endpointJson("@dst_id", "host"), null),
                true);

        SchemaCatalog catalog = catalogOf(host, person, anything, owns);

        // OUT hop → closed endpoint "host"; the open-label vtype can also hold host-labeled rows,
        // so it must survive pruning alongside the closed-linked host.
        Set<ElementSchema> outTargets = catalog.joinTargets(owns, Direction.OUT, Collections.emptySet());
        assertTrue("closed-linked host present", outTargets.contains(host));
        assertTrue("same-source open-label vtype must not be pruned", outTargets.contains(anything));
        assertFalse("person is the OUT source, not an OUT target", outTargets.contains(person));
    }

    @Test
    public void openEndpointFallsBackToSameSourceVertices() {
        FakeVertex host = vertex("host", "status");
        FakeVertex person = vertex("person", "status");
        FakeEdge knows = edge("knows",
                new ReferenceVertexSchema(endpointJsonOpen("@outid"), null),
                new ReferenceVertexSchema(endpointJsonOpen("@inid"), null),
                true);

        SchemaCatalog catalog = catalogOf(host, person, knows);

        Set<ElementSchema> targets = catalog.joinTargets(knows, Direction.OUT, Collections.emptySet());
        assertEquals(2, targets.size());
        assertTrue(targets.contains(host));
        assertTrue(targets.contains(person));
    }

    @Test
    public void supportsJoinFalseWhenContributorSaysSo() {
        FakeVertex host = vertex("host");
        FakeEdge inner = edge("embedded",
                new ReferenceVertexSchema(endpointJson("@o", "host"), null),
                new ReferenceVertexSchema(endpointJson("@i", "host"), null),
                false);

        SchemaCatalog catalog = new SchemaCatalog();
        catalog.rebuild(Collections.singletonList(contributor(false, host, inner)));
        assertFalse(catalog.canJoin(inner));
    }

    @Test
    public void hasPropertyRespectsDeclaredKeysAndDynamic() {
        FakeVertex host = vertex("host", "status", "name");
        FakeVertex dyn = dynamicVertex("blob");
        SchemaCatalog catalog = catalogOf(host, dyn);

        assertTrue(catalog.hasProperty(host, "status"));
        assertFalse(catalog.hasProperty(host, "missing"));
        assertTrue(catalog.hasProperty(dyn, "anything"));
    }

    @Test
    public void sameSourceTrueWithinContribution() {
        FakeVertex host = vertex("host");
        FakeEdge owns = edge("owns",
                new ReferenceVertexSchema(endpointJson("@s", "host"), null),
                new ReferenceVertexSchema(endpointJson("@d", "host"), null),
                true);
        SchemaCatalog catalog = catalogOf(host, owns);
        assertTrue(catalog.sameSource(host, owns));
    }

    @Test
    public void filterByLabelsDropsClosedMismatches() {
        FakeVertex host = vertex("host");
        FakeVertex person = vertex("person");
        SchemaCatalog catalog = catalogOf(host, person);
        Set<ElementSchema> onlyHost = catalog.filterByLabels(Arrays.asList(host, person), Collections.singleton("host"));
        assertEquals(Collections.singleton(host), onlyHost);
    }

    @Test
    public void canPushOrderRespectsDeclaredProperties() {
        FakeVertex host = vertex("host", "status", "name");
        SchemaCatalog catalog = catalogOf(host);
        assertTrue(catalog.canPushOrder(host, Arrays.asList("status", "name")));
        assertFalse(catalog.canPushOrder(host, Collections.singletonList("missing")));
        assertTrue(catalog.canPushOrderAnywhere(Collections.singletonList("status")));
        assertFalse(catalog.canPushOrderAnywhere(Collections.singletonList("missing")));
    }

    @Test
    public void edgesFromAndReachability() {
        FakeVertex host = vertex("host", "status");
        FakeVertex person = vertex("person", "name");
        FakeEdge owns = edge("owns",
                new ReferenceVertexSchema(endpointJson("@src", "person"), null),
                new ReferenceVertexSchema(endpointJson("@dst", "host"), null),
                true);
        SchemaCatalog catalog = catalogOf(host, person, owns);

        Set<ElementSchema> fromPerson = catalog.edgesFrom(Collections.singleton("person"), Direction.OUT);
        assertTrue(fromPerson.contains(owns));

        Set<ElementSchema> fromHostOut = catalog.edgesFrom(Collections.singleton("host"), Direction.OUT);
        assertFalse("host is not an out-endpoint of owns", fromHostOut.contains(owns));

        assertTrue(catalog.pathExists(Collections.singleton("person"), Collections.singleton("host"), 1));
        // Reachability treats catalog edges as bidirectional for multi-hop planning.
        assertTrue(catalog.pathExists(Collections.singleton("host"), Collections.singleton("person"), 1));

        Set<ElementSchema> reachable = catalog.reachableVertexTypes(Collections.singleton("person"), 1);
        assertTrue(reachable.contains(person));
        assertTrue(reachable.contains(host));
    }

    @Test
    public void findPathsTwoHopClosedTopology() {
        FakeVertex host = vertex("host", "status");
        FakeVertex person = vertex("person", "name");
        FakeEdge owns = edge("owns",
                new ReferenceVertexSchema(endpointJson("@src", "person"), null),
                new ReferenceVertexSchema(endpointJson("@dst", "host"), null),
                true);
        // host -runs-> person (second hop type)
        FakeEdge runs = edge("runs",
                new ReferenceVertexSchema(endpointJson("@src", "host"), null),
                new ReferenceVertexSchema(endpointJson("@dst", "person"), null),
                true);
        SchemaCatalog catalog = catalogOf(host, person, owns, runs);

        List<Hop> hops = Arrays.asList(
                Hop.of(Direction.OUT, Collections.singleton("owns")),
                Hop.of(Direction.OUT, Collections.singleton("runs"), Collections.singleton("person")));
        List<PathPlan> plans = catalog.findPaths(Collections.singleton("person"), hops, 32);
        assertFalse(plans.isEmpty());
        assertEquals(2, plans.get(0).size());
        assertEquals(owns, plans.get(0).getHops().get(0).getEdgeSchema());
        assertEquals(runs, plans.get(0).getHops().get(1).getEdgeSchema());

        // Impossible: person -runs-> ...
        List<PathPlan> none = catalog.findPaths(Collections.singleton("person"),
                Collections.singletonList(Hop.of(Direction.OUT, Collections.singleton("runs"))), 32);
        assertTrue(none.isEmpty());
    }

    @Test
    public void extractClosedLabelsFromPredicates() {
        org.unipop.query.predicates.PredicatesHolder ph =
                org.unipop.query.predicates.PredicatesHolderFactory.predicate(
                        new org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer(
                                org.apache.tinkerpop.gremlin.structure.T.label.getAccessor(),
                                org.apache.tinkerpop.gremlin.process.traversal.P.within("a", "b")));
        Set<String> labels = SchemaCatalog.extractClosedLabels(ph);
        assertEquals(new HashSet<>(Arrays.asList("a", "b")), labels);
    }

    @Test
    public void rebuildReplacesContents() {
        FakeVertex host = vertex("host");
        SchemaCatalog catalog = catalogOf(host);
        assertEquals(1, catalog.vertexSchemas(Collections.emptySet()).size());

        FakeVertex person = vertex("person");
        catalog.rebuild(Collections.singletonList(contributor(false, person)));
        assertEquals(1, catalog.vertexSchemas(Collections.emptySet()).size());
        assertTrue(catalog.vertexSchemas(Collections.singleton("person")).contains(person));
        assertTrue(catalog.vertexSchemas(Collections.singleton("host")).isEmpty()
                || !catalog.vertexSchemas(Collections.singleton("host")).contains(host));
    }

    // ---- helpers ----

    private static SchemaCatalog catalogOf(ElementSchema... schemas) {
        SchemaCatalog catalog = new SchemaCatalog();
        catalog.rebuild(Collections.singletonList(contributor(true, schemas)));
        return catalog;
    }

    private static SchemaContributor contributor(boolean joinAllEdges, ElementSchema... schemas) {
        Set<ElementSchema> set = new HashSet<>(Arrays.asList(schemas));
        return new SchemaContributor() {
            @Override
            public Set<? extends ElementSchema> contributedSchemas() {
                return set;
            }

            @Override
            public String sourceId() {
                return "test-source";
            }

            @Override
            public boolean supportsJoin(ElementSchema schema) {
                return joinAllEdges && schema instanceof EdgeSchema;
            }
        };
    }

    private static FakeVertex vertex(String label, String... props) {
        JSONObject json = new JSONObject()
                .put("id", "@id")
                .put("label", label)
                .put("dynamicProperties", false);
        JSONObject properties = new JSONObject();
        for (String p : props) properties.put(p, "@" + p);
        json.put("properties", properties);
        return new FakeVertex(json);
    }

    private static FakeVertex openLabelVertex(String... props) {
        JSONObject json = new JSONObject()
                .put("id", "@id")
                .put("label", "@label")
                .put("dynamicProperties", false);
        JSONObject properties = new JSONObject();
        for (String p : props) properties.put(p, "@" + p);
        json.put("properties", properties);
        return new FakeVertex(json);
    }

    private static FakeVertex fieldLabelVertex(Set<String> includeLabels, String... props) {
        JSONObject label = new JSONObject().put("field", "label");
        org.json.JSONArray include = new org.json.JSONArray();
        for (String l : includeLabels) include.put(l);
        label.put("include", include);
        JSONObject json = new JSONObject()
                .put("id", "@id")
                .put("label", label)
                .put("dynamicProperties", false);
        JSONObject properties = new JSONObject();
        for (String p : props) properties.put(p, "@" + p);
        json.put("properties", properties);
        return new FakeVertex(json);
    }

    private static JSONObject endpointJsonWithInclude(String idField, Set<String> includeLabels) {
        JSONObject label = new JSONObject().put("field", "label");
        org.json.JSONArray include = new org.json.JSONArray();
        for (String l : includeLabels) include.put(l);
        label.put("include", include);
        return new JSONObject()
                .put("ref", true)
                .put("id", idField)
                .put("label", label)
                .put("properties", new JSONObject());
    }

    private static FakeVertex dynamicVertex(String label) {
        JSONObject json = new JSONObject()
                .put("id", "@id")
                .put("label", label)
                .put("dynamicProperties", true)
                .put("properties", new JSONObject());
        return new FakeVertex(json);
    }

    private static FakeEdge edge(String label, VertexSchema out, VertexSchema in, boolean supportsJoinMarker) {
        JSONObject json = new JSONObject()
                .put("id", "@id")
                .put("label", label)
                .put("dynamicProperties", false)
                .put("properties", new JSONObject());
        // supportsJoin is decided by contributor, not schema; marker unused on schema itself
        return new FakeEdge(json, out, in);
    }

    private static VertexSchema ref(String idField, String label) {
        return new ReferenceVertexSchema(endpointJson(idField, label), null);
    }

    private static JSONObject endpointJson(String idField, String label) {
        return new JSONObject()
                .put("ref", true)
                .put("id", idField)
                .put("label", label)
                .put("properties", new JSONObject());
    }

    private static JSONObject endpointJsonOpen(String idField) {
        return new JSONObject()
                .put("ref", true)
                .put("id", idField)
                .put("label", "@label")
                .put("properties", new JSONObject());
    }

    private static final class FakeVertex extends AbstractElementSchema<Vertex> implements VertexSchema {
        FakeVertex(JSONObject configuration) {
            super(configuration, null);
        }

        @Override
        public Vertex createElement(Map<String, Object> fields) {
            return null;
        }

        @Override
        public Collection<Vertex> fromFields(Map<String, Object> fields) {
            return null;
        }
    }

    private static final class FakeEdge extends AbstractElementSchema<Edge> implements EdgeSchema {
        private final VertexSchema out;
        private final VertexSchema in;

        FakeEdge(JSONObject configuration, VertexSchema out, VertexSchema in) {
            super(configuration, null);
            this.out = out;
            this.in = in;
        }

        @Override
        public VertexSchema getOutVertexSchema() {
            return out;
        }

        @Override
        public VertexSchema getInVertexSchema() {
            return in;
        }

        @Override
        public Collection<Edge> fromFields(Map<String, Object> fields) {
            return null;
        }
    }
}
