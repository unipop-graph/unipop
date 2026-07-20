package org.unipop.schema.catalog;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.unipop.schema.element.EdgeSchema;
import org.unipop.schema.element.ElementSchema;
import org.unipop.schema.element.VertexSchema;
import org.unipop.schema.property.DynamicPropertySchema;
import org.unipop.schema.property.NonDynamicPropertySchema;
import org.unipop.schema.property.PropertySchema;
import org.unipop.schema.property.StaticPropertySchema;
import org.unipop.schema.reference.ReferenceVertexSchema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Populates a TinkerGraph schema catalog from {@link SchemaContributor}s.
 */
final class SchemaCatalogBuilder {

    private SchemaCatalogBuilder() {}

    static void build(Graph graph, Collection<? extends SchemaContributor> contributors) {
        if (contributors == null) return;
        for (SchemaContributor contributor : contributors) {
            if (contributor == null) continue;
            addContribution(graph, contributor);
        }
    }

    private static void addContribution(Graph graph, SchemaContributor contributor) {
        Vertex source = graph.addVertex(T.label, SchemaCatalog.V_SOURCE);
        source.property(SchemaCatalog.P_SOURCE_ID, contributor.sourceId());
        source.property(SchemaCatalog.P_PROVIDER_CLASS, contributor.providerClass());

        // schema instance → catalog vertex (vtype or etype)
        Map<ElementSchema, Vertex> typeBySchema = new IdentityHashMap<>();
        // Only true joinable / searchable roots from the controller — not pure ref endpoints.
        List<ElementSchema> roots = new ArrayList<>(contributor.contributedSchemas());

        for (ElementSchema schema : roots) {
            if (schema == null || typeBySchema.containsKey(schema)) continue;
            Vertex typeVertex = addTypeVertex(graph, schema, contributor, source);
            if (typeVertex != null) typeBySchema.put(schema, typeVertex);
        }

        // Link edge endpoints to same-source vtypes by closed label intersection.
        for (ElementSchema schema : roots) {
            if (!(schema instanceof EdgeSchema)) continue;
            Vertex etype = typeBySchema.get(schema);
            if (etype == null) continue;
            EdgeSchema edgeSchema = (EdgeSchema) schema;
            linkEndpoint(etype, edgeSchema.getOutVertexSchema(), SchemaCatalog.E_OUT, SchemaCatalog.P_OUT_OPEN, typeBySchema, source);
            linkEndpoint(etype, edgeSchema.getInVertexSchema(), SchemaCatalog.E_IN, SchemaCatalog.P_IN_OPEN, typeBySchema, source);
        }
    }

    private static Vertex addTypeVertex(Graph graph, ElementSchema schema, SchemaContributor contributor, Vertex source) {
        String kind;
        if (schema instanceof VertexSchema) {
            // Skip pure reference endpoints if somehow contributed — they are not join targets.
            if (schema instanceof ReferenceVertexSchema) return null;
            kind = SchemaCatalog.V_VTYPE;
        } else if (schema instanceof EdgeSchema) {
            kind = SchemaCatalog.V_ETYPE;
        } else {
            return null;
        }

        LabelInfo labelInfo = extractLabels(schema);
        PropInfo propInfo = extractProperties(schema);

        Vertex v = graph.addVertex(T.label, kind);
        v.property(SchemaCatalog.P_SCHEMA, schema);
        v.property(SchemaCatalog.P_LABEL_OPEN, labelInfo.open);
        for (String label : labelInfo.labels) {
            v.property(VertexProperty.Cardinality.set, SchemaCatalog.P_LABELS, label);
        }
        v.property(SchemaCatalog.P_DYNAMIC_PROPS, propInfo.dynamic);
        for (String key : propInfo.keys) {
            v.property(VertexProperty.Cardinality.set, SchemaCatalog.P_PROPERTY_KEYS, key);
        }
        v.property(SchemaCatalog.P_SUPPORTS_ORDER, true);
        v.property(SchemaCatalog.P_SUPPORTS_RANGE, true);
        if (SchemaCatalog.V_ETYPE.equals(kind)) {
            v.property(SchemaCatalog.P_SUPPORTS_JOIN, contributor.supportsJoin(schema));
            // Default open until linkEndpoint resolves; overwritten there.
            v.property(SchemaCatalog.P_OUT_OPEN, true);
            v.property(SchemaCatalog.P_IN_OPEN, true);
        } else {
            v.property(SchemaCatalog.P_SUPPORTS_JOIN, false);
        }
        v.addEdge(SchemaCatalog.E_BOUND_TO, source);
        return v;
    }

    private static void linkEndpoint(Vertex etype, VertexSchema endpoint, String catalogEdgeLabel,
                                     String openFlag, Map<ElementSchema, Vertex> typeBySchema, Vertex source) {
        if (endpoint == null) {
            etype.property(openFlag, true);
            return;
        }

        // Nested non-ref vertex schema that is also a contributed vtype — link directly.
        Vertex direct = typeBySchema.get(endpoint);
        if (direct != null && SchemaCatalog.V_VTYPE.equals(direct.label())) {
            etype.property(openFlag, false);
            etype.addEdge(catalogEdgeLabel, direct);
            return;
        }

        LabelInfo info = extractLabels(endpoint);
        if (info.open || info.labels.isEmpty()) {
            etype.property(openFlag, true);
            return;
        }

        etype.property(openFlag, false);
        // Link to every same-source vtype whose closed labels intersect the endpoint labels.
        for (Map.Entry<ElementSchema, Vertex> e : typeBySchema.entrySet()) {
            Vertex vtype = e.getValue();
            if (!SchemaCatalog.V_VTYPE.equals(vtype.label())) continue;
            if (!sameSource(vtype, source)) continue;
            if (Boolean.TRUE.equals(vtype.property(SchemaCatalog.P_LABEL_OPEN).orElse(false))) {
                // Open vtypes on the same source remain candidates via joinTargets fallback path;
                // still add a link so closed-endpoint topology includes them? Prefer not — open
                // vtypes are pulled in by joinTargets when target labels empty / via sameSource.
                continue;
            }
            Set<String> vLabels = SchemaCatalog.labelSet(vtype);
            if (intersects(vLabels, info.labels)) {
                etype.addEdge(catalogEdgeLabel, vtype);
            }
        }
    }

    private static boolean sameSource(Vertex typeVertex, Vertex source) {
        Iterator<org.apache.tinkerpop.gremlin.structure.Edge> it =
                typeVertex.edges(org.apache.tinkerpop.gremlin.structure.Direction.OUT, SchemaCatalog.E_BOUND_TO);
        return it.hasNext() && it.next().inVertex().equals(source);
    }

    static LabelInfo extractLabels(ElementSchema schema) {
        LabelInfo info = new LabelInfo();
        if (!(schema instanceof org.unipop.schema.property.AbstractPropertyContainer)) {
            info.open = true;
            return info;
        }
        List<PropertySchema> props = ((org.unipop.schema.property.AbstractPropertyContainer) schema).getPropertySchemas();
        PropertySchema labelSchema = null;
        for (PropertySchema p : props) {
            if (p != null && Objects.equals(p.getKey(), T.label.getAccessor())) {
                labelSchema = p;
                break;
            }
        }
        if (labelSchema == null) {
            info.open = true;
            return info;
        }
        // Closed domains: static label, FieldPropertySchema include, EnumPropertySchema values/include, …
        Set<Object> known = labelSchema.knownValues();
        if (known != null && !known.isEmpty()) {
            info.open = false;
            for (Object v : known) {
                if (v != null) info.labels.add(v.toString());
            }
            if (info.labels.isEmpty()) info.open = true;
            return info;
        }
        // Legacy path for StaticPropertySchema if knownValues ever empty
        if (labelSchema instanceof StaticPropertySchema) {
            info.open = false;
            Object value = ((StaticPropertySchema) labelSchema).toProperties(java.util.Collections.emptyMap())
                    .get(T.label.getAccessor());
            if (value != null) info.labels.add(value.toString());
            if (info.labels.isEmpty()) info.open = true;
            return info;
        }
        info.open = true;
        return info;
    }

    static PropInfo extractProperties(ElementSchema schema) {
        PropInfo info = new PropInfo();
        if (!(schema instanceof org.unipop.schema.property.AbstractPropertyContainer)) {
            info.dynamic = true;
            return info;
        }
        List<PropertySchema> props = ((org.unipop.schema.property.AbstractPropertyContainer) schema).getPropertySchemas();
        for (PropertySchema p : props) {
            if (p == null) continue;
            if (p instanceof NonDynamicPropertySchema) {
                info.dynamic = false;
                continue;
            }
            if (p instanceof DynamicPropertySchema) {
                info.dynamic = true;
                continue;
            }
            String key = p.getKey();
            if (key != null
                    && !key.equals(T.id.getAccessor())
                    && !key.equals(T.label.getAccessor())) {
                info.keys.add(key);
            }
        }
        return info;
    }

    private static boolean intersects(Set<String> a, Set<String> b) {
        for (String s : a) {
            if (b.contains(s)) return true;
        }
        return false;
    }

    static final class LabelInfo {
        boolean open;
        final Set<String> labels = new HashSet<>();
    }

    static final class PropInfo {
        boolean dynamic;
        final Set<String> keys = new HashSet<>();
    }
}
