package com.tinkerpop.gremlin.elastic.elastic;

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.impl.PointImpl;
import com.tinkerpop.gremlin.elastic.elasticservice.*;
import com.tinkerpop.gremlin.elastic.structure.ElasticGraph;
import com.tinkerpop.gremlin.elastic.process.graph.traversal.strategy.Geo;
import com.tinkerpop.gremlin.process.T;
import com.tinkerpop.gremlin.structure.Element;
import org.apache.commons.configuration.BaseConfiguration;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.junit.Assert.assertEquals;

/**
 * Created by Eliran on 6/3/2015.
 */
public class SpatialStepTests {

    String CLUSTER_NAME = "testscluster";
    String INDEX_NAME = "geo_index";
    String DOCUMENT_TYPE = "geo_item";
    ElasticGraph graph;
    @Before
    public void startUp() throws IOException {
        BaseConfiguration config = new BaseConfiguration();
        config.addProperty("elasticsearch.cluster.name", CLUSTER_NAME);
        config.addProperty("elasticsearch.index.name", INDEX_NAME);
        config.addProperty("elasticsearch.refresh", true);
        config.addProperty("elasticsearch.client", ElasticService.ClientType.NODE.toString());

        graph = new ElasticGraph(config);

        graph.elasticService.clearAllData();
        createGeoShapeMapping(graph.elasticService.client,DOCUMENT_TYPE);
    }

    @Test
    public void geoPointPolygonsIntersectionTest() throws IOException {
        //create polygons
        List<Point> firstPolygonPoints = new ArrayList<Point>();
        firstPolygonPoints.add(new PointImpl(10,10, SpatialContext.GEO));
        firstPolygonPoints.add(new PointImpl(8,10, SpatialContext.GEO));
        firstPolygonPoints.add(new PointImpl(8,8, SpatialContext.GEO));
        firstPolygonPoints.add(new PointImpl(10,8, SpatialContext.GEO));
        firstPolygonPoints.add(new PointImpl(10,10, SpatialContext.GEO));
        Map<String, Object> firstPolygon = buildGeoJsonPolygon(firstPolygonPoints);
        List<Point> secondPolygonPoints = new ArrayList<Point>();
        secondPolygonPoints.add(new PointImpl(14, 10, SpatialContext.GEO));
        secondPolygonPoints.add(new PointImpl(12, 10, SpatialContext.GEO));
        secondPolygonPoints.add(new PointImpl(12, 8, SpatialContext.GEO));
        secondPolygonPoints.add(new PointImpl(14, 8, SpatialContext.GEO));
        secondPolygonPoints.add(new PointImpl(14, 10, SpatialContext.GEO));
        Map<String, Object> secondPolygon = buildGeoJsonPolygon(secondPolygonPoints);

        //add the vertices to graph
        graph.addVertex(T.label,DOCUMENT_TYPE,T.id,"1","location",firstPolygon);
        graph.addVertex(T.label,DOCUMENT_TYPE,T.id,"2","location",secondPolygon);

        String geoJsonPoint = "{ \"type\": \"Point\",\"coordinates\": [9, 9]}";
        long intersectionCounter = graph.V().has("location", Geo.INTERSECT, geoJsonPoint).count().next();
        assertEquals(1l, intersectionCounter);
        Element location = graph.V().has("location", Geo.INTERSECT, geoJsonPoint).next();
        assertEquals("1",location.id().toString());
    }

    @Test
    public void polygonToPolygonsIntersectionTest() throws IOException {
        //create polygons
        List<Point> firstPolygonPoints = new ArrayList<Point>();
        firstPolygonPoints.add(new PointImpl(10,10, SpatialContext.GEO));
        firstPolygonPoints.add(new PointImpl(8,10, SpatialContext.GEO));
        firstPolygonPoints.add(new PointImpl(8,8, SpatialContext.GEO));
        firstPolygonPoints.add(new PointImpl(10,8, SpatialContext.GEO));
        firstPolygonPoints.add(new PointImpl(10,10, SpatialContext.GEO));
        Map<String, Object> firstPolygon = buildGeoJsonPolygon(firstPolygonPoints);
        List<Point> secondPolygonPoints = new ArrayList<Point>();
        secondPolygonPoints.add(new PointImpl(14, 10, SpatialContext.GEO));
        secondPolygonPoints.add(new PointImpl(12, 10, SpatialContext.GEO));
        secondPolygonPoints.add(new PointImpl(12, 8, SpatialContext.GEO));
        secondPolygonPoints.add(new PointImpl(14, 8, SpatialContext.GEO));
        secondPolygonPoints.add(new PointImpl(14, 10, SpatialContext.GEO));
        Map<String, Object> secondPolygon = buildGeoJsonPolygon(secondPolygonPoints);

        //add the vertices to graph
        graph.addVertex(T.label,DOCUMENT_TYPE,T.id,"1","location",firstPolygon);
        graph.addVertex(T.label,DOCUMENT_TYPE,T.id,"2","location",secondPolygon);

        String geoJsonPoint = "{ \"type\": \"Polygon\",\"coordinates\": [[[9, 10],[11, 10],[11, 8],[9, 8],[9, 10]]]}";
        long intersectionCounter = graph.V().has("location", Geo.INTERSECT, geoJsonPoint).count().next();
        assertEquals(1l, intersectionCounter);
        Element location = graph.V().has("location", Geo.INTERSECT, geoJsonPoint).next();
        assertEquals("1",location.id().toString());
    }

    private Map<String, Object> buildGeoJsonPolygon(List<Point> points) throws IOException {
        Map<String, Object> json = new HashMap<String, Object>();
        json.put("type","Polygon");
        List<double[]> newPoints = new ArrayList<double[]>();
        for (Point point : points){
            newPoints.add(new double[]{point.getX(),point.getY()});
        }
        Object[] pointsArray = newPoints.toArray();
        Object[] envelopeArray = new Object[] {pointsArray};
        json.put("coordinates",envelopeArray);
        return json;
    }
    private void createGeoShapeMapping(Client client, String documentType) throws IOException {

        final XContentBuilder mappingBuilder =

                jsonBuilder()
                        .startObject()
                        .startObject(documentType)
                        .startObject("properties")
                        .startObject("location")
                        .field("type", "geo_shape")
                        .field("tree_levels", "8")
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject();

        PutMappingResponse putMappingResponse = client.admin().indices()
                .preparePutMapping("geo_index")
                .setType(documentType)
                .setSource(mappingBuilder)
                .execute().actionGet();

    }

}
