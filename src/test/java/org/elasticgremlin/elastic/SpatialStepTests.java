package org.elasticgremlin.elastic;

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.impl.PointImpl;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.*;
import org.elasticgremlin.ElasticGraphGraphProvider;
import org.elasticgremlin.queryhandler.elasticsearch.Geo;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.junit.*;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.junit.Assert.assertEquals;

/**
 * Created by Eliran on 6/3/2015.
 */
public class SpatialStepTests {

    String CLUSTER_NAME = "testscluster";
    String INDEX_NAME = "geo_index";
    String DOCUMENT_TYPE = "geo_item";
    Graph graph;

    @Before
    public void startUp() throws InstantiationException, IOException, ExecutionException, InterruptedException {
        ElasticGraphGraphProvider elasticGraphProvider = new ElasticGraphGraphProvider();
        final Configuration configuration = elasticGraphProvider.newGraphConfiguration("testGraph", this.getClass(), "spatialTests", LoadGraphWith.GraphData.MODERN);
        this.graph = elasticGraphProvider.openTestGraph(configuration);

        createGeoShapeMapping(elasticGraphProvider.getClient(),DOCUMENT_TYPE);
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

        GraphTraversalSource g = graph.traversal();

        String geoJsonPoint = "{ \"type\": \"Point\",\"coordinates\": [9, 9]}";
        long intersectionCounter = g.V().has("location", Geo.intersercts(geoJsonPoint)).count().next();
        assertEquals(1l, intersectionCounter);
        Element location = g.V().has("location", Geo.intersercts(geoJsonPoint)).next();
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

        GraphTraversalSource g = graph.traversal();

        String geoJsonPoint = "{ \"type\": \"Polygon\",\"coordinates\": [[[9, 10],[11, 10],[11, 8],[9, 8],[9, 10]]]}";
        long intersectionCounter = g.V().has("location", Geo.intersercts(geoJsonPoint)).count().next();
        assertEquals(1l, intersectionCounter);
        Element location = g.V().has("location", Geo.intersercts(geoJsonPoint)).next();
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
