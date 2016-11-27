package org.unipop.rest;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.exceptions.UnirestException;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unipop.query.controller.SimpleController;
import org.unipop.query.mutation.AddEdgeQuery;
import org.unipop.query.mutation.AddVertexQuery;
import org.unipop.query.mutation.PropertyQuery;
import org.unipop.query.mutation.RemoveQuery;
import org.unipop.query.search.DeferredVertexQuery;
import org.unipop.query.search.SearchQuery;
import org.unipop.query.search.SearchVertexQuery;
import org.unipop.schema.element.ElementSchema;
import org.unipop.structure.UniGraph;
import org.unipop.structure.UniVertex;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by sbarzilay on 24/11/16.
 */
public class RestController implements SimpleController{
    private static final Logger logger = LoggerFactory.getLogger(RestController.class);

    private final UniGraph graph;

    private Set<? extends RestVertexSchema> vertexSchemas = new HashSet<>();
    private Set<? extends RestEdgeSchema> edgeSchemas = new HashSet<>();

    public RestController(UniGraph graph, Set<RestSchema> schemas) {
        this.graph = graph;
        Set<RestSchema> documentSchemas = collectSchemas(schemas);
        this.vertexSchemas = documentSchemas.stream().filter(schema -> schema instanceof RestVertexSchema)
                .map(schema -> ((RestVertexSchema) schema)).collect(Collectors.toSet());
        this.edgeSchemas = documentSchemas.stream().filter(schema -> schema instanceof RestEdgeSchema)
                .map(schema -> ((RestEdgeSchema) schema)).collect(Collectors.toSet());

        logger.debug("Instantiated RestController: {}", this);
    }

    private Set<RestSchema> collectSchemas(Set<? extends ElementSchema> schemas) {
        Set<RestSchema> docSchemas = new HashSet<>();

        schemas.forEach(schema -> {
            if (schema instanceof RestSchema) {
                docSchemas.add((RestSchema) schema);
                Set<RestSchema> childSchemas = collectSchemas(schema.getChildSchemas());
                docSchemas.addAll(childSchemas);
            }
        });
        return docSchemas;
    }

    @Override
    public Iterator<Edge> search(SearchVertexQuery uniQuery) {
        return null;
    }

    @Override
    public <E extends Element> Iterator<E> search(SearchQuery<E> uniQuery) {
        return null;
    }


    @Override
    public void fetchProperties(DeferredVertexQuery query) {

    }

    @Override
    public <E extends Element> void remove(RemoveQuery<E> uniQuery) {

    }

    @Override
    public Vertex addVertex(AddVertexQuery uniQuery) {
        UniVertex vertex = new UniVertex(uniQuery.getProperties(), graph);
        try {
            HttpResponse<JsonNode> jsonNodeHttpResponse = vertexSchemas.iterator().next().addElement(vertex).asJson();
            System.out.println(jsonNodeHttpResponse);
        } catch (UnirestException e) {
            e.printStackTrace();
        }
        return vertex;
    }

    @Override
    public <E extends Element> void property(PropertyQuery<E> uniQuery) {

    }

    @Override
    public Edge addEdge(AddEdgeQuery uniQuery) {
        return null;
    }

}
