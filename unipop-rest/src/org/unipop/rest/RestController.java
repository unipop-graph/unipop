package org.unipop.rest;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.mashape.unirest.request.BaseRequest;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.unipop.elastic.document.DocumentSchema;
import org.unipop.query.controller.SimpleController;
import org.unipop.query.mutation.AddEdgeQuery;
import org.unipop.query.mutation.AddVertexQuery;
import org.unipop.query.mutation.PropertyQuery;
import org.unipop.query.mutation.RemoveQuery;
import org.unipop.query.search.DeferredVertexQuery;
import org.unipop.query.search.SearchQuery;
import org.unipop.query.search.SearchVertexQuery;
import org.unipop.schema.element.ElementSchema;
import org.unipop.schema.reference.DeferredVertex;
import org.unipop.structure.UniEdge;
import org.unipop.structure.UniElement;
import org.unipop.structure.UniGraph;
import org.unipop.structure.UniVertex;

import java.util.*;
import java.util.function.Function;
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
        BaseRequest search = edgeSchemas.iterator().next().getSearch(uniQuery);
        try {
            HttpResponse<JsonNode> jsonNodeHttpResponse = search.asJson();
            return edgeSchemas.iterator().next().parseResults(jsonNodeHttpResponse, uniQuery).iterator();
        } catch (UnirestException e) {
            e.printStackTrace();
        }
        return EmptyIterator.instance();
    }

    @Override
    public <E extends Element> Iterator<E> search(SearchQuery<E> uniQuery) {
        Set<? extends RestSchema<E>> schemas = getSchemas(uniQuery.getReturnType());
        BaseRequest search = schemas.iterator().next().getSearch(uniQuery);
        try {
            HttpResponse<JsonNode> jsonNodeHttpResponse = search.asJson();
            return schemas.iterator().next().parseResults(jsonNodeHttpResponse, uniQuery).iterator(); // TODO: iterate all schemas
        } catch (UnirestException e) {
            e.printStackTrace();
        }
        return EmptyIterator.instance();
    }


    @Override
    public void fetchProperties(DeferredVertexQuery query) {
        BaseRequest search = vertexSchemas.iterator().next().getSearch(query);
        try {
            HttpResponse<JsonNode> jsonNodeHttpResponse = search.asJson();
            Iterator<Vertex> iterator = vertexSchemas.iterator().next().parseResults(jsonNodeHttpResponse, query).iterator();// TODO: iterate all schemas
            Map<Object, DeferredVertex> vertexMap = query.getVertices().stream()
                    .collect(Collectors.toMap(UniElement::id, Function.identity(), (a, b) -> a));
            iterator.forEachRemaining(newVertex -> {
                DeferredVertex deferredVertex = vertexMap.get(newVertex.id());
                if (deferredVertex != null) deferredVertex.loadProperties(newVertex);
            });

        } catch (UnirestException e) {
            e.printStackTrace();
        }
    }

    @Override
    public <E extends Element> void remove(RemoveQuery<E> uniQuery) {

    }

    @Override
    public Vertex addVertex(AddVertexQuery uniQuery) {
        UniVertex vertex = new UniVertex(uniQuery.getProperties(), graph);
        try {
            vertexSchemas.iterator().next().addElement(vertex).asJson(); // TODO: iterate all schemas
        } catch (UnirestException e) {
            e.printStackTrace();
        }
        return vertex;
    }

    @Override
    public <E extends Element> void property(PropertyQuery<E> uniQuery) {
        E element = uniQuery.getElement();
        Set<? extends RestSchema<E>> schemas = getSchemas(element.getClass());
        try {
            schemas.iterator().next().addElement(element).asJson(); // TODO: iterate all schemas
        } catch (UnirestException e) {
            e.printStackTrace();
        }
    }

    private <E extends Element> Set<? extends RestSchema<E>> getSchemas(Class elementClass) {
        if (Vertex.class.isAssignableFrom(elementClass))
            return (Set<? extends RestSchema<E>>) vertexSchemas;
        else return (Set<? extends RestSchema<E>>) edgeSchemas;
    }

    @Override
    public Edge addEdge(AddEdgeQuery uniQuery) {
        UniEdge edge = new UniEdge(uniQuery.getProperties(), uniQuery.getOutVertex(), uniQuery.getInVertex(), graph);
        try {
            edgeSchemas.iterator().next().addElement(edge).asJson(); // TODO: iterate all schemas
        } catch (UnirestException e) {
            e.printStackTrace();
        }
        return edge;
    }
}
