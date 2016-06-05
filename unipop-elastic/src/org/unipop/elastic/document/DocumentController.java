package org.unipop.elastic.document;

import com.google.common.collect.Iterators;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.core.Delete;
import io.searchbox.core.DocumentResult;
import io.searchbox.core.Get;
import io.searchbox.core.Index;
import io.searchbox.indices.CreateIndex;
import io.searchbox.indices.IndicesExists;
import io.searchbox.indices.Refresh;
import org.apache.tinkerpop.gremlin.structure.*;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.ShardId;
import org.jooq.lambda.Seq;
import org.unipop.common.schema.referred.DeferredVertex;
import org.unipop.elastic.common.FilterHelper;
import org.unipop.elastic.common.QueryIterator;
import org.unipop.elastic.document.schema.DocEdgeSchema;
import org.unipop.elastic.document.schema.DocSchema;
import org.unipop.elastic.document.schema.DocVertexSchema;
import org.unipop.query.predicates.PredicatesHolder;
import org.unipop.query.controller.SimpleController;
import org.unipop.query.mutation.AddEdgeQuery;
import org.unipop.query.mutation.AddVertexQuery;
import org.unipop.query.mutation.PropertyQuery;
import org.unipop.query.mutation.RemoveQuery;
import org.unipop.query.predicates.PredicatesHolderFactory;
import org.unipop.query.search.DeferredVertexQuery;
import org.unipop.query.search.SearchQuery;
import org.unipop.query.search.SearchVertexQuery;
import org.unipop.structure.UniEdge;
import org.unipop.structure.UniElement;
import org.unipop.structure.UniGraph;
import org.unipop.structure.UniVertex;

import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DocumentController implements SimpleController {

    private final JestClient client;
    private final Set<? extends DocVertexSchema> vertexSchemas;
    private final Set<? extends DocEdgeSchema> edgeSchemas;
    private UniGraph graph;
    private boolean dirty;

    public DocumentController(JestClient client, Set<DocVertexSchema> vertexSchemas, Set<DocEdgeSchema> edgeSchemas, UniGraph graph) {
        this.client = client;
        this.vertexSchemas = vertexSchemas;
        this.edgeSchemas = edgeSchemas;
        this.graph = graph;
    }

    //region QueryController
    @Override
    public <E extends Element>  Iterator<E> search(SearchQuery<E> uniQuery) {
        Set<? extends DocSchema<E>> schemas = getSchemas(uniQuery.getReturnType());
        Set<PredicatesHolder> schemasPredicates = schemas.stream().map(schema ->
                schema.toPredicates(uniQuery.getPredicates())).collect(Collectors.toSet());
        PredicatesHolder schemaPredicateHolders = PredicatesHolderFactory.or(schemasPredicates);

        Iterator<E> searchResponse;
        try {
            searchResponse = search(schemaPredicateHolders, schemas, uniQuery.getLimit());
        } catch (IOException e) {
            searchResponse = Collections.emptyIterator();
        }

        return searchResponse;
    }

    @Override
    public Iterator<Edge> search(SearchVertexQuery uniQuery) {
        Set<PredicatesHolder> schemasPredicates = edgeSchemas.stream().map(schema ->
                schema.toPredicates(uniQuery.getPredicates(), uniQuery.gertVertices(), uniQuery.getDirection())).collect(Collectors.toSet());
        PredicatesHolder schemaPredicateHolders = PredicatesHolderFactory.or(schemasPredicates);
        Iterator<Edge> searchResponse;
        try {
            searchResponse = search(schemaPredicateHolders, edgeSchemas, uniQuery.getLimit());
        } catch (IOException e) {
            searchResponse = Collections.emptyIterator();
        }

        return searchResponse;
    }

    @Override
    public void fetchProperties(DeferredVertexQuery uniQuery) {
        Set<PredicatesHolder> schemasPredicates = vertexSchemas.stream().map(schema ->
                schema.toPredicates(uniQuery.getVertices())).collect(Collectors.toSet());
        PredicatesHolder schemaPredicateHolders = PredicatesHolderFactory.or(schemasPredicates);

        if(schemaPredicateHolders.isEmpty()) return;
        Iterator<Vertex> search;
        try {
            search = search(schemaPredicateHolders, vertexSchemas, -1);
        } catch (IOException e) {
            search = Collections.emptyIterator();
        }

        Map<Object, DeferredVertex> vertexMap = uniQuery.getVertices().stream().collect(Collectors.toMap(UniElement::id, Function.identity(), (a, b) -> a));
        search.forEachRemaining(newVertex -> {
            DeferredVertex deferredVertex = vertexMap.get(newVertex.id());
            if(deferredVertex != null) deferredVertex.loadProperties(newVertex);
        });

    }

    @Override
    public Edge addEdge(AddEdgeQuery uniQuery) {
        UniEdge edge = new UniEdge(uniQuery.getProperties(), uniQuery.getOutVertex(), uniQuery.getInVertex(), graph);
        try {
            index(this.edgeSchemas, edge, true);
        }
        catch (DocumentAlreadyExistsException ex) {
            throw Graph.Exceptions.edgeWithIdAlreadyExists(edge.id());
        } catch (IOException e) {
            //TODO: Handle Exception
        }
        return edge;
    }

    @Override
    public Vertex addVertex(AddVertexQuery uniQuery) {
        UniVertex vertex = new UniVertex(uniQuery.getProperties(), graph);
        try {
            index(this.vertexSchemas, vertex, true);
        }
        catch(DocumentAlreadyExistsException ex){
            throw Graph.Exceptions.vertexWithIdAlreadyExists(vertex.id());
        } catch (IOException e) {
            //TODO: Handle Exception
        }
        return vertex;
    }

    @Override
    public <E extends Element> void property(PropertyQuery<E> uniQuery) {
        Set<? extends DocSchema<E>> schemas = getSchemas(uniQuery.getElement().getClass());
        try {
            index(schemas, uniQuery.getElement(), false);
        } catch (IOException e) {
            //TODO: Handle Exception
        }
    }

    @Override
    public <E extends Element> void remove(RemoveQuery<E> uniQuery) {
        uniQuery.getElements().forEach(element -> {
            Set<? extends DocSchema<Element>> schemas = getSchemas(element.getClass());
            try {
                delete(schemas, element);
            } catch (IOException e) {
                //TODO: Handle Exception
            }
        });
    }
    //endregion

    //region Helpers
    private <E extends Element> Set<? extends DocSchema<E>> getSchemas(Class elementClass) {
        if(Vertex.class.isAssignableFrom(elementClass))
            return (Set<? extends DocSchema<E>>) vertexSchemas;
        else return (Set<? extends DocSchema<E>>) edgeSchemas;
    }
    //endregion

    //region Elastic Queries
    private <E extends Element, S extends DocSchema<E>> Iterator<E> search(PredicatesHolder allPredicates, Set<S> schemas, int limit) throws IOException {
        if(schemas.size() == 0 || allPredicates.isAborted()) return Collections.emptyIterator();

        FilterBuilder filterBuilder = FilterHelper.createFilterBuilder(allPredicates);
        QueryBuilder query = QueryBuilders.filteredQuery(QueryBuilders.matchAllQuery(), filterBuilder);

        String[] indices = schemas.stream().map(DocSchema::getIndex).toArray(String[]::new);
        refresh(indices);
        QueryIterator.Parser<E> parser = (searchHit) -> schemas.stream().map(schema -> schema.fromFields(searchHit)).findFirst().get();
        return new QueryIterator<>(query, 0, limit, client, parser, indices);
    }

    private <E extends Element> DocumentResult index(Set<? extends DocSchema<E>> schemas, E element, boolean create) throws IOException {
        for(DocSchema<E> schema : schemas) {
            DocSchema.Document document = schema.toDocument(element);
            if (document != null) {
                if (create == true) {
                    Get getRequest = new Get.Builder(document.getIndex(), document.getId()).build();
                    if (this.client.execute(getRequest).isSucceeded()) {
                        throw new DocumentAlreadyExistsException(
                                // shard id not interesting.
                                new ShardId(document.getIndex(), -1),
                                document.getType(),
                                document.getId());
                    }
                }
                Index index = new Index.Builder(Seq.seq(document.getFields()))
                        .id(document.getId())
                        .index(document.getIndex())
                        .type(document.getType())
                        .build();

//                IndexRequestBuilder indexRequest = client.prepareIndex(document.getIndex(), document.getType(), document.getId())
//                        .setSource(document.getFields()).setCreate(create);
                dirty = true;
                return this.client.execute(index);
            }
        }
        return null;
    }

    private <E extends Element> JestResult delete(Set<? extends DocSchema<E>> schemas, E element) throws IOException {
        for(DocSchema<E> schema : schemas) {
            DocSchema.Document document = schema.toDocument(element);
            if (document != null) {
                Delete delete = new Delete.Builder(document.getId())
                        .index(document.getIndex())
                        .type(document.getType())
                        .build();

                dirty = true;
                return this.client.execute(delete);
            }
        }
        return null;
    }

    private void refresh(String... indices) throws IOException {
        if (this.dirty) {
            Refresh refresh = new Refresh.Builder().addIndex(Seq.of(indices).toList()).build();
            this.client.execute(refresh);
            this.dirty = false;
        }
    }
    //endregion
}
