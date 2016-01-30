package org.unipop.elastic.controller.schema;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.NotImplementedException;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.client.Client;
import org.unipop.controller.EdgeController;
import org.unipop.controller.Predicates;
import org.unipop.controller.VertexController;
import org.unipop.controllerprovider.BasicControllerManager;
import org.unipop.controllerprovider.ControllerManager;
import org.unipop.elastic.controller.schema.helpers.ElasticGraphConfiguration;
import org.unipop.elastic.controller.schema.helpers.LazyGetterFactory;
import org.unipop.elastic.controller.schema.helpers.ReflectionHelper;
import org.unipop.elastic.controller.schema.helpers.elementConverters.CompositeElementConverter;
import org.unipop.elastic.controller.schema.helpers.elementConverters.DualEdgeConverter;
import org.unipop.elastic.controller.schema.helpers.elementConverters.SingularEdgeConverter;
import org.unipop.elastic.controller.schema.helpers.elementConverters.VertexConverter;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.CachedGraphElementSchemaProvider;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.DefaultGraphElementSchemaProvider;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphElementSchemaProvider;
import org.unipop.elastic.controller.schema.helpers.schemaProviders.GraphElementSchemaProviderFactory;
import org.unipop.elastic.helpers.ElasticClientFactory;
import org.unipop.elastic.helpers.ElasticHelper;
import org.unipop.elastic.helpers.ElasticMutations;
import org.unipop.elastic.helpers.TimingAccessor;
import org.unipop.structure.BaseEdge;
import org.unipop.structure.BaseVertex;
import org.unipop.structure.UniGraph;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Created by Gilad on 12/10/2015.
 */
public class SchemaControllerManager extends BasicControllerManager {

    //region QueryHandler Implementation
    @Override
    public void init(UniGraph graph, Configuration configuration) throws IOException, ExecutionException, InterruptedException {
        ElasticGraphConfiguration elasticConfiguration = new ElasticGraphConfiguration(configuration);

        configuration.setProperty(Graph.GRAPH, UniGraph.class.getName());

        if (elasticConfiguration.getElasticGraphSchemaProviderFactory() != null) {
            this.schemaProvider = elasticConfiguration.getElasticGraphSchemaProviderFactory().getSchemaProvider();
        } else {
            try {
                this.schemaProvider = ((GraphElementSchemaProviderFactory) ReflectionHelper.createNew(elasticConfiguration.getElasticGraphSchemaProviderFactoryClass())).getSchemaProvider();
            } catch (Exception ex) {
                this.schemaProvider = new CachedGraphElementSchemaProvider(
                        new DefaultGraphElementSchemaProvider(Arrays.asList("_all"))
                );
            }
        }

        client = ElasticClientFactory.create(elasticConfiguration);

        String indexName = configuration.getString("graphName", "graph");
        ElasticHelper.createIndex(indexName, client);

        lazyGetterFactory = new LazyGetterFactory(client, schemaProvider);
        this.elasticMutations = new ElasticMutations(false, client, new TimingAccessor());

        this.vertexController = new SchemaVertexController(
                graph,
                this.schemaProvider,
                client,
                this.elasticMutations,
                elasticConfiguration,
                new VertexConverter(graph, schemaProvider, elasticMutations, lazyGetterFactory)
                );

        this.edgeController = new SchemaEdgeController(
                graph,
                this.schemaProvider,
                client,
                this.elasticMutations,
                elasticConfiguration,
                new CompositeElementConverter(
                        CompositeElementConverter.Mode.First,
                        new SingularEdgeConverter(graph, schemaProvider, elasticMutations, lazyGetterFactory),
                        new DualEdgeConverter(graph, schemaProvider, elasticMutations, lazyGetterFactory)
                ));
    }

    @Override
    public void commit() {
        this.elasticMutations.commit();
    }

    @Override
    public void close() {
        if (!isClosed) {
            //client.admin().indices().refresh(new RefreshRequest("standard"));
            client.close();
            isClosed = true;
        }
    }

    @Override
    protected VertexController getDefaultVertexController() {
        return this.vertexController;
    }

    @Override
    protected EdgeController getDefaultEdgeController() {
        return this.edgeController;
    }
    //endregion

    //region Fields
    protected GraphElementSchemaProvider schemaProvider;
    protected VertexController vertexController;
    protected EdgeController edgeController;
    protected ElasticMutations elasticMutations;
    protected LazyGetterFactory lazyGetterFactory;
    protected Client client;

    protected boolean isClosed;

    @Override
    public void init(Map<String, Object> conf, UniGraph graph) throws Exception {
        throw new NotImplementedException();
    }
    //endregion
}
