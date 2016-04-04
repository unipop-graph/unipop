package org.unipop.elastic.schema.suite;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.process.ProcessStandardSuite;
import org.junit.runner.RunWith;
import org.unipop.elastic.schema.ElasticElementSchema.misc.ModernSchemaGraphProvider;
import org.unipop.structure.UniGraph;

/**
 * Created by Gilad on 19/10/2015.
 */
@RunWith(ProcessStandardSuite.class)
@GraphProviderClass(provider = ModernSchemaGraphProvider.class, graph = UniGraph.class)
public class ModernSchemaGraphProcessStandardTest {
}
