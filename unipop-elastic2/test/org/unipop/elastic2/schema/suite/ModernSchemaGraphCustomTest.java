package org.unipop.elastic2.schema.suite;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.junit.runner.RunWith;
import org.unipop.elastic2.schema.misc.CustomTestSuite;
import org.unipop.elastic2.schema.misc.ModernSchemaGraphProvider;
import org.unipop.structure.UniGraph;

/**
 * Created by Gilad on 12/10/2015.
 */
@RunWith(CustomTestSuite.class)
@GraphProviderClass(provider = ModernSchemaGraphProvider.class, graph = UniGraph.class)
public class ModernSchemaGraphCustomTest {
}
