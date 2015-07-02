package org.elasticgremlin.starQueryHandler.groovy;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.process.ProcessStandardSuite;
import org.elasticgremlin.starQueryHandler.ModernStarGraphGraphProvider;
import org.elasticgremlin.structure.ElasticGraph;
import org.junit.runner.RunWith;

@RunWith(ProcessStandardSuite.class)
@GraphProviderClass(provider = ModernStarGraphGraphProvider.class, graph = ElasticGraph.class)
public class ModernStarGraphProcessStandardTest {
}
