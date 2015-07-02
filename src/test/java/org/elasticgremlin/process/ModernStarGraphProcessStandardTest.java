package org.elasticgremlin.process;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.process.ProcessStandardSuite;
import org.elasticgremlin.ModernStarGraphGraphProvider;
import org.elasticgremlin.testimpl.ModernStarGraph;
import org.junit.runner.RunWith;

@RunWith(ProcessStandardSuite.class)
@GraphProviderClass(provider = ModernStarGraphGraphProvider.class, graph = ModernStarGraph.class)
public class ModernStarGraphProcessStandardTest {
}
