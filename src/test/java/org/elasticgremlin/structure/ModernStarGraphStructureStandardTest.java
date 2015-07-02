package org.elasticgremlin.structure;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.structure.StructureStandardSuite;
import org.elasticgremlin.ModernStarGraphGraphProvider;
import org.elasticgremlin.testimpl.ModernStarGraph;
import org.junit.runner.RunWith;

@RunWith(StructureStandardSuite.class)
@GraphProviderClass(provider = ModernStarGraphGraphProvider.class, graph = ModernStarGraph.class)
public class ModernStarGraphStructureStandardTest {

}
