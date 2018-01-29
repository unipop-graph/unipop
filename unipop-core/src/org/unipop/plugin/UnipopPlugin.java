package org.unipop.plugin;

import org.apache.tinkerpop.gremlin.jsr223.AbstractGremlinPlugin;
import org.apache.tinkerpop.gremlin.jsr223.DefaultImportCustomizer;
import org.apache.tinkerpop.gremlin.jsr223.ImportCustomizer;
import org.unipop.structure.UniGraph;

public class UnipopPlugin extends AbstractGremlinPlugin {
    private static final UnipopPlugin instance = new UnipopPlugin();
    private static final ImportCustomizer imports;

    static {
        try {
            imports = DefaultImportCustomizer.build()
                    .addClassImports(UniGraph.class)
                    .create();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }


    public UnipopPlugin() {
        super("unipop", imports);
    }

    public static UnipopPlugin instance() {
        return instance;
    }

    @Override
    public boolean requireRestart() {
        return true;
    }
}
