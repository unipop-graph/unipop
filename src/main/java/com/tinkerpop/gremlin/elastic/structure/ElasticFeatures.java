package com.tinkerpop.gremlin.elastic.structure;

import com.tinkerpop.gremlin.structure.Graph;
import com.tinkerpop.gremlin.structure.util.StringFactory;

public class ElasticFeatures implements Graph.Features {
    @Override
    public GraphFeatures graph() {
        return new GraphFeatures() {
            @Override
            public VariableFeatures variables() {
                return new VariableFeatures() {
                    @Override
                    public boolean supportsVariables() {
                        return false;
                    }
                };
            }

            @Override
            public boolean supportsComputer() {
                return false;
            }

            @Override
            public boolean supportsTransactions() {
                return false;
            }

            @Override
            public boolean supportsThreadedTransactions() {
                return false;
            }
        };
    }

    @Override
    public EdgeFeatures edge() {
        return new EdgeFeatures() {
            @Override
            public boolean supportsNumericIds() {
                return false;
            }

            @Override
            public boolean supportsAnyIds() {
                return false;
            }

            @Override
            public boolean supportsUuidIds() {
                return false;
            }

            @Override
            public boolean supportsCustomIds() {
                return false;
            }
        };
    }

    @Override
    public VertexFeatures vertex() {
        return new VertexFeatures() {
            @Override
            public boolean supportsMultiProperties() {
                return false;
            }

            @Override
            public boolean supportsNumericIds() {
                return false;
            }

            @Override
            public boolean supportsUuidIds() {
                return false;
            }

            @Override
            public boolean supportsCustomIds() {
                return false;
            }

            @Override
            public boolean supportsAnyIds() {
                return false;
            }

            @Override
            public boolean supportsMetaProperties() {
                return false;
            }
        };
    }

    @Override
    public String toString() {
        return StringFactory.featureString(this);
    }
}
