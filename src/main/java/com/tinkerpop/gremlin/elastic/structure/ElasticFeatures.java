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

                    @Override
                    public boolean supportsBooleanValues() {
                        return false;
                    }

                    @Override
                    public boolean supportsByteValues() {
                        return false;
                    }

                    @Override
                    public boolean supportsDoubleValues() {
                        return false;
                    }

                    @Override
                    public boolean supportsFloatValues() {
                        return false;
                    }

                    @Override
                    public boolean supportsIntegerValues() {
                        return false;
                    }

                    @Override
                    public boolean supportsLongValues() {
                        return false;
                    }

                    @Override
                    public boolean supportsMapValues() {
                        return false;
                    }

                    @Override
                    public boolean supportsMixedListValues() {
                        return false;
                    }

                    @Override
                    public boolean supportsBooleanArrayValues() {
                        return false;
                    }

                    @Override
                    public boolean supportsByteArrayValues() {
                        return false;
                    }

                    @Override
                    public boolean supportsDoubleArrayValues() {
                        return false;
                    }

                    @Override
                    public boolean supportsFloatArrayValues() {
                        return false;
                    }

                    @Override
                    public boolean supportsIntegerArrayValues() {
                        return false;
                    }

                    @Override
                    public boolean supportsStringArrayValues() {
                        return false;
                    }

                    @Override
                    public boolean supportsLongArrayValues() {
                        return false;
                    }

                    @Override
                    public boolean supportsSerializableValues() {
                        return false;
                    }

                    @Override
                    public boolean supportsStringValues() {
                        return false;
                    }

                    @Override
                    public boolean supportsUniformListValues() {
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

            @Override
            public VertexPropertyFeatures properties() {
                return new VertexPropertyFeatures() {
                    @Override
                    public boolean supportsUserSuppliedIds() {
                        return false;
                    }

                    @Override
                    public boolean supportsNumericIds() {
                        return false;
                    }

                    @Override
                    public boolean supportsStringIds() {
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

                };
            }
        };
    }

    @Override
    public String toString() {
        return StringFactory.featureString(this);
    }
}
