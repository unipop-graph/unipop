# elastic-gremlin
TinkerPop 3 implementation on Elasticsearch backend.

Doesn't work completely yet.

# TODO
1. Make Structure unit tests pass.
2. finish implementing ELasticStep with all "has" predicates.
3. Make Proccess unit tests pass.
4. Optimize Queries
5. Optimize batch inserts (BatchGraph)
6. Configurable partitioning in ES (multiple indices) and ES mapping validation
7. Optimize aggregation steps with elasticsearch aggregations. (BarrierSteps - GraphTraversal.barrier())
8. Add multi-properties & meta-properties support.
9. Add graph variables support
10. Add ComputerGraph (OLAP) support. Spark?
11. Add transactions support
