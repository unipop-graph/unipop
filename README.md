# elastic-gremlin
TinkerPop 3 implementation on Elasticsearch backend.

Doesn't work completely yet.

# TODO
1. Make Structure unit tests pass.
2. finish implementing ELasticStep with all "has" predicates.
3. Make Proccess unit tests pass.
4. Add geo-spatial support
5. Optimize Queries
6. Optimize batch inserts (BatchGraph)
7. Configurable partitioning in ES (multiple indices) and ES mapping validation
8. Optimize aggregation steps with elasticsearch aggregations. (BarrierSteps - GraphTraversal.barrier())
9. Add multi-properties & meta-properties support.
10. Add graph variables support
11. Add ComputerGraph (OLAP) support. Spark?
12. Add transactions support
