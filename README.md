# elastic-gremlin
TinkerPop 3 implementation on Elasticsearch backend.

Doesn't work completely yet.

# TODO
* Make all tests pass
* Optimize Queries with Scroll API
* Optimize inserts with Batch API
* Optimize traversals with MultiSearch API
* Optimize aggregation steps with elasticsearch aggregations. (GraphTraversal.barrier())
* Support multiple indices in ES, with some kind of configurable partitioning strategy.
* Add multi-properties & meta-properties support.
* Add graph variables support
* Add ComputerGraph (OLAP) support. Spark?
