package com.zilliz.spark.connector

/** The connector's execution of a NEAREST BY join. The `extensions` package
  * plans the join into `MilvusNearestByJoinExec` over a Milvus table or
  * `MilvusNearestByJoinFrameExec` over any other base, and either node's
  * `doExecute` runs `NearestBySearch`; the distances are computed in
  * `core.vector`.
  *
  * `NearestBySearch` coordinates the join and assembles its output.
  * `NearestByInput` adapts the base's fields, layout, options and search entry,
  * then returns each query id with its requested base columns;
  * `NearestByQueryRows` classifies, caches, numbers and packs query rows.
  * Executor query vectors travel directly as numbered arrays. Their stable ids
  * identify join rows; dense packing positions only address query groups.
  * Queries outside Knowhere's range go to Spark's execution. Each hit gets its
  * query row back by number: from a broadcast when the driver holds the rows,
  * by a join after the take when they stay on the executors.
  *
  * `MilvusSearch` runs the search over the segments of a Milvus table input:
  * the partitions of the Milvus scan the optimizer left, with the predicate
  * Spark pushed into it (the `read` package). It consumes a
  * `SearchExecutionPlan`, delivers the query set through `SearchQueries` --
  * broadcast whole, or packed by group into a shuffle -- and reads it through
  * `SearchQueryRanges`, runs the first stage (`SegmentSetSearch`), merges every
  * query's top-k (`SearchMerge`: `CandidateRuns` over an RDD shuffle by query
  * id, compacted with `CandidateBytes.merge`) and reads the base columns of the
  * rows that survived (`SearchTake`, which projects requested columns once into
  * owned rows paired with query ids). Rank and score stop at the global top-k
  * merge. Every local top-k and merge ranks Float scores. Every candidate has
  * one 12-byte encoding: a UInt32 segment ordinal, a UInt32 physical row offset
  * and a Float score. `MilvusSearch` broadcasts the whole search's ordered
  * segment dictionary once; task packers retain only their set's global
  * ordinals and merge states carry no dictionary. Known oversized physical
  * extents fail planning; every encoded offset is checked even when metadata
  * cannot establish the extent. Final rows recover the original segment ids.
  * Resource planning keeps its conservative candidate estimates. A base that is
  * not such a scan is a DataFrame input (`FrameSearch`): every pair of a base
  * partition and a query group is one task, which reads the partition a block
  * at a time into float32 batches for the exact scan and keeps the rows some
  * query's heap holds; the candidates use the base partition id as their
  * ordinal and carry those rows after a 16-byte header through the merge
  * (`CarriedCandidates`), so there is no take stage. Both inputs share the
  * delivery and the merge, and `NearestBySearch` handles their query rows the
  * same way; a DataFrame input's dimension is the commonest length among the
  * query vectors it could search. `SearchExecutionPlan` resolves budgets, task
  * resources, query delivery, retention, concurrency, prefetch and partition
  * counts for both inputs; the merge stage's tasks per executor follow what one
  * query's planned state needs, and a k no executor's heap can merge fails at
  * planning. Proven physical row counts bound each set's candidate capacity by
  * the sum of its segments and the global merge by the sum of all segments,
  * each capped at the requested k. Unknown counts retain the requested k. Query
  * grouping budgets for the global bound before sets are formed; tasks allocate
  * their mergers with the capacity recorded in the execution plan. Native
  * result buffers, direct-memory group limits and index prefetch reserve only
  * the largest per-segment answer, because each engine call searches one
  * segment. `SearchResources` holds the resource formulas and
  * `core.vector.SearchPlan` the format-independent grouping algorithm.
  * Executors consume the plan and check actual retained bytes as data loads.
  * `SearchGroupPipeline` overlaps one held-set group search with the current
  * group's packing and output when the task holds every executor core. The task
  * thread consumes input and output; close drains the worker before releasing
  * its borrowed native resources.
  *
  * The search evaluates `milvus.filter` on a Milvus table base (R7) and holds
  * the take stage's Arrow child allocators (G3).
  */
package object search
