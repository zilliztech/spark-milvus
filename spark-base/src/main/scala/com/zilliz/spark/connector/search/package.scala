package com.zilliz.spark.connector

/** The connector's execution of a NEAREST BY join
  * (docs/design/architecture/dataframe-api.html sections 2 and 4). The
  * `extensions` package plans the join into `MilvusNearestByJoinExec` over a
  * Milvus table or `MilvusNearestByJoinFrameExec` over any other base, and
  * either node's `doExecute` runs `NearestBySearch`; the distances are computed
  * in `core.vector`.
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
  * query's top-k (`SearchMerge`: `CandidateBytes.merge` over an RDD shuffle by
  * query id) and reads the base columns of the rows that survived
  * (`SearchTake`, which hands rows on as `InternalRow`s). A base that is not
  * such a scan is a DataFrame input (`FrameSearch`): every pair of a base
  * partition and a query group is one task, which reads the partition a block
  * at a time into float32 batches for the exact scan and keeps the rows some
  * query's heap holds; the candidates carry those rows through the merge
  * (`CarriedCandidates`), so there is no take stage. Both inputs share the
  * delivery and the merge, and `NearestBySearch` handles their query rows the
  * same way; a DataFrame input's dimension is the commonest length among the
  * query vectors it could search. `SearchExecutionPlan` resolves budgets, task
  * resources, query delivery, retention, concurrency, prefetch and partition
  * counts for both inputs. `SearchResources` holds the resource formulas and
  * `core.vector.SearchPlan` the format-independent grouping algorithm.
  * Executors consume the plan and check actual retained bytes as data loads
  * (docs/design/architecture/search-resources.html section 3.3).
  *
  * The search evaluates `milvus.filter` on a Milvus table base (R7) and holds
  * the take stage's Arrow child allocators (G3). Capabilities: R7, V5, V7, V9,
  * G3 (see docs/design/capabilities.md).
  */
package object search
