package com.zilliz.spark.connector.apps

/** The SQL vector functions (`cosine_similarity`, `l2_distance`,
  * `inner_product`, `hamming_distance`, `jaccard_distance`) and the session
  * extension that registers them. They evaluate on the JVM, row by row, over
  * values already in a DataFrame; they do not read a collection. `vector_knn`
  * was deleted on 2026-09-18 (decision 24).
  *
  * Capabilities: V8 (see docs/design/capabilities.md).
  */
package object search
