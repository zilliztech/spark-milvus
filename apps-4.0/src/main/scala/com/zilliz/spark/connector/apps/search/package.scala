package com.zilliz.spark.connector.apps

/** The SQL vector functions (`cosine_similarity`, `l2_distance`,
  * `inner_product`, `hamming_distance`, `jaccard_distance`, `vector_knn`) and
  * the session extension that registers them. They evaluate on the JVM, row by
  * row, over values already in a DataFrame; they do not read a collection.
  * Whether `vector_knn`, which ranks an array in the JVM, stays is decision 24.
  *
  * Exact-KNN ground-truth and recall-evaluation jobs (O3) are planned here;
  * they call `MilvusSearch.search` and add no search path of their own. Design:
  * docs/design/architecture/vector-search.html section 1.
  *
  * Capabilities: V8 (see docs/design/capabilities.md).
  */
package object search
