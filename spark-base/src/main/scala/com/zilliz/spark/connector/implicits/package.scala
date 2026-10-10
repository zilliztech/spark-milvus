package com.zilliz.spark.connector

import scala.language.implicitConversions

import org.apache.spark.sql.DataFrame

/** The connector's methods on a DataFrame: Spark 4.2's `nearestByJoin` on the
  * Spark lines before 4.2, and `buildIndex` and `writeSnapshot` on a Milvus
  * table's DataFrame on every line (docs/design/architecture/dataframe-api.html
  * sections 6 and 9).
  *
  * `MilvusDataFrame` holds the methods, and importing this package's members
  * puts them on any DataFrame; Java constructs `MilvusDataFrame` itself. On 4.2
  * `Dataset.nearestByJoin` is a member, which Scala calls in preference, and
  * `MilvusDataFrame.nearestByJoin` calls it too. `buildIndex` and
  * `writeSnapshot` register the DataFrame as a temporary view and run `CALL
  * milvus.system.build_index` or `write_snapshot` with `table` naming it.
  * Nothing here refers to the classic session's classes, so the methods work in
  * a Spark Connect client on 4.x; each line's `NearestByEntry` builds the plan,
  * and on 4.0 and 4.1 it tells a Connect DataFrame by its class name and leaves
  * the plan to `ClassicNearestByJoin`.
  *
  * Capabilities: V5, V7, W6, W8 (see docs/design/capabilities.md).
  */
package object implicits {

  implicit def toMilvusDataFrame(frame: DataFrame): MilvusDataFrame =
    new MilvusDataFrame(frame)
}
