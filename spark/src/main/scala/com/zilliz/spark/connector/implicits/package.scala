package com.zilliz.spark.connector

import scala.language.implicitConversions

import org.apache.spark.sql.DataFrame

/** DataFrame nearestByJoin methods, adapting Spark versions without native
  * support. Java callers construct `MilvusDataFrame` directly. Connect and
  * classic class loading remain separate in the selected NearestByEntry shim.
  *
  * Capabilities: V5, V7 (see design document capabilities.md).
  */
package object implicits {

  implicit def toMilvusDataFrame(frame: DataFrame): MilvusDataFrame =
    new MilvusDataFrame(frame)
}
