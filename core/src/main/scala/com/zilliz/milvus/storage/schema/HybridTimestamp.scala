package com.zilliz.milvus.storage.schema

/** Converts Unix epoch microseconds to the greatest Milvus HybridTS in the same
  * physical millisecond. Saturating the two ends avoids shift overflow while
  * preserving as-of ordering for every Long input.
  */
object HybridTimestamp {
  private val LogicalBits = 18
  private val LogicalMask = (1L << LogicalBits) - 1L
  private val MaxPhysicalMillis = Long.MaxValue >>> LogicalBits

  /** The first HybridTS of a physical millisecond, which is what a write stamps
    * its rows with.
    */
  def ofMillis(millis: Long): Long =
    if (millis < 0L) 0L
    else if (millis > MaxPhysicalMillis) Long.MaxValue
    else millis << LogicalBits

  def upperBound(epochMicros: Long): Long = {
    val millis = Math.floorDiv(epochMicros, 1000L)
    if (millis < 0L) Long.MinValue
    else if (millis > MaxPhysicalMillis) Long.MaxValue
    else (millis << LogicalBits) | LogicalMask
  }
}
