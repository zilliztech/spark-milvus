package com.zilliz.milvus.storage.expr

import java.util.Arrays

/** Rows excluded from one Arrow batch.
  *
  * A set bit means that the row must not be returned. This is the same polarity
  * as row deletion, so readers combine the two exclusion decisions with OR.
  */
final class Bitmap private (
    val rowCount: Int,
    private val words: Array[Long]
) extends Serializable {

  def isExcluded(row: Int): Boolean = {
    if (row < 0 || row >= rowCount) {
      throw new IndexOutOfBoundsException(
        s"row $row is outside bitmap with $rowCount rows"
      )
    }
    (words(row >>> 6) & (1L << (row & 63))) != 0L
  }

  lazy val excludedCount: Int = {
    var count = 0
    var i = 0
    while (i < words.length) {
      count += java.lang.Long.bitCount(words(i))
      i += 1
    }
    count
  }

  def isEmpty: Boolean = excludedCount == 0

  def isFull: Boolean = excludedCount == rowCount

  def or(other: Bitmap): Bitmap = {
    require(
      other.rowCount == rowCount,
      s"cannot combine bitmaps with $rowCount and ${other.rowCount} rows"
    )
    val combined = new Array[Long](words.length)
    var i = 0
    while (i < words.length) {
      combined(i) = words(i) | other.words(i)
      i += 1
    }
    new Bitmap(rowCount, combined)
  }

  override def equals(other: Any): Boolean = other match {
    case that: Bitmap =>
      rowCount == that.rowCount && Arrays.equals(words, that.words)
    case _ => false
  }

  override def hashCode(): Int =
    31 * rowCount + Arrays.hashCode(words)

  override def toString: String =
    s"Bitmap(rowCount=$rowCount, excludedCount=$excludedCount)"
}

object Bitmap {
  def empty(rowCount: Int): Bitmap = {
    require(rowCount >= 0, s"row count must be non-negative: $rowCount")
    new Bitmap(rowCount, new Array[Long](wordCount(rowCount)))
  }

  private[expr] def fromWords(
      rowCount: Int,
      words: Array[Long]
  ): Bitmap = {
    require(rowCount >= 0, s"row count must be non-negative: $rowCount")
    require(
      words.length == wordCount(rowCount),
      s"${words.length} words cannot represent $rowCount rows"
    )
    new Bitmap(rowCount, words)
  }

  private def wordCount(rowCount: Int): Int =
    if (rowCount == 0) 0 else ((rowCount - 1) >>> 6) + 1
}
