package com.zilliz.milvus.storage.read.exec

import java.util.BitSet

/** What an index label means in the segment it was built from.
  *
  * Milvus builds a vector index over the rows that have a value, so a nullable
  * column's index holds fewer rows than its segment and label `i` is the `i`-th
  * row with a value. The `valid_data` bitmap the index files carry says which
  * rows those are, least significant bit first, and this turns it into the two
  * directions a search needs: a returned label becomes a segment row number,
  * and the rows a search must skip become the labels to mask
  * (docs/design/architecture/vector-search.html section 2.4).
  *
  * A column without nulls maps label to row directly, which is what
  * [[IndexRowMapping.identity]] carries.
  */
final class IndexRowMapping private (
    private val words: Array[Long],
    private val before: Array[Long],
    val segmentRows: Long,
    val rows: Long
) {

  /** True when every row of the segment is in the index. */
  def isIdentity: Boolean = words.isEmpty

  /** The segment row the index means by this label. */
  def rowOf(label: Long): Long = {
    require(
      label >= 0 && label < rows,
      s"The index returned label $label of its $rows rows"
    )
    if (isIdentity) return label
    var low = 0
    var high = words.length - 1
    while (low < high) {
      val middle = (low + high + 1) >>> 1
      if (before(middle) <= label) low = middle else high = middle - 1
    }
    var word = words(low)
    var remaining = (label - before(low)).toInt
    while (remaining > 0) {
      word &= word - 1
      remaining -= 1
    }
    low.toLong * 64L + java.lang.Long.numberOfTrailingZeros(word).toLong
  }

  /** The label of this segment row, or -1 when the row has no value and is
    * therefore not in the index.
    */
  def labelOf(row: Long): Long = {
    require(
      row >= 0 && row < segmentRows,
      s"Row $row is outside the segment's $segmentRows rows"
    )
    if (isIdentity) return row
    val word = (row / 64L).toInt
    val bit = (row % 64L).toInt
    if ((words(word) & (1L << bit)) == 0L) -1L
    else
      before(word) + java.lang.Long.bitCount(words(word) & ((1L << bit) - 1L))
  }

  /** The rows a search must skip, expressed as the labels that carry them. A
    * row without a value needs no bit: it is not in the index at all.
    */
  def labelsOf(excluded: BitSet): BitSet = {
    if (isIdentity) return excluded
    val labels = new BitSet(math.max(rows.toInt, 1))
    var row = excluded.nextSetBit(0)
    while (row >= 0 && row < segmentRows) {
      val label = labelOf(row.toLong)
      if (label >= 0) labels.set(label.toInt)
      row = excluded.nextSetBit(row + 1)
    }
    labels
  }
}

object IndexRowMapping {

  /** Every row is in the index, so a label is a row. */
  def identity(rows: Long): IndexRowMapping = {
    require(rows >= 0, s"A segment holds $rows rows")
    new IndexRowMapping(Array.emptyLongArray, Array.emptyLongArray, rows, rows)
  }

  /** The mapping a `valid_data` bitmap describes: bit `i` set means row `i` has
    * a value and is in the index, counted from the least significant bit of the
    * first byte.
    */
  def of(validData: Array[Byte], segmentRows: Long): IndexRowMapping = {
    require(validData != null && validData.nonEmpty, "valid_data is empty")
    require(
      segmentRows > 0 && segmentRows <= Int.MaxValue,
      s"A segment of $segmentRows rows is outside what an index bitmap covers"
    )
    require(
      validData.length.toLong * 8L >= segmentRows,
      s"valid_data covers ${validData.length * 8} rows; the segment has $segmentRows"
    )
    val words = new Array[Long](((segmentRows + 63L) / 64L).toInt)
    var row = 0L
    while (row < segmentRows) {
      if ((validData((row / 8L).toInt) & (1 << (row % 8L).toInt)) != 0)
        words((row / 64L).toInt) |= 1L << (row % 64L).toInt
      row += 1L
    }
    val before = new Array[Long](words.length)
    var total = 0L
    var index = 0
    while (index < words.length) {
      before(index) = total
      total += java.lang.Long.bitCount(words(index)).toLong
      index += 1
    }
    require(total > 0, "valid_data marks no row as present")
    new IndexRowMapping(words, before, segmentRows, total)
  }
}
