package com.zilliz.milvus.storage.read.exec

import java.util.BitSet

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** What an index label means in its segment, for a column with nulls and
  * without (docs/design/architecture/vector-search.html section 2.4).
  */
class IndexRowMappingTest extends AnyFunSuite with Matchers {

  /** A bitmap in the layout the index files use: bit i of byte i/8, least
    * significant bit first.
    */
  private def validData(rows: Int)(present: Int => Boolean): Array[Byte] = {
    val bytes = new Array[Byte]((rows + 7) / 8)
    (0 until rows).foreach { row =>
      if (present(row)) {
        bytes(row / 8) = (bytes(row / 8) | (1 << (row % 8))).toByte
      }
    }
    bytes
  }

  test("without nulls a label is the row itself") {
    val mapping = IndexRowMapping.identity(1000L)

    mapping.isIdentity shouldBe true
    mapping.rows shouldBe 1000L
    mapping.rowOf(0L) shouldBe 0L
    mapping.rowOf(999L) shouldBe 999L
    mapping.labelOf(42L) shouldBe 42L
    the[IllegalArgumentException] thrownBy mapping.rowOf(1000L)
  }

  test("a label is the row of the label-th value the column has") {
    // Rows 1, 4 and 7 have no value.
    val mapping = IndexRowMapping.of(
      validData(9)(row => row != 1 && row != 4 && row != 7),
      9L
    )

    mapping.isIdentity shouldBe false
    mapping.rows shouldBe 6L
    (0 until 6).map(label => mapping.rowOf(label.toLong)) shouldBe
      Seq(0L, 2L, 3L, 5L, 6L, 8L)
    (0 until 9).map(row => mapping.labelOf(row.toLong)) shouldBe
      Seq(0L, -1L, 1L, 2L, -1L, 3L, 4L, -1L, 5L)
    the[IllegalArgumentException] thrownBy mapping.rowOf(6L)
  }

  test("the mapping holds across word boundaries") {
    val rows = 500
    // Every third row has no value, so the answer crosses several 64-bit words.
    val mapping =
      IndexRowMapping.of(validData(rows)(_ % 3 != 0), rows.toLong)
    val present = (0 until rows).filter(_ % 3 != 0).map(_.toLong)

    mapping.rows shouldBe present.size.toLong
    present.zipWithIndex.foreach { case (row, label) =>
      mapping.rowOf(label.toLong) shouldBe row
      mapping.labelOf(row) shouldBe label.toLong
    }
    (0 until rows)
      .filter(_ % 3 == 0)
      .foreach(row => mapping.labelOf(row.toLong) shouldBe -1L)
  }

  test("excluded rows become the labels that carry them") {
    val mapping = IndexRowMapping.of(
      validData(9)(row => row != 1 && row != 4 && row != 7),
      9L
    )
    val excluded = new BitSet(9)
    // Rows 1 and 4 have no value; rows 3 and 8 are deleted or filtered out.
    Seq(1, 3, 4, 8).foreach(excluded.set)

    val labels = mapping.labelsOf(excluded)

    labels.stream().toArray.toSeq shouldBe Seq(2, 5)
  }

  test("a segment without nulls passes its exclusions through") {
    val mapping = IndexRowMapping.identity(8L)
    val excluded = new BitSet(8)
    Seq(2, 5).foreach(excluded.set)

    mapping.labelsOf(excluded).stream().toArray.toSeq shouldBe Seq(2, 5)
  }

  test("a bitmap that covers fewer rows than the segment is refused") {
    the[IllegalArgumentException] thrownBy IndexRowMapping
      .of(validData(8)(_ => true), 100L)
    the[IllegalArgumentException] thrownBy IndexRowMapping
      .of(validData(8)(_ => false), 8L)
    the[IllegalArgumentException] thrownBy IndexRowMapping
      .of(Array.emptyByteArray, 8L)
  }
}
