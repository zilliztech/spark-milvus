package com.zilliz.milvus.storage.stats

import java.nio.charset.StandardCharsets
import java.nio.file.Files

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.expr._
import com.zilliz.milvus.storage.io.{FileInfo, LocalObjectStore, ObjectStore}
import com.zilliz.milvus.storage.snapshot._
import com.zilliz.milvus.storage.stats.PrimaryKeyValue.LongValue
import io.milvus.grpc.schema.{CollectionSchema, DataType, FieldSchema}

class PrimaryKeyBloomPrunerTest extends AnyFunSuite with Matchers {
  private val primaryKey = FieldSchema(
    fieldID = 100L,
    name = "id",
    dataType = DataType.Int64,
    isPrimaryKey = true
  )
  private val schema = CollectionSchema(name = "c", fields = Seq(primaryKey))

  test("predicate extraction respects AND intersection and OR completeness") {
    val id = FieldRef(100L, DataType.Int64)
    val other = FieldRef(101L, DataType.Int64)
    val oneOrTwo = Or(
      Comparison(id, ComparisonOperator.EqualTo, Literal.IntegerValue(1L)),
      In(id, Vector(Literal.IntegerValue(2L), Literal.NullValue))
    )
    val constrained = And(
      oneOrTwo,
      Comparison(id, ComparisonOperator.EqualNullSafe, Literal.IntegerValue(2L))
    )

    PrimaryKeyFilter.fromPredicate(constrained, primaryKey).get.values shouldBe
      Set(LongValue(2L))
    PrimaryKeyFilter
      .fromPredicate(
        And(
          Comparison(id, ComparisonOperator.EqualTo, Literal.IntegerValue(1L)),
          Comparison(
            other,
            ComparisonOperator.GreaterThan,
            Literal.IntegerValue(0L)
          )
        ),
        primaryKey
      )
      .get
      .values shouldBe Set(LongValue(1L))
    PrimaryKeyFilter.fromPredicate(
      Or(
        Comparison(id, ComparisonOperator.EqualTo, Literal.IntegerValue(1L)),
        Comparison(
          other,
          ComparisonOperator.EqualTo,
          Literal.IntegerValue(1L)
        )
      ),
      primaryKey
    ) shouldBe None
    PrimaryKeyFilter
      .fromPredicate(
        In(id, Vector(Literal.NullValue)),
        primaryKey
      )
      .get
      .values shouldBe empty
  }

  test("pruning keeps applicable partition and global L0 delete sources") {
    val root = Files.createTempDirectory("milvus-bloom-pruner")
    val counting = new CountingObjectStore(new LocalObjectStore(root.toString))
    val first = stats(100L, 1L, 2L)
    val candidate = Iterator
      .from(1000)
      .map(_.toLong)
      .find(value => !first.mightContainLong(value))
      .get
    counting.write("stats/30", first.toBytes)
    counting.write("stats/31", stats(100L, candidate).toBytes)

    val data20 = segment(30L, 20L, Seq("stats/30"))
    val data21 = segment(31L, 21L, Seq("stats/31"))
    val l020 = l0(40L, 20L)
    val l021 = l0(41L, 21L)
    val global = l0(42L, -1L)
    val fixed = snapshot(Seq(data20, data21, l020, l021, global))
    val pruner = PrimaryKeyBloomPruner.load(
      fixed,
      primaryKey,
      Map.empty,
      "",
      "",
      counting
    )
    counting.reads shouldBe 2

    val filter = PrimaryKeyFilter(
      100L,
      DataType.Int64,
      Set(LongValue(candidate))
    )
    val pruned = pruner.prune(filter)
    pruned.segments shouldBe Seq(data21, l021, global)
    pruned.partitionIds shouldBe Seq(21L)
    pruner.prune(filter).segments shouldBe pruned.segments
    counting.reads shouldBe 2
  }

  test("every relevant file must reliably prove a miss") {
    val root = Files.createTempDirectory("milvus-bloom-proof")
    val counting = new CountingObjectStore(new LocalObjectStore(root.toString))
    val miss = stats(100L, 1L)
    val candidate = Iterator
      .from(1000)
      .map(_.toLong)
      .find(value => !miss.mightContainLong(value))
      .get
    val hit = stats(100L, candidate)
    val wrongField = stats(101L, 1L)

    counting.write("stats/hit-a", miss.toBytes)
    counting.write("stats/hit-b", hit.toBytes)
    counting.write("stats/corrupt-a", miss.toBytes)
    counting.write(
      "stats/corrupt-b",
      "not-json".getBytes(StandardCharsets.UTF_8)
    )
    counting.write("stats/wrong", wrongField.toBytes)
    counting.write("stats/miss", miss.toBytes)
    counting.write(
      "stats/compound/1",
      s"[${miss.toJson},${miss.toJson}]".getBytes(StandardCharsets.UTF_8)
    )
    counting.write("stats/compound/9", hit.toBytes)

    val hitInSecondFile = segment(30L, 20L, Seq("stats/hit-a", "stats/hit-b"))
    val corruptFile =
      segment(31L, 20L, Seq("stats/corrupt-a", "stats/corrupt-b"))
    val mismatched = segment(32L, 20L, Seq("stats/wrong"))
    val provenMiss = segment(33L, 20L, Seq("stats/miss"))
    val compoundWins =
      segment(34L, 20L, Seq("stats/compound/9", "stats/compound/1"))
    val unknown = segment(35L, 20L, Seq.empty).copy(
      statistics = SegmentStatistics.Unknown
    )
    val otherBucket =
      segment(36L, 20L, Seq("s3a://other/stats/miss"))
    val fixed = snapshot(
      Seq(
        hitInSecondFile,
        corruptFile,
        mismatched,
        provenMiss,
        compoundWins,
        unknown,
        otherBucket
      ),
      bucket = "expected"
    )

    val pruned = PrimaryKeyBloomPruner
      .load(fixed, primaryKey, Map.empty, "expected", "", counting)
      .prune(
        PrimaryKeyFilter(
          100L,
          DataType.Int64,
          Set(LongValue(candidate))
        )
      )

    pruned.dataSegments.map(_.id) shouldBe Seq(30L, 31L, 32L, 35L, 36L)
    // The stale non-compound file is ignored when basename `1` is present.
    counting.keys should not contain "stats/compound/9"
  }

  private def stats(fieldId: Long, values: Long*): PrimaryKeyStats = {
    val builder = new PrimaryKeyStats.Builder(fieldId, DataType.Int64)
    values.foreach(builder.addLong)
    builder.build()
  }

  private def segment(
      id: Long,
      partitionId: Long,
      paths: Seq[String]
  ): Segment =
    Segment.v2(
      id,
      partitionId,
      rows = 1L,
      columnGroups = Seq(V2ColumnGroup(Seq(100L), Seq(s"data/$id"), Seq(1L))),
      statistics = SegmentStatistics.Listed(Map(100L -> paths))
    )

  private def l0(id: Long, partitionId: Long): Segment =
    Segment.v2(
      id,
      partitionId,
      rows = 0L,
      columnGroups = Seq.empty,
      deltaLogs = Seq(DeltaLogFile(id, s"delete/$id", 1L))
    )

  private def snapshot(
      segments: Seq[Segment],
      bucket: String = ""
  ): Snapshot = Snapshot(
    name = "fixed",
    collectionId = 10L,
    createdAt = None,
    schema = schema,
    partitionIds = segments.filter(_.hasData).map(_.partitionId).distinct,
    segments = segments,
    origin = SnapshotOrigin.Options,
    bucket = bucket
  )

  private final class CountingObjectStore(delegate: ObjectStore)
      extends ObjectStore {
    var reads = 0
    var keys = Vector.empty[String]

    override def readAll(key: String): Array[Byte] = {
      reads += 1
      keys :+= key
      delegate.readAll(key)
    }
    override def size(key: String): Long = delegate.size(key)
    override def list(key: String, recursive: Boolean): Seq[FileInfo] =
      delegate.list(key, recursive)
    override def exists(key: String): Boolean = delegate.exists(key)
    override def readAt(
        key: String,
        offset: Long,
        length: Long,
        fileSize: Long
    ): Array[Byte] = delegate.readAt(key, offset, length, fileSize)
    override def write(key: String, data: Array[Byte]): Unit =
      delegate.write(key, data)
    override def createDir(key: String, recursive: Boolean): Unit =
      delegate.createDir(key, recursive)
    override def delete(key: String): Unit = delegate.delete(key)
    override def close(): Unit = delegate.close()
  }
}
