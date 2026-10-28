package com.zilliz.milvus.storage.stats

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.expr._
import com.zilliz.milvus.storage.io.{
  FileInfo,
  LocalObjectStore,
  ObjectStore,
  ReadFixtures
}
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

  test("pruning retains matching data and its partition") {
    val root = Files.createTempDirectory("milvus-bloom-pruner")
    val store = new LocalObjectStore(root.toString)
    val counting = new CountingObjectStore(store)
    ReadFixtures.copyTo("stats/primary-key", root.resolve("fixture"))
    val candidate = 1000L
    val first = PrimaryKeyStats
      .fromBytes(
        Files.readAllBytes(
          root.resolve("fixture/long-1-2.json")
        )
      )
      .head
    first.mightContainLong(candidate) shouldBe false
    copy(root, "long-1-2.json", "stats/30")
    copy(root, "long-1000.json", "stats/31")

    val data20 = segment(30L, 20L, Seq("stats/30"))
    val data21 = segment(31L, 21L, Seq("stats/31"))
    val fixed = snapshot(Seq(data20, data21))
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
    val pruned = fixed.retainDataSegments(pruner.retainedSegmentIds(filter))
    pruned.segments shouldBe Seq(data21)
    pruned.partitionIds shouldBe Seq(21L)
    fixed
      .retainDataSegments(pruner.retainedSegmentIds(filter))
      .segments shouldBe pruned.segments
    counting.reads shouldBe 2
  }

  test("every relevant file must reliably prove a miss") {
    val root = Files.createTempDirectory("milvus-bloom-proof")
    val store = new LocalObjectStore(root.toString)
    val counting = new CountingObjectStore(store)
    ReadFixtures.copyTo("stats/primary-key", root.resolve("fixture"))
    val candidate = 1000L
    val miss = Files.readAllBytes(root.resolve("fixture/long-1.json"))
    PrimaryKeyStats
      .fromBytes(miss)
      .head
      .mightContainLong(candidate) shouldBe false
    copy(root, "long-1.json", "stats/hit-a")
    copy(root, "long-1000.json", "stats/hit-b")
    copy(root, "long-1.json", "stats/corrupt-a")
    Files.write(
      root.resolve("stats/corrupt-b"),
      "not-json".getBytes(StandardCharsets.UTF_8)
    )
    copy(root, "wrong-field.json", "stats/wrong")
    copy(root, "long-1.json", "stats/miss")
    Files.createDirectories(root.resolve("stats/compound"))
    val missJson = new String(miss, StandardCharsets.UTF_8)
    Files.write(
      root.resolve("stats/compound/1"),
      s"[$missJson,$missJson]".getBytes(StandardCharsets.UTF_8)
    )
    copy(root, "long-1000.json", "stats/compound/9")

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
      .retainedSegmentIds(
        PrimaryKeyFilter(
          100L,
          DataType.Int64,
          Set(LongValue(candidate))
        )
      )

    pruned shouldBe Set(30L, 31L, 32L, 35L, 36L)
    // The stale non-compound file is ignored when basename `1` is present.
    counting.keys should not contain "stats/compound/9"
  }

  private def copy(root: Path, source: String, destination: String): Unit = {
    val target = root.resolve(destination)
    Files.createDirectories(target.getParent)
    Files.copy(root.resolve("fixture").resolve(source), target)
  }

  test("listed statistics share cached pruning without a snapshot model") {
    val root = Files.createTempDirectory("milvus-listed-bloom")
    ReadFixtures.copyTo("stats/primary-key", root)
    val store = new CountingObjectStore(new LocalObjectStore(root.toString))
    val pruner = PrimaryKeyBloomPruner.loadListed(
      Map(
        30L -> Seq("long-1-2.json"),
        31L -> Seq("long-1000.json"),
        32L -> Seq.empty,
        33L -> Seq("missing.json")
      ),
      primaryKey,
      "",
      "",
      store
    )
    val filter = PrimaryKeyFilter(100L, DataType.Int64, Set(LongValue(1000L)))
    pruner.retainedSegmentIds(filter) shouldBe Set(31L, 32L, 33L)
    val reads = store.reads
    pruner.retainedSegmentIds(filter) shouldBe Set(31L, 32L, 33L)
    store.reads shouldBe reads
    pruner.retainedSegmentIds(filter.copy(values = Set.empty)) shouldBe empty
    PrimaryKeyBloomPruner
      .unavailable(Set(30L, 31L), primaryKey)
      .retainedSegmentIds(filter) shouldBe Set(30L, 31L)
  }

  private def segment(
      id: Long,
      partitionId: Long,
      paths: Seq[String]
  ): Segment =
    Segment(
      id,
      partitionId,
      storageVersion = 3,
      rows = Some(1L),
      layout = SegmentLayout.Manifest(s"data/$id", 1L),
      deletes = DeleteFiles.Empty,
      statistics = SegmentStatistics.Listed(Map(100L -> paths))
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
    override def close(): Unit = delegate.close()
  }
}
