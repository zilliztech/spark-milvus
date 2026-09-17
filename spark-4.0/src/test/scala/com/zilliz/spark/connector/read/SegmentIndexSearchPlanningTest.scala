package com.zilliz.spark.connector.read

import scala.collection.JavaConverters._

import org.apache.spark.sql.connector.expressions.{Expression, Expressions}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funsuite.AnyFunSuite

import com.zilliz.milvus.storage.read.plan.SegmentReadTask
import com.zilliz.milvus.storage.schema.FieldMetadata
import com.zilliz.milvus.storage.snapshot.{
  Segment,
  SegmentIndexes,
  SegmentLayout,
  Snapshot,
  SnapshotOrigin,
  V2ColumnGroup
}
import com.zilliz.spark.connector.options.{MilvusOption, VectorSearch}
import io.milvus.grpc.common.KeyValuePair
import io.milvus.grpc.schema.{CollectionSchema, DataType, FieldSchema}

/** Planning failures must be observable before storage is opened. */
class SegmentIndexSearchPlanningTest extends AnyFunSuite {
  private val vector = FieldSchema(
    fieldID = 101L,
    name = "vector",
    dataType = DataType.FloatVector,
    typeParams = Seq(KeyValuePair("dim", "2"))
  )
  private val collection = CollectionSchema(
    name = "c",
    fields = Seq(
      FieldSchema(
        fieldID = 100L,
        name = "id",
        dataType = DataType.Int64,
        isPrimaryKey = true
      ),
      vector
    )
  )
  private val snapshot = Snapshot(
    "s",
    1L,
    None,
    collection,
    Seq(2L),
    Seq.empty,
    SnapshotOrigin.Options,
    ""
  )
  private val query =
    VectorSearch(Array(1f, 0f), 2, "COSINE", "vector", mode = "index")
  private val options = Map(
    MilvusOption.VectorSearchMode -> "index",
    MilvusOption.VectorSearchQueryVector -> "[1,0]",
    MilvusOption.VectorSearchTopK -> "2",
    MilvusOption.VectorSearchMetric -> "COSINE",
    MilvusOption.ReadColumnar -> "true"
  )

  test(
    "index planning rejects unsupported fields, dimensions and filter semantics"
  ) {
    SegmentIndexSearch.validate(query, collection)
    Seq(
      query.copy(vectorColumn = "missing"),
      query.copy(queryVector = Array(1f)),
      query.copy(queryVector = Array(0f, 0f)),
      query.copy(queryVector = Array(Float.NaN, 0f)),
      query.copy(filter = Some("missing == 1")),
      query.copy(filter = Some("vector == 1")),
      query.copy(filter = Some("id == '1'")),
      query.copy(searchParameters = Map("nprobe" -> "8")),
      query.copy(searchParameters = Map("ef" -> "1"))
    ).foreach { invalid =>
      intercept[IllegalArgumentException](
        SegmentIndexSearch.validate(invalid, collection)
      )
    }
    Seq(
      vector.copy(nullable = true),
      vector.copy(dataType = DataType.BinaryVector)
    ).foreach { invalid =>
      intercept[IllegalArgumentException](
        SegmentIndexSearch.validate(
          query,
          collection.copy(fields = Seq(collection.fields.head, invalid))
        )
      )
    }
  }

  test(
    "index column pruning preserves requested score position and omits the raw vector"
  ) {
    val full = StructType(
      Seq(
        StructField("id", LongType),
        StructField("vector", ArrayType(FloatType)),
        StructField("_score", DoubleType)
      )
    )
    Seq(
      StructType(Seq(full("_score"), full("id"))),
      StructType(Seq(full("id"))),
      StructType(Seq(full("_score"))),
      StructType(Seq.empty)
    ).foreach { projection =>
      val builder = new MilvusScanBuilder(
        full,
        new CaseInsensitiveStringMap(options.asJava),
        snapshot
      )
      builder.pruneColumns(projection)
      assert(builder.build().readSchema() == projection)
      val predicates = Array(
        new Predicate(
          "=",
          Array[Expression](Expressions.column("id"), Expressions.literal(1L))
        )
      )
      assert(builder.pushPredicates(predicates).sameElements(predicates))
      assert(builder.pushedPredicates().isEmpty)
    }
  }

  test("index pruning validates names against the fixed snapshot schema") {
    val id = StructField(
      "id",
      LongType,
      nullable = false,
      metadata = new MetadataBuilder()
        .putLong(FieldMetadata.MilvusFieldIdMetadataKey, 100L)
        .build()
    )
    val builder = new MilvusScanBuilder(
      StructType(Seq(id, StructField("_score", DoubleType))),
      new CaseInsensitiveStringMap(options.asJava),
      snapshot
    )
    builder.pruneColumns(StructType(Seq(StructField("id", StringType))))
    assert(builder.build().readSchema() == StructType(Seq(id)))
    intercept[IllegalArgumentException] {
      builder.pruneColumns(StructType(Seq(StructField("absent", LongType))))
    }
  }

  test("index pruning uses the normalized search mode") {
    val score = StructField("_score", DoubleType, nullable = false)
    val configured = options.updated(MilvusOption.VectorSearchMode, " index ")
    val builder = new MilvusScanBuilder(
      StructType(Seq(StructField("id", LongType), score)),
      new CaseInsensitiveStringMap(configured.asJava),
      snapshot
    )
    builder.pruneColumns(StructType(Seq(score)))
    assert(builder.build().readSchema() == StructType(Seq(score)))
  }

  // Whether a segment can serve the search is known on the driver: the
  // snapshot names its builds, says it has none, or says nothing. A segment
  // that cannot serve it fails planning instead of failing in every task.
  test("index search refuses unusable segments while planning") {
    def partitionsOver(segment: Segment, allowUnindexed: Boolean) = {
      val configured =
        if (allowUnindexed)
          options.updated(MilvusOption.VectorSearchAllowUnindexed, "true")
        else options
      val scan = new MilvusScanBuilder(
        StructType(
          Seq(
            StructField("id", LongType),
            StructField("_score", DoubleType, nullable = false)
          )
        ),
        new CaseInsensitiveStringMap(configured.asJava),
        snapshot
      ).build().asInstanceOf[MilvusScan]
      scan.inputPartitions(snapshot.copy(segments = Seq(segment)))
    }
    val segment = Segment.v2(
      3L,
      2L,
      3L,
      Seq(V2ColumnGroup(Seq(100L, 101L), Seq("absent.parquet"), Seq(3L)))
    )
    val unknown = intercept[IllegalArgumentException](
      partitionsOver(segment, allowUnindexed = false)
    )
    assert(unknown.getMessage.contains("segment 3"))
    val unindexed = segment.copy(indexes = SegmentIndexes.Unindexed)
    intercept[IllegalArgumentException](
      partitionsOver(unindexed, allowUnindexed = false)
    )
    assert(partitionsOver(unindexed, allowUnindexed = true).length == 1)
  }

  test("both V2 and V3 index partitions force the search row reader") {
    val task = SegmentReadTask(
      3L,
      2L,
      SegmentLayout.ColumnGroups(
        Seq(V2ColumnGroup(Seq(100L, 101L), Seq("absent.parquet"), Seq(3L)))
      ),
      collection.toByteArray,
      Map.empty
    )
    val configuration = MilvusOption(options)
    val factory = new MilvusPartitionReaderFactory(
      StructType(Seq(StructField("id", LongType))),
      options
    )
    assert(
      !factory.supportColumnarReads(MilvusV2InputPartition(task, configuration))
    )
    assert(
      !factory.supportColumnarReads(
        MilvusV3InputPartition(
          task.copy(layout = SegmentLayout.Manifest("absent", 1)),
          "2",
          configuration
        )
      )
    )
  }

  test(
    "malformed or incomplete search options cannot become an ordinary scan"
  ) {
    Seq(
      Map(MilvusOption.VectorSearchMode -> "index"),
      options - MilvusOption.VectorSearchTopK,
      options - MilvusOption.VectorSearchQueryVector,
      options.updated(MilvusOption.VectorSearchMode, "approximate"),
      options.updated(MilvusOption.VectorSearchQueryVector, "1,0"),
      options.updated(MilvusOption.VectorSearchQueryVector, "[1,0"),
      options.updated(MilvusOption.VectorSearchQueryVector, "[1,\"0\"]"),
      options.updated(MilvusOption.VectorSearchQueryVector, "[1e1000,0]"),
      options.updated(MilvusOption.VectorSearchQueryVector, "[1,0] [2]"),
      options.updated(MilvusOption.VectorSearchQueryVector, "[1,0] garbage"),
      options.updated(MilvusOption.VectorSearchParameters, "[]"),
      options.updated(MilvusOption.VectorSearchParameters, "{} {}"),
      options.updated(MilvusOption.VectorSearchAllowUnindexed, "yes"),
      options
        .updated(MilvusOption.VectorSearchMode, "brute_force")
        .updated(MilvusOption.VectorSearchFilter, "id > 1")
    ).foreach { invalid =>
      intercept[Exception](MilvusOption(invalid))
    }
    assert(
      MilvusOption(
        options.updated(MilvusOption.VectorSearchFilter, "")
      ).vectorSearch.get.filter.isEmpty
    )
    assert(MilvusOption(Map.empty[String, String]).vectorSearch.isEmpty)
  }
}
