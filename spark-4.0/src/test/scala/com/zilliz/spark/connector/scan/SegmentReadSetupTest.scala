package com.zilliz.spark.connector.scan

import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.delete.MilvusDeletePlan
import com.zilliz.milvus.storage.read.plan.{
  DeleteSource,
  InputSpec,
  SegmentLayout
}
import com.zilliz.milvus.storage.snapshot.V2ColumnGroup
import com.zilliz.spark.connector.options.MilvusOption
import io.milvus.grpc.schema.{CollectionSchema, DataType, FieldSchema}

/** The derivation both readers share.
  *
  * It was inside each row reader before, and the columnar reader would have
  * been a third and fourth copy. These cases pin the part that differs between
  * the two lines — what a column is called — and the part that does not.
  */
class SegmentReadSetupTest extends AnyFunSuite with Matchers {

  private val milvusSchema = CollectionSchema(
    name = "t",
    fields = Seq(
      FieldSchema(
        fieldID = 100L,
        name = "id",
        dataType = DataType.Int64,
        isPrimaryKey = true
      ),
      FieldSchema(fieldID = 1L, name = "Timestamp", dataType = DataType.Int64),
      FieldSchema(
        fieldID = 101L,
        name = "vec",
        dataType = DataType.FloatVector,
        // Every vector field carries its dim; the schema mapper needs it to
        // size the fixed-width blob.
        typeParams = Seq(io.milvus.grpc.common.KeyValuePair("dim", "4"))
      )
    )
  )

  private val schemaBytes = milvusSchema.toByteArray

  private val dataSchema = StructType(
    Seq(
      StructField("id", LongType),
      StructField("Timestamp", LongType),
      StructField("vec", ArrayType(FloatType))
    )
  )

  private val options = MilvusOption(
    new CaseInsensitiveStringMap(new java.util.HashMap[String, String]())
  )

  private def spec(
      layout: SegmentLayout,
      deletes: DeleteSource = DeleteSource.None
  ) = InputSpec(
    segmentId = 30L,
    partitionId = 20L,
    layout = layout,
    schemaBytes = schemaBytes,
    properties = Map("fs.storage_type" -> "local"),
    deletes = deletes
  )

  private val columnGroups = Seq(
    V2ColumnGroup(
      fieldIds = Seq(100L, 1L, 101L),
      filePaths = Seq("files/a.parquet"),
      fileRowCounts = Seq(10L)
    )
  )

  private def packed(deletes: DeleteSource = DeleteSource.None) =
    PackedV2ReadSetup(
      MilvusPackedV2InputPartition(
        spec(SegmentLayout.ColumnGroups(columnGroups), deletes),
        options
      ),
      dataSchema
    )

  private def loon(deletes: DeleteSource = DeleteSource.None) =
    LoonReadSetup(
      MilvusStorageV3InputPartition(
        spec(SegmentLayout.Manifest("files/seg"), deletes),
        "20",
        options
      ),
      dataSchema
    )

  // The manifest matches columns by field id; a column group uses the field's
  // own name. Getting this backwards means the native reader is asked for
  // columns that are not there.
  test("the two lines name columns differently") {
    packed().columnNameFor(101L) shouldBe Some("vec")
    loon().columnNameFor(101L) shouldBe Some("101")

    packed().pkColumnName shouldBe "id"
    loon().pkColumnName shouldBe "100"

    packed().timestampColumnName shouldBe "Timestamp"
    loon().timestampColumnName shouldBe "1"
  }

  // The schema mapper prepends the system fields, so this is about naming
  // style, not position.
  test("each line's arrow schema is named the same way as its columns") {
    import scala.collection.JavaConverters._
    val packedNames = packed().arrowSchema.getFields.asScala.map(_.getName)
    packedNames should contain allOf ("id", "vec")

    val loonNames = loon().arrowSchema.getFields.asScala.map(_.getName)
    loonNames should contain allOf ("100", "101")
    loonNames should not contain "vec"
  }

  test("without deletes only the requested columns are read") {
    loon().neededColumns should contain theSameElementsAs Seq("100", "1", "101")
    packed().neededColumns should contain allOf ("id", "vec")
  }

  // Deletes are keyed by primary key and timestamp, so both columns have to be
  // read even when nothing asked for them — which is what makes deletes cost
  // column pruning.
  test("deletes pull in the primary key and the timestamp") {
    val plan = MilvusDeletePlan.fromLongPks(Map(1L -> 100L))
    val narrow = StructType(Seq(StructField("vec", ArrayType(FloatType))))

    val withDeletes = LoonReadSetup(
      MilvusStorageV3InputPartition(
        spec(
          SegmentLayout.Manifest("files/seg"),
          DeleteSource.Materialized(plan)
        ),
        "20",
        options
      ),
      narrow
    )
    withDeletes.neededColumns should contain allOf ("100", "1")
    withDeletes.appliesDeletes shouldBe true

    val without = LoonReadSetup(
      MilvusStorageV3InputPartition(
        spec(SegmentLayout.Manifest("files/seg")),
        "20",
        options
      ),
      narrow
    )
    without.neededColumns should not contain "1"
    without.appliesDeletes shouldBe false
  }

  test("a setup is chosen by the partition's line") {
    SegmentReadSetup(
      MilvusPackedV2InputPartition(
        spec(SegmentLayout.ColumnGroups(columnGroups)),
        options
      ),
      dataSchema
    ) shouldBe a[PackedV2ReadSetup]

    SegmentReadSetup(
      MilvusStorageV3InputPartition(
        spec(SegmentLayout.Manifest("files/seg")),
        "20",
        options
      ),
      dataSchema
    ) shouldBe a[LoonReadSetup]
  }

  test("a partition with no deletes never calls the delete plan") {
    // isDeleted has to be cheap and safe on the common path: no primary key
    // lookup, no column lookup, no exception when neither is loaded.
    packed().isDeleted(null, 0) shouldBe false
    loon().isDeleted(null, 0) shouldBe false
  }
}
