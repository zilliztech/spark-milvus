package com.zilliz.spark.connector.read

import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.delete.DeletePlan
import com.zilliz.milvus.storage.read.plan.{DeleteSource, SegmentReadTask}
import com.zilliz.milvus.storage.snapshot.V2ColumnGroup
import com.zilliz.spark.connector.options.MilvusOption
import io.milvus.grpc.schema.{CollectionSchema, DataType, FieldSchema}
import com.zilliz.milvus.storage.snapshot.SegmentLayout

/** The derivation both readers share.
  *
  * It was inside each row reader before, and the columnar reader would have
  * been a third and fourth copy. These cases pin the part that differs between
  * the two lines — what a column is called — and the part that does not.
  */
class ColumnBindingTest extends AnyFunSuite with Matchers {

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

  private def task(
      layout: SegmentLayout,
      deletes: DeleteSource = DeleteSource.None
  ) = SegmentReadTask(
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

  private def v2(deletes: DeleteSource = DeleteSource.None) =
    V2ColumnBinding(
      MilvusV2InputPartition(
        task(SegmentLayout.ColumnGroups(columnGroups), deletes),
        options
      ),
      dataSchema
    )

  private def v3(deletes: DeleteSource = DeleteSource.None) =
    V3ColumnBinding(
      MilvusV3InputPartition(
        task(SegmentLayout.Manifest("files/seg"), deletes),
        "20",
        options
      ),
      dataSchema
    )

  // The manifest matches columns by field id; a column group uses the field's
  // own name. Getting this backwards means the native reader is asked for
  // columns that are not there.
  test("the two lines name columns differently") {
    v2().columnNameFor(101L) shouldBe Some("vec")
    v3().columnNameFor(101L) shouldBe Some("101")

    v2().pkColumnName shouldBe "id"
    v3().pkColumnName shouldBe "100"

    v2().timestampColumnName shouldBe "Timestamp"
    v3().timestampColumnName shouldBe "1"
  }

  // The schema mapper prepends the system fields, so this is about naming
  // style, not position.
  test("each line's arrow schema is named the same way as its columns") {
    import scala.collection.JavaConverters._
    val v2Names = v2().arrowSchema.getFields.asScala.map(_.getName)
    v2Names should contain allOf ("id", "vec")

    val v3Names = v3().arrowSchema.getFields.asScala.map(_.getName)
    v3Names should contain allOf ("100", "101")
    v3Names should not contain "vec"
  }

  test("without deletes only the requested columns are read") {
    v3().neededColumns should contain theSameElementsAs Seq("100", "1", "101")
    v2().neededColumns should contain allOf ("id", "vec")
  }

  // Deletes are keyed by primary key and timestamp, so both columns have to be
  // read even when nothing asked for them — which is what makes deletes cost
  // column pruning.
  test("deletes pull in the primary key and the timestamp") {
    val plan = DeletePlan.fromLongPks(Map(1L -> 100L))
    val narrow = StructType(Seq(StructField("vec", ArrayType(FloatType))))

    val withDeletes = V3ColumnBinding(
      MilvusV3InputPartition(
        task(
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

    val without = V3ColumnBinding(
      MilvusV3InputPartition(
        task(SegmentLayout.Manifest("files/seg")),
        "20",
        options
      ),
      narrow
    )
    without.neededColumns should not contain "1"
    without.appliesDeletes shouldBe false
  }

  test("a setup is chosen by the partition's line") {
    ColumnBinding(
      MilvusV2InputPartition(
        task(SegmentLayout.ColumnGroups(columnGroups)),
        options
      ),
      dataSchema
    ) shouldBe a[V2ColumnBinding]

    ColumnBinding(
      MilvusV3InputPartition(
        task(SegmentLayout.Manifest("files/seg")),
        "20",
        options
      ),
      dataSchema
    ) shouldBe a[V3ColumnBinding]
  }

  test("a partition with no deletes never calls the delete plan") {
    // isDeleted has to be cheap and safe on the common path: no primary key
    // lookup, no column lookup, no exception when neither is loaded.
    v2().isDeleted(null, 0) shouldBe false
    v3().isDeleted(null, 0) shouldBe false
  }
}
