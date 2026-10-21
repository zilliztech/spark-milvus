package com.zilliz.spark.connector.read

import java.nio.file.Files
import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.types.pojo.Schema
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.jni.storage.NativeStorageLibrary
import com.zilliz.milvus.storage.delete.DeletePlan
import com.zilliz.milvus.storage.read.plan.{DeleteSource, SegmentReadTask}
import com.zilliz.milvus.storage.snapshot.{DeltaLogFile, SegmentLayout}
import com.zilliz.milvus.storage.snapshot.V2ColumnGroup
import com.zilliz.milvus.storage.write.codec.BinlogEncoder
import com.zilliz.spark.connector.options.MilvusOption
import io.milvus.grpc.schema.{CollectionSchema, DataType, FieldSchema}

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
    ColumnBinding(
      MilvusInputPartition(
        task(SegmentLayout.ColumnGroups(columnGroups), deletes),
        options.milvusFilter
      ),
      dataSchema
    )

  private def v3(deletes: DeleteSource = DeleteSource.None) =
    ColumnBinding(
      MilvusInputPartition(
        task(SegmentLayout.Manifest("files/seg"), deletes),
        options.milvusFilter
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
    v3().arrowColumnFor(
      MilvusOption.MilvusExtraColumnTimestamp
    ) shouldBe "1"
  }

  // A user field named "timestamp" is not field 1; the schema handed to the
  // native reader must still hold field 1, which delete application asks for.
  test("a user field named timestamp does not displace stored field 1") {
    val schema = CollectionSchema(
      name = "t",
      fields = Seq(
        FieldSchema(
          fieldID = 100L,
          name = "id",
          dataType = DataType.Int64,
          isPrimaryKey = true
        ),
        FieldSchema(
          fieldID = 101L,
          name = "timestamp",
          dataType = DataType.Int64
        ),
        FieldSchema(
          fieldID = 102L,
          name = "vec",
          dataType = DataType.FloatVector,
          typeParams = Seq(io.milvus.grpc.common.KeyValuePair("dim", "4"))
        )
      )
    )
    val deletes =
      DeleteSource.Materialized(DeletePlan.fromLongPks(Map(1L -> 100L)))
    val wanted = StructType(Seq(StructField("vec", ArrayType(FloatType))))
    val v3Binding = ColumnBinding(
      MilvusInputPartition(
        task(SegmentLayout.Manifest("files/seg"), deletes)
          .copy(schemaBytes = schema.toByteArray),
        options.milvusFilter
      ),
      wanted
    )
    v3Binding.neededColumns should contain("1")
    v3Binding.arrowSchema.getFields.asScala.map(_.getName) should contain("1")
    v3Binding.arrowSchema.getFields.asScala.map(_.getName) should contain("101")

    val v2Binding = ColumnBinding(
      MilvusInputPartition(
        task(
          SegmentLayout.ColumnGroups(
            Seq(
              V2ColumnGroup(
                fieldIds = Seq(100L, 1L, 101L, 102L),
                filePaths = Seq("files/a.parquet"),
                fileRowCounts = Seq(10L)
              )
            )
          ),
          deletes
        ).copy(schemaBytes = schema.toByteArray),
        options.milvusFilter
      ),
      wanted
    )
    v2Binding.neededColumns should contain("Timestamp")
    v2Binding.arrowSchema.getFields.asScala.map(_.getName) should contain(
      "Timestamp"
    )
  }

  test("a dynamic field keeps its recorded id across both storage lines") {
    val dynamicField = FieldSchema(
      fieldID = 407L,
      name = "$meta",
      dataType = DataType.JSON,
      isDynamic = true,
      nullable = true
    )
    val dynamicSchema = milvusSchema.copy(
      enableDynamicField = true,
      fields = milvusSchema.fields :+ dynamicField
    )
    val outputSchema = StructType(Seq(StructField("$meta", StringType)))

    val v2Task = task(
      SegmentLayout.ColumnGroups(
        Seq(
          V2ColumnGroup(
            fieldIds = Seq(407L),
            filePaths = Seq("files/meta.parquet"),
            fileRowCounts = Seq(10L)
          )
        )
      )
    ).copy(
      schemaBytes = dynamicSchema.toByteArray,
      neededFieldIds = Seq(407L)
    )
    val v2Binding = ColumnBinding(
      MilvusInputPartition(v2Task, options.milvusFilter),
      outputSchema
    )

    val v3Task = task(SegmentLayout.Manifest("files/seg")).copy(
      schemaBytes = dynamicSchema.toByteArray,
      neededFieldIds = Seq(407L)
    )
    val v3Binding = ColumnBinding(
      MilvusInputPartition(v3Task, options.milvusFilter),
      outputSchema
    )

    v2Binding.columnNameFor(407L) shouldBe Some("$meta")
    v2Binding.arrowColumnFor("$meta") shouldBe "$meta"
    v2Binding.neededColumns shouldBe Seq("$meta")
    v3Binding.arrowColumnFor("$meta") shouldBe "407"
    v3Binding.neededColumns shouldBe Seq("407")
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

  test("task field ids include a projected-away physical dependency") {
    val projectedTask = task(SegmentLayout.Manifest("files/seg")).copy(
      neededFieldIds = Seq(101L)
    )
    val binding = ColumnBinding(
      MilvusInputPartition(projectedTask, options.milvusFilter),
      StructType(Seq(StructField("id", LongType)))
    )

    binding.neededColumns shouldBe Seq("101")
  }

  test("the V2 timestamp alias follows the schema's physical field name") {
    val renamedTimestamp = milvusSchema.copy(fields = milvusSchema.fields.map {
      case field if field.fieldID == 1L => field.copy(name = "ts")
      case field                        => field
    })
    val projectedTask = task(
      SegmentLayout.ColumnGroups(
        Seq(
          V2ColumnGroup(
            fieldIds = Seq(1L),
            filePaths = Seq("files/ts.parquet"),
            fileRowCounts = Seq(10L)
          )
        )
      )
    ).copy(schemaBytes = renamedTimestamp.toByteArray)
    val binding = ColumnBinding(
      MilvusInputPartition(projectedTask, options.milvusFilter),
      StructType(
        Seq(
          StructField(MilvusOption.MilvusExtraColumnTimestamp, LongType)
        )
      )
    )

    binding.arrowColumnFor(
      MilvusOption.MilvusExtraColumnTimestamp
    ) shouldBe "ts"
    binding.timestampColumnName shouldBe "ts"
    binding.neededColumns shouldBe Seq("ts")
  }

  test("the V3 timestamp metadata column reads stored field id 1") {
    val binding = ColumnBinding(
      MilvusInputPartition(
        task(SegmentLayout.Manifest("files/seg")),
        options.milvusFilter
      ),
      StructType(
        Seq(
          StructField(MilvusOption.MilvusExtraColumnTimestamp, LongType)
        )
      )
    )

    binding.arrowColumnFor(
      MilvusOption.MilvusExtraColumnTimestamp
    ) shouldBe "1"
    binding.neededColumns shouldBe Seq("1")
  }

  // Deletes are keyed by primary key and timestamp, so both columns have to be
  // read even when nothing asked for them — which is what makes deletes cost
  // column pruning.
  test("deletes pull in the primary key and the timestamp") {
    val plan = DeletePlan.fromLongPks(Map(1L -> 100L))
    val narrow = StructType(Seq(StructField("vec", ArrayType(FloatType))))

    val withDeletes = ColumnBinding(
      MilvusInputPartition(
        task(
          SegmentLayout.Manifest("files/seg"),
          DeleteSource.Materialized(plan)
        ),
        options.milvusFilter
      ),
      narrow
    )
    withDeletes.neededColumns should contain allOf ("100", "1")
    withDeletes.appliesDeletes shouldBe true

    val without = ColumnBinding(
      MilvusInputPartition(
        task(SegmentLayout.Manifest("files/seg")),
        options.milvusFilter
      ),
      narrow
    )
    without.neededColumns should not contain "1"
    without.appliesDeletes shouldBe false
  }

  test("column binding is chosen by the task layout") {
    ColumnBinding(
      MilvusInputPartition(
        task(SegmentLayout.ColumnGroups(columnGroups)),
        options.milvusFilter
      ),
      dataSchema
    ) shouldBe a[V2ColumnBinding]

    ColumnBinding(
      MilvusInputPartition(
        task(SegmentLayout.Manifest("files/seg")),
        options.milvusFilter
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

  test("valid empty delete files keep a narrow V2 projection readable") {
    try NativeStorageLibrary.load()
    catch {
      case missing: UnsatisfiedLinkError =>
        cancel("the native storage library is not available", missing)
    }
    val directory = Files.createTempDirectory("empty-delete-plan-")
    val file = directory.resolve("empty.binlog")
    val bytes = BinlogEncoder.envelope(
      2,
      10L,
      20L,
      30L,
      100L,
      DataType.Int64.value,
      new ObjectMapper().createObjectNode().put("version", "MULTI_FIELD"),
      Array.emptyByteArray
    )
    def binding = ColumnBinding(
      MilvusInputPartition(
        task(
          SegmentLayout.ColumnGroups(columnGroups),
          DeleteSource.Files(Seq(DeltaLogFile(1L, "empty.binlog", 0L)))
        ).copy(
          properties = Map(
            "fs.storage_type" -> "local",
            "fs.root_path" -> directory.toString
          ),
          neededFieldIds = Seq(101L)
        ),
        options.milvusFilter
      ),
      StructType(Seq(dataSchema("vec")))
    )
    try {
      Files.write(file, bytes)
      val setup = binding
      setup.appliesDeletes shouldBe true
      setup.neededColumns shouldBe Seq("vec")
      setup.deletePlan.isEmpty shouldBe true
      val allocator = new RootAllocator()
      val root = VectorSchemaRoot.create(
        new Schema(
          setup.arrowSchema.getFields.asScala.filter(_.getName == "vec").asJava
        ),
        allocator
      )
      try {
        root.allocateNew()
        root.setRowCount(2)
        (0 until root.getRowCount).foreach(row =>
          setup.isDeleted(root, row) shouldBe false
        )
      } finally {
        root.close()
        allocator.close()
      }
      Files.write(file, bytes.take(10))
      an[IllegalStateException] should be thrownBy binding.isDeleted(null, 0)
      Files.delete(file)
      an[IllegalStateException] should be thrownBy binding.isDeleted(null, 0)
    } finally {
      Files.deleteIfExists(file)
      Files.delete(directory)
    }
  }
}
