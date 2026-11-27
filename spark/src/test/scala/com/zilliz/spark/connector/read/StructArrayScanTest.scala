package com.zilliz.spark.connector.read

import scala.jdk.CollectionConverters._

import org.apache.arrow.vector.types.pojo.{Field, FieldType, Schema}
import org.apache.spark.sql.types.{
  ArrayType,
  BinaryType,
  ByteType,
  FloatType,
  IntegerType,
  MetadataBuilder,
  StructField,
  StructType
}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalatest.funsuite.AnyFunSuite

import com.zilliz.milvus.storage.read.exec.PhysicalColumnGroup
import com.zilliz.milvus.storage.read.plan.SegmentReadTask
import com.zilliz.milvus.storage.schema.{FieldMetadata, SchemaMapper}
import com.zilliz.milvus.storage.snapshot.{
  SegmentLayout,
  Snapshot,
  SnapshotOrigin
}
import com.zilliz.spark.connector.options.MilvusOption
import com.zilliz.spark.connector.table.{MilvusTable, MilvusTables}
import io.milvus.grpc.common.KeyValuePair
import io.milvus.grpc.schema.{
  CollectionSchema,
  DataType,
  FieldSchema,
  StructArrayFieldSchema
}

class StructArrayScanTest extends AnyFunSuite {
  private val items = StructArrayFieldSchema(
    fieldID = 101,
    name = "items",
    nullable = true,
    fields = Seq(
      FieldSchema(
        name = "items[count]",
        fieldID = 102,
        dataType = DataType.Array,
        elementType = DataType.Int8,
        nullable = true
      ),
      FieldSchema(
        name = "items[embedding]",
        fieldID = 103,
        dataType = DataType.ArrayOfVector,
        elementType = DataType.FloatVector,
        nullable = true,
        elementNullable = true,
        typeParams = Seq(KeyValuePair(key = "dim", value = "2"))
      )
    )
  )
  private val snapshot = Snapshot(
    "s",
    10,
    None,
    CollectionSchema(
      fields = Seq(
        FieldSchema(
          fieldID = 100,
          name = "id",
          dataType = DataType.Int64,
          isPrimaryKey = true
        )
      ),
      structArrayFields = Seq(items)
    ),
    Nil,
    Nil,
    SnapshotOrigin.Options,
    ""
  )
  private val options = new CaseInsensitiveStringMap(
    Map("fs.storage_type" -> "local").asJava
  )
  private val output = MilvusTables.sparkSchemaOf(snapshot, rawVectors = false)
  private val fieldId = FieldMetadata.MilvusFieldIdMetadataKey

  test(
    "struct array inference retains member order and identities and applies raw vector reads to members"
  ) {
    val field = output("items")
    assert(field.nullable && field.metadata.getLong(fieldId) == 101)
    val array = field.dataType.asInstanceOf[ArrayType]
    assert(!array.containsNull)
    val members = array.elementType.asInstanceOf[StructType]
    assert(members.fieldNames.toSeq == Seq("count", "embedding"))
    assert(members("count").dataType == ByteType && !members("count").nullable)
    assert(members("count").metadata.getLong(fieldId) == 102)
    assert(
      members("embedding").dataType == ArrayType(FloatType, false) && members(
        "embedding"
      ).nullable
    )
    assert(
      members("embedding").metadata.getLong(
        FieldMetadata.MilvusVectorDimensionMetadataKey
      ) == 2
    )
    val raw = MilvusTables
      .sparkSchemaOf(snapshot, rawVectors = true)("items")
      .dataType
      .asInstanceOf[ArrayType]
      .elementType
      .asInstanceOf[StructType]
    assert(raw("embedding").dataType == BinaryType)
  }

  test(
    "external struct arrays recover nested metadata and reject conflicting member identity"
  ) {
    def external(id: Long): StructType = {
      val count = StructField(
        "count",
        ByteType,
        nullable = true,
        metadata = new MetadataBuilder()
          .putLong(fieldId, id)
          .putString("note", "caller")
          .build()
      )
      val embedding = StructField("embedding", ArrayType(FloatType))
      StructType(
        Seq(StructField("items", ArrayType(StructType(Seq(count, embedding)))))
      )
    }
    def rehydrate(id: Long) =
      MilvusTable(snapshot, MilvusOption(options), Some(external(id))).schema()
    val members = rehydrate(102)("items").dataType
      .asInstanceOf[ArrayType]
      .elementType
      .asInstanceOf[StructType]
    assert(
      !members("count").nullable && members("count").metadata.getString(
        "note"
      ) == "caller"
    )
    assert(
      members("embedding") == output("items").dataType
        .asInstanceOf[ArrayType]
        .elementType
        .asInstanceOf[StructType]("embedding")
    )
    assert(
      intercept[IllegalArgumentException](rehydrate(999)).getMessage
        .contains("milvus.field_id=999")
    )
  }

  test(
    "both inferred and explicit scan projections bind a parent to its physical leaves"
  ) {
    val selected = StructType(Seq(output("items")))
    val builder = new MilvusScanBuilder(output, options, snapshot)
    builder.pruneColumns(selected)
    val scan = builder.build().asInstanceOf[MilvusScan]
    assert(scan.neededFieldIds == Seq(102L, 103L))
    val task = SegmentReadTask(
      30,
      20,
      SegmentLayout.Manifest("base", 1),
      snapshot.schemaBytes,
      Map.empty
    )
    for (ids <- Seq(Seq.empty[Long], scan.neededFieldIds)) {
      val binding = V3ColumnBinding(task.copy(neededFieldIds = ids), selected)
      assert(binding.neededColumns == Seq("102", "103"))
      assert(!binding.arrowColumnNames.contains("items"))
      val resolve = binding.resolveManifestColumns.get
      assert(resolve(Set("1", "100", "102")) == Seq("102"))
      assert(resolve(Set("1", "100")) == Seq("1"))
      val error =
        intercept[IllegalStateException](resolve(Set("1", "100", "104")))
      assert(error.getMessage.contains("none of its current members"))
    }
    builder.pruneColumns(StructType(Seq(output("id"))))
    assert(builder.build().asInstanceOf[MilvusScan].neededFieldIds == Seq(100L))
  }

  test(
    "unannotated physical inputs distinguish a struct array from a synthetic position by its Spark type"
  ) {
    val name = MetadataColumns.SegmentId
    val parent = items.copy(name = name)
    val collection = snapshot.schema.copy(structArrayFields = Seq(parent))
    val array = StructField(name, output("items").dataType)
    assert(!MetadataColumns.isSynthetic(array))
    assert(
      ColumnBinding
        .structArrayParents(collection, StructType(Seq(array)))
        .get(name)
        .contains(parent)
    )
    val hidden = MetadataColumns
      .hiddenFields(StructType(Nil), collection)
      .find(_.name == name)
      .get
    assert(MetadataColumns.isSynthetic(hidden))
    assert(
      ColumnBinding
        .structArrayParents(collection, StructType(Seq(hidden)))
        .isEmpty
    )
  }

  test(
    "resolved physical groups distinguish an older segment from missing struct members using adapter field ids"
  ) {
    val selected = StructType(Seq(output("items")))
    val retired =
      FieldSchema(fieldID = 104, name = "retired", dataType = DataType.Int64)
    val storedSchema =
      snapshot.schema.copy(fields = snapshot.schema.fields :+ retired)
    val arrow = SchemaMapper.convertToArrowSchema(storedSchema)
    val fieldNames = SchemaMapper
      .physicalFields(storedSchema)
      .map(field => field.fieldID -> field.name)
      .toMap
    def binding(
        names: Seq[String],
        mapping: Map[Long, String] = fieldNames,
        withoutId: Option[String] = None
    ) = {
      val fields = arrow.getFields.asScala.map { field =>
        if (withoutId.contains(field.getName)) {
          val metadata = field.getMetadata.asScala.toMap - "PARQUET:field_id"
          new Field(
            field.getName,
            new FieldType(
              field.isNullable,
              field.getType,
              field.getDictionary,
              metadata.asJava
            ),
            field.getChildren
          )
        } else field
      }
      val layout = SegmentLayout.PhysicalColumnGroups(
        Seq(PhysicalColumnGroup(names, Seq("data/a"), Seq(3L))),
        new Schema(fields.asJava).toJson,
        mapping,
        Map.empty
      )
      PhysicalColumnBinding(
        SegmentReadTask(
          30,
          20,
          layout,
          snapshot.schemaBytes,
          Map.empty,
          neededFieldIds = Seq(102L, 103L)
        ),
        selected
      )
    }
    val older = Seq("Timestamp", "id")
    assert(binding(older).neededColumns == Seq("Timestamp"))
    assert(
      binding(older :+ "items[count]", fieldNames - 103L).neededColumns == Seq(
        "items[count]"
      )
    )
    assert(
      binding(older ++ Seq("retired", "items[count]")).neededColumns == Seq(
        "items[count]"
      )
    )
    val newer = older :+ "retired"
    val error = intercept[IllegalStateException](binding(newer).neededColumns)
    assert(error.getMessage.contains("none of its current members"))
    val identified = intercept[IllegalStateException](
      binding(newer, fieldNames - 104L).neededColumns
    )
    assert(identified.getMessage.contains("none of its current members"))
    val unresolved = intercept[IllegalStateException](
      binding(newer, fieldNames - 104L, Some("retired")).neededColumns
    )
    assert(
      unresolved.getMessage.contains(
        "lacks field ids for stored columns retired"
      )
    )
    assert(
      binding(
        newer :+ "items[count]",
        fieldNames - 104L,
        Some("retired")
      ).neededColumns ==
        Seq("items[count]")
    )
    val ambiguous = intercept[IllegalStateException](
      binding(
        older ++ Seq("items[count]", "items[embedding]"),
        fieldNames - 103L,
        Some("items[embedding]")
      ).neededColumns
    )
    assert(
      ambiguous.getMessage.contains(
        "lacks field ids for stored columns items[embedding]"
      )
    )
    val conflict = intercept[IllegalArgumentException](
      binding(
        older :+ "items[count]",
        (fieldNames - 102L).updated(103L, "items[count]")
      ).neededColumns
    )
    assert(conflict.getMessage.contains("conflicting mappings"))
  }

  test(
    "a StructArray projection keeps the stored Timestamp when a historical user field shares its name"
  ) {
    val historical = FieldSchema(
      fieldID = 104L,
      name = "Timestamp",
      dataType = DataType.Int32
    )
    val collection =
      snapshot.schema.copy(fields = snapshot.schema.fields :+ historical)
    val arrow = SchemaMapper.convertToArrowSchema(collection)
    val timestamps = arrow.getFields.asScala
      .filter(_.getName == "Timestamp")
      .map(field => field.getMetadata.get("PARQUET:field_id").toLong -> field)
      .toMap
    val systemId = SchemaMapper.TimestampFieldId
    assert(timestamps.keySet == Set(systemId, historical.fieldID))
    val members = items.fields.map(field => field.fieldID -> field.name).toMap
    val group = PhysicalColumnGroup(
      "Timestamp" +: items.fields.map(_.name),
      Seq("data/a"),
      Seq(3L)
    )
    val parent = StructType(Seq(output("items")))
    def bindingFor(ids: Seq[Long], projected: StructType = parent) = {
      val layout = SegmentLayout.PhysicalColumnGroups(
        Seq(group),
        arrow.toJson,
        members ++ ids.map(_ -> "Timestamp"),
        Map("Timestamp" -> "Timestamp", "_timestamp" -> "Timestamp")
      )
      PhysicalColumnBinding(
        SegmentReadTask(30, 20, layout, collection.toByteArray, Map.empty),
        projected
      )
    }
    for (
      (storedId, absentId) <- Seq(
        systemId -> historical.fieldID,
        historical.fieldID -> systemId
      )
    ) {
      val binding = bindingFor(Seq(storedId))
      assert(binding.neededColumns == items.fields.map(_.name))
      assert(binding.columnNameFor(storedId).contains("Timestamp"))
      assert(binding.columnNameFor(absentId).isEmpty)
      val physical = binding.arrowSchema.getFields.asScala
        .filter(_.getName == "Timestamp")
      assert(physical.toList == List(timestamps(storedId)))
    }
    for (ids <- Seq(Seq.empty[Long], Seq(systemId, historical.fieldID))) {
      val error =
        intercept[IllegalArgumentException](bindingFor(ids).neededColumns)
      assert(error.getMessage.contains("Timestamp"))
    }

    val withHistorical = parent.add(StructField("Timestamp", IntegerType))
    assert(
      bindingFor(Seq(historical.fieldID), withHistorical).neededColumns ==
        (items.fields.map(_.name) :+ "Timestamp")
    )
    val absent = intercept[IllegalArgumentException](
      bindingFor(Seq(systemId), withHistorical).neededColumns
    )
    assert(
      absent.getMessage.contains("Timestamp") ||
        absent.getMessage.contains(historical.fieldID.toString)
    )
  }

  test(
    "a projected ordinary column with a physical alias can accompany a newly added struct member"
  ) {
    val arrow = SchemaMapper.convertToArrowSchema(snapshot.schema)
    val id = Field.notNullable("stored_id", arrow.findField("id").getType)
    val count = arrow.findField("items[count]")
    val stored = Seq(id, count)
    val layout = SegmentLayout.PhysicalColumnGroups(
      Seq(PhysicalColumnGroup(stored.map(_.getName), Seq("data/a"), Seq(3L))),
      new Schema(stored.asJava).toJson,
      Map(102L -> count.getName),
      Map("id" -> id.getName)
    )
    val selected =
      StructType(Seq(StructField("id", output("id").dataType), output("items")))
    val binding = PhysicalColumnBinding(
      SegmentReadTask(30, 20, layout, snapshot.schemaBytes, Map.empty),
      selected
    )
    assert(binding.neededColumns == Seq("stored_id", "items[count]"))
  }
}
