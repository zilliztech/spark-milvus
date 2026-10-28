package com.zilliz.spark.connector.testkit

import java.nio.file.Path

import com.zilliz.milvus.storage.io.ReadFixtures
import com.zilliz.spark.connector.options.MilvusOption
import io.milvus.grpc.common.KeyValuePair
import io.milvus.grpc.schema.{CollectionSchema, DataType, FieldSchema}

/** Fixed local collections read through `milvus.snapshot.path`. The tracked
  * fixtures contain native V3 segments, delta logs and persisted HNSW indexes;
  * loading verifies and copies their files without generating storage data.
  * Entity and Segment describe independent expected rows in the read tests.
  */
object LocalMilvusCollection {

  final case class Entity(
      id: Long,
      vector: Array[Float],
      category: Option[Long],
      floats: Option[Array[Float]] = None
  )

  final case class Segment(
      id: Long,
      entities: Seq[Entity],
      deletedIds: Seq[Long] = Seq.empty,
      indexMetric: Option[String] = Some("L2")
  )

  def schema(dimension: Int, floats: Boolean): CollectionSchema =
    CollectionSchema(
      name = "local-collection",
      fields = Seq(
        FieldSchema(
          fieldID = 100L,
          name = "id",
          dataType = DataType.Int64,
          isPrimaryKey = true
        ),
        FieldSchema(
          fieldID = 101L,
          name = "vector",
          dataType = DataType.FloatVector,
          typeParams = Seq(KeyValuePair("dim", dimension.toString))
        ),
        FieldSchema(
          fieldID = 102L,
          name = "category",
          dataType = DataType.Int64,
          nullable = true
        )
      ) ++ (if (floats)
              Some(
                FieldSchema(
                  fieldID = 103L,
                  name = "floats",
                  dataType = DataType.Array,
                  elementType = DataType.Float,
                  typeParams = Seq(KeyValuePair("max_capacity", "16")),
                  nullable = true
                )
              )
            else None)
    )

  def load(name: String, directory: Path): Map[String, String] = {
    ReadFixtures.copyTo(s"collections/$name", directory)
    Map(
      "fs.storage_type" -> "local",
      "fs.root_path" -> directory.toString,
      MilvusOption.SnapshotPath -> "snapshot.json",
      MilvusOption.ReadColumnar -> "true"
    )
  }
}
