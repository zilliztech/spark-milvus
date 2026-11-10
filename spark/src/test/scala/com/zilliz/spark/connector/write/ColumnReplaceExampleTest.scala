package com.zilliz.spark.connector.write

import java.nio.file.{Files, Path}
import java.util.{Comparator, UUID}

import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.types.{
  ArrayType,
  FloatType,
  LongType,
  StructField,
  StructType
}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

import com.zilliz.milvus.jni.storage.NativeStorageLibrary
import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.milvus.storage.io.NativeWritableObjectStore
import com.zilliz.milvus.storage.manifest.SnapshotSegmentFixture
import com.zilliz.milvus.storage.schema.FieldMetadata
import com.zilliz.milvus.storage.write.{
  ManifestTransaction,
  V3SegmentWriter,
  WriterResources
}
import com.zilliz.spark.connector.options.MilvusOption
import com.zilliz.spark.connector.types.RowIdentity
import io.milvus.grpc.common.KeyValuePair
import io.milvus.grpc.schema.{CollectionSchema, DataType, FieldSchema}

/** The four-row example of the column update design, end to end on local
  * storage: the update table matches ids 9 and 1, sets one score to NULL and
  * replaces both vectors; unmatched rows keep their values. The collection has
  * no index, as in the example.
  */
class ColumnReplaceExampleTest extends AnyFunSuite with BeforeAndAfterAll {
  private val mapper = new ObjectMapper()
  private var spark: SparkSession = _
  private var available = true

  private val collection = CollectionSchema(
    name = "column_demo",
    fields = Seq(
      FieldSchema(
        fieldID = 100L,
        name = "id",
        dataType = DataType.Int64,
        isPrimaryKey = true
      ),
      FieldSchema(
        fieldID = 101L,
        name = "score",
        dataType = DataType.Int64,
        nullable = true
      ),
      FieldSchema(
        fieldID = 102L,
        name = "embedding",
        dataType = DataType.FloatVector,
        typeParams = Seq(KeyValuePair("dim", "2"))
      )
    )
  )
  // Two segments of two rows each, in insertion order.
  private val segments = Seq(
    41L -> Seq((9L, 9L, Seq(0.9f, 0.1f)), (2L, 20L, Seq(0.2f, 0.3f))),
    42L -> Seq((7L, 70L, Seq(0.7f, 0.6f)), (1L, 1L, Seq(0.1f, 0.0f)))
  )

  override def beforeAll(): Unit = {
    val line = org.apache.spark.SPARK_VERSION.split('.').take(2).mkString(".")
    available = line != "3.5" || Runtime.version().feature() < 21
    if (available) {
      try NativeStorageLibrary.load()
      catch {
        case _: UnsatisfiedLinkError | _: NoClassDefFoundError |
            _: RuntimeException =>
          available = false
      }
    }
    if (available) {
      spark = SparkSession
        .builder()
        .master("local[2,2]")
        .appName("column-replace-example")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
      spark.sparkContext.setLogLevel("WARN")
    }
  }

  override def afterAll(): Unit = if (spark != null) spark.stop()

  /** Where one run of the example lives: a storage identity, the key prefix
    * every object of the run is under, and how Spark names a key there.
    */
  private final case class Place(
      storage: Map[String, String],
      prefix: String,
      path: String => String,
      sparkOptions: Map[String, String]
  ) {
    def key(name: String): String = prefix + name
    def options: Map[String, String] =
      sparkOptions + (MilvusOption.SnapshotPath -> path("snapshot.json"))
    def store: NativeWritableObjectStore =
      NativeWritableObjectStore.open(storage)
    def read(name: String): Array[Byte] = {
      val opened = store
      try opened.readAll(key(name))
      finally opened.close()
    }
    def write(name: String, bytes: Array[Byte]): Unit = {
      val opened = store
      try opened.write(key(name), bytes)
      finally opened.close()
    }
    def exists(name: String): Boolean = {
      val opened = store
      try opened.exists(key(name))
      finally opened.close()
    }
  }

  private def local(body: Place => Unit): Unit = {
    assume(
      available,
      "the native libraries and a compatible Arrow JVM are required"
    )
    val root = Files.createTempDirectory("column-replace-example-")
    try {
      val storage = StorageProperties.from(
        Map(
          StorageProperties.StorageType -> StorageProperties.StorageTypeLocal,
          StorageProperties.RootPath -> root.toString
        )
      )
      // Relative keys, resolved in the snapshot's own storage.
      body(prepare(Place(storage, "", identity, storage)))
    } finally {
      val paths = Files.walk(root)
      try paths.sorted(Comparator.reverseOrder[Path]()).forEach(Files.delete(_))
      finally paths.close()
    }
  }

  /** The prefix-scoped identity of the isolated object store fixture, which may
    * not list or probe the bucket outside its own prefix.
    */
  private def objectStorage(body: Place => Unit): Unit = {
    assume(
      available,
      "the native libraries and a compatible Arrow JVM are required"
    )
    val settings = Seq(
      "STORAGE_TEST_S3_ENDPOINT",
      "STORAGE_TEST_S3_BUCKET",
      "STORAGE_TEST_S3_TARGET_ACCESS_KEY",
      "STORAGE_TEST_S3_TARGET_SECRET_KEY"
    ).map(name => name -> sys.env.get(name).filter(_.nonEmpty))
    assume(
      settings.forall(_._2.nonEmpty),
      "an isolated object store from scripts/ci/input_storage.py is required"
    )
    val env = settings.map { case (name, value) => name -> value.get }.toMap
    val endpoint = env("STORAGE_TEST_S3_ENDPOINT")
    val bucket = env("STORAGE_TEST_S3_BUCKET")
    val storage = StorageProperties.from(
      Map(
        StorageProperties.StorageType -> "remote",
        StorageProperties.CloudProvider -> "aws",
        StorageProperties.Address -> endpoint
          .stripPrefix("http://")
          .stripPrefix("https://"),
        StorageProperties.UseSSL -> endpoint.startsWith("https://").toString,
        StorageProperties.BucketName -> bucket,
        StorageProperties.Region -> "us-east-1",
        StorageProperties.UseIam -> "false",
        StorageProperties.AccessKeyId -> env(
          "STORAGE_TEST_S3_TARGET_ACCESS_KEY"
        ),
        StorageProperties.AccessKeyValue -> env(
          "STORAGE_TEST_S3_TARGET_SECRET_KEY"
        )
      )
    )
    val prefix = s"target/${UUID.randomUUID()}/"
    body(
      prepare(
        Place(storage, prefix, key => s"s3a://$bucket/$prefix$key", storage)
      )
    )
  }

  /** The same store, but Spark is given no storage option: reads, the write and
    * the receipt all resolve the identity from the session's per-bucket Hadoop
    * settings, as a job configured by its runtime does.
    */
  private def objectStorageFromSession(body: Place => Unit): Unit =
    objectStorage { place =>
      val bucket = place.storage(StorageProperties.BucketName)
      val hadoop = spark.sparkContext.hadoopConfiguration
      val settings = Map(
        "endpoint" -> sys.env("STORAGE_TEST_S3_ENDPOINT"),
        "access.key" -> sys.env("STORAGE_TEST_S3_TARGET_ACCESS_KEY"),
        "secret.key" -> sys.env("STORAGE_TEST_S3_TARGET_SECRET_KEY"),
        "path.style.access" -> "true",
        "endpoint.region" -> "us-east-1"
      ).map { case (key, value) => s"fs.s3a.bucket.$bucket.$key" -> value }
      settings.foreach { case (key, value) => hadoop.set(key, value) }
      try body(place.copy(sparkOptions = Map.empty))
      finally settings.keys.foreach(hadoop.unset)
    }

  /** Writes the example's segments, their snapshot Avro records and the
    * snapshot file at `place`. Like Milvus, the file omits the empty index
    * list; the segment records say that no index files exist.
    */
  private def prepare(place: Place): Place = {
    val schema = WriteSchema.resolve(
      StructType(
        Seq(
          StructField("id", LongType, nullable = false),
          StructField("score", LongType, nullable = true),
          StructField("embedding", ArrayType(FloatType, containsNull = false))
        )
      ),
      collection,
      WriteSchema.Mode.Columns
    )
    val items = mapper.createArrayNode()
    val avros = mapper.createArrayNode()
    segments.foreach { case (segment, rows) =>
      val base = place.key(s"files/insert_log/90/80/$segment")
      val version = writeSegment(
        base,
        schema,
        place.storage,
        rows.map { case (id, score, vector) =>
          InternalRow(id, score, new GenericArrayData(vector.toArray[Any]))
        }
      )
      items
        .addObject()
        .put("segmentID", segment)
        .put(
          "manifest",
          mapper
            .createObjectNode()
            .put("ver", version)
            .put("base_path", base)
            .toString
        )
      val avro = s"files/snapshots/90/manifests/1/$segment.avro"
      place.write(
        avro,
        SnapshotSegmentFixture.encode(
          segmentId = segment,
          partitionId = 80L,
          rows = rows.size.toLong
        )
      )
      avros.add(place.key(avro))
    }
    val snapshot = mapper
      .readTree(
        """{
        "snapshot_info": {"name": "before_update", "id": 5, "collection_id": 90,
                          "partition_ids": [80], "create_ts": 100},
        "collection": {"schema": {"name": "column_demo", "fields": [
          {"fieldID": 100, "name": "id", "data_type": "Int64", "is_primary_key": true},
          {"fieldID": 101, "name": "score", "data_type": "Int64", "nullable": true},
          {"fieldID": 102, "name": "embedding", "data_type": "FloatVector",
           "type_params": [{"key": "dim", "value": "2"}]}]}},
        "format_version": 4
      }"""
      )
      .asInstanceOf[ObjectNode]
    snapshot.set("manifest_list", avros)
    snapshot.set("storagev2_manifest_list", items)
    place.write("snapshot.json", mapper.writeValueAsBytes(snapshot))
    place
  }

  private def writeSegment(
      base: String,
      schema: StructType,
      storage: Map[String, String],
      rows: Seq[InternalRow]
  ): Long = {
    val fieldIds = schema.fields
      .map(f =>
        f.name -> f.metadata.getLong(FieldMetadata.MilvusFieldIdMetadataKey)
      )
      .toMap
    val arrow =
      WriteSchema.arrowSchemaFor(schema, fieldIds, withSystemFields = false)
    val allocator = new RootAllocator(Long.MaxValue)
    WriterResources.closing(allocator.close()) {
      val batch = VectorSchemaRoot.create(arrow, allocator)
      WriterResources.closing(batch.close()) {
        batch.allocateNew()
        rows.zipWithIndex.foreach { case (row, index) =>
          ArrowWriter.internalRowToArrow(batch, index, row, schema)
        }
        batch.setRowCount(rows.size)
        val writer = new V3SegmentWriter(base, arrow, storage, allocator)
        WriterResources.closing(writer.close()) {
          writer.write(batch)
          val groups = writer.finish()
          WriterResources.closing(groups.close()) {
            ManifestTransaction.commit(
              base,
              storage,
              groups,
              ManifestTransaction.AppendFiles
            )
          }
        }
      }
    }
  }

  /** The example's update table and final-value rule, as in the design. */
  private def prepared(options: Map[String, String]): DataFrame = {
    val source = spark.read
      .format("milvus")
      .options(options)
      .option(MilvusOption.ReadMode, MilvusOption.ReadModePhysical)
      .option(MilvusOption.ReadIncludeRowId, "true")
      .option(MilvusOption.ReadVectorRaw, "false")
      .load()
      .select(RowIdentity.ColumnName, "id", "score", "embedding")
    val updates = spark
      .createDataFrame(
        spark.sparkContext.parallelize(
          Seq(Row(9L, 90L, Seq(0.9f, 0.8f)), Row(1L, null, Seq(0.1f, 0.2f)))
        ),
        StructType(
          Seq(
            StructField("id", LongType),
            StructField("new_score", LongType),
            StructField("new_embedding", ArrayType(FloatType))
          )
        )
      )
      .withColumn("_matched", lit(true))
    source
      .join(updates, Seq("id"), "left")
      .select(
        col(RowIdentity.ColumnName),
        when(col("_matched").isNotNull, col("new_score"))
          .otherwise(col("score"))
          .alias("score"),
        when(col("_matched").isNotNull, col("new_embedding"))
          .otherwise(col("embedding"))
          .alias("embedding")
      )
  }

  private def values(frame: DataFrame): Map[Long, (Option[Long], Seq[Float])] =
    frame
      .select("id", "score", "embedding")
      .collect()
      .map { row =>
        row.getLong(0) -> (
          (
            if (row.isNullAt(1)) None else Some(row.getLong(1)),
            row.getAs[scala.collection.Seq[Float]](2).toList
          )
        )
      }
      .toMap

  /** The example's run: write, read the receipt, then read the revisions it
    * committed through a snapshot that pins them.
    */
  private def runExample(place: Place): Unit = {
    val result = place.path("column-demo/run/write-result")
    prepared(place.options)
      .repartition(3)
      .write
      .format("milvus")
      .options(place.options)
      .option(MilvusOption.WriteOperation, "replace_columns")
      .mode("append")
      .save(result)

    val receipt = spark.read
      .format("milvus")
      .options(place.sparkOptions)
      .option(MilvusOption.ReadMode, MilvusOption.ReadModeWriteResult)
      .load(result)
      .collect()
    assert(receipt.length == 2)
    receipt.foreach { row =>
      assert(row.getAs[Long]("collection_id") == 90L)
      assert(row.getAs[Int]("schema_version") == 0)
      assert(row.getAs[Long]("source_version") == 1L)
      assert(row.getAs[Long]("version") == 2L)
      assert(row.getAs[Long]("row_count") == 2L)
      assert(
        row.getAs[scala.collection.Seq[String]]("changed_fields").toList ==
          List("score", "embedding")
      )
    }

    val after =
      mapper.readTree(place.read("snapshot.json")).asInstanceOf[ObjectNode]
    after
      .get("storagev2_manifest_list")
      .elements()
      .forEachRemaining { item =>
        val manifest = mapper.readTree(item.get("manifest").asText())
        item
          .asInstanceOf[ObjectNode]
          .put(
            "manifest",
            manifest.asInstanceOf[ObjectNode].put("ver", 2).toString
          )
      }
    place.write("snapshot-after.json", mapper.writeValueAsBytes(after))

    assert(
      values(
        spark.read
          .format("milvus")
          .options(
            place.options +
              (MilvusOption.SnapshotPath -> place.path("snapshot-after.json"))
          )
          .load()
      ) == Map(
        9L -> (Some(90L), List(0.9f, 0.8f)),
        1L -> (None, List(0.1f, 0.2f)),
        2L -> (Some(20L), List(0.2f, 0.3f)),
        7L -> (Some(70L), List(0.7f, 0.6f))
      )
    )
    // The source snapshot still reads the original values.
    assert(
      values(spark.read.format("milvus").options(place.options).load()) == Map(
        9L -> (Some(9L), List(0.9f, 0.1f)),
        1L -> (Some(1L), List(0.1f, 0.0f)),
        2L -> (Some(20L), List(0.2f, 0.3f)),
        7L -> (Some(70L), List(0.7f, 0.6f))
      )
    )
    // The address is taken: a second write fails before writing anything.
    val reuse = intercept[Exception](
      prepared(place.options).write
        .format("milvus")
        .options(place.options)
        .option(MilvusOption.WriteOperation, "replace_columns")
        .mode("append")
        .save(result)
    )
    assert(messages(reuse).contains("already belongs to another column write"))
  }

  test(
    "the design example replaces score and embedding and keeps unmatched rows"
  ) {
    local(runExample)
  }

  test("the design example runs on prefix-scoped object storage") {
    objectStorage(runExample)
  }

  test("the design example takes its storage identity from the session") {
    objectStorageFromSession(runExample)
  }

  private def messages(error: Throwable): String =
    Iterator
      .iterate[Throwable](error)(_.getCause)
      .takeWhile(_ != null)
      .map(e => Option(e.getMessage).getOrElse(""))
      .mkString(" | ")

  test("a NULL in a required vector field and a wrong dimension are refused") {
    local { place =>
      def attempt(
          embedding: org.apache.spark.sql.Column,
          name: String
      ): String = messages(
        intercept[Exception](
          prepared(place.options)
            .withColumn("embedding", embedding)
            .write
            .format("milvus")
            .options(place.options)
            .option(MilvusOption.WriteOperation, "replace_columns")
            .mode("append")
            .save(place.path(name))
        )
      )
      assert(
        attempt(lit(null).cast(ArrayType(FloatType)), "null")
          .contains("is NULL at offset")
      )
      assert(
        attempt(
          org.apache.spark.sql.functions.array(lit(1.0f), lit(2.0f), lit(3.0f)),
          "dimension"
        ).contains("expected 8 bytes, got 12")
      )
      // Neither attempt changed a manifest.
      segments.foreach { case (segment, _) =>
        assert(
          !place.exists(
            s"files/insert_log/90/80/$segment/_metadata/manifest-2.avro"
          )
        )
      }
    }
  }
}
