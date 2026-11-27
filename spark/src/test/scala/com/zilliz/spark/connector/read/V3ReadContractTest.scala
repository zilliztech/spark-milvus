package com.zilliz.spark.connector.read

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}
import java.util.{Collections, Comparator}
import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.avro.file.{DataFileStream, DataFileWriter}
import org.apache.avro.generic.{
  GenericDatumReader,
  GenericDatumWriter,
  GenericRecord
}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import org.apache.spark.TaskContext
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

import com.zilliz.milvus.jni.storage.NativeStorageLibrary
import com.zilliz.milvus.storage.manifest.{
  SnapshotSegmentFixture,
  SnapshotSegmentSchema
}
import com.zilliz.spark.connector.options.MilvusOption
import com.zilliz.spark.connector.testkit.LocalMilvusCollection
import com.zilliz.spark.connector.types.RowIdentity

/** Public file reads against frozen V3 data. Every destructive case changes
  * only a temporary copy; no writer or online service prepares it.
  */
class V3ReadContractTest extends AnyFunSuite with BeforeAndAfterAll {
  private val mapper = new ObjectMapper()
  private var spark: SparkSession = _
  private var available = true
  private val avro = "files/snapshots/10/manifests/1/30.avro"
  private val manifest = "files/insert_log/10/20/30/_metadata/manifest-2.avro"
  private val delete = "files/insert_log/10/20/30/_delta/delete.parquet"
  private val liveIds = ((3000L until 3008L) ++ (3100L until 3108L))
    .filterNot(Set(3002L, 3105L))
    .sorted

  override def beforeAll(): Unit = {
    val line = org.apache.spark.SPARK_VERSION.split('.').take(2).mkString(".")
    available = line != "3.5" || Runtime.version().feature() < 21
    if (available) {
      try {
        NativeStorageLibrary.load()
      } catch {
        case _: UnsatisfiedLinkError | _: NoClassDefFoundError |
            _: RuntimeException =>
          available = false
      }
    }
    if (available) {
      spark = SparkSession
        .builder()
        .master("local[2,2]")
        .appName("v3-read-contract")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
      spark.sparkContext.setLogLevel("WARN")
    }
  }

  override def afterAll(): Unit = if (spark != null) spark.stop()

  private def fixture(body: (Path, Map[String, String]) => Unit): Unit = {
    withCollection("basic")(body)
  }

  private def withCollection(name: String)(
      body: (Path, Map[String, String]) => Unit
  ): Unit = {
    assume(
      available,
      "the native libraries and a compatible Arrow JVM are required"
    )
    val root = Files.createTempDirectory("v3-read-contract-")
    try body(root, LocalMilvusCollection.load(name, root))
    finally {
      val paths = Files.walk(root)
      try paths.sorted(Comparator.reverseOrder[Path]()).forEach(Files.delete(_))
      finally paths.close()
    }
  }

  private def read(options: Map[String, String]): DataFrame =
    spark.read.format("milvus").options(options).load()

  private def ids(frame: DataFrame): Seq[Long] =
    frame.select("id").collect().map(_.getLong(0)).toSeq.sorted

  private def metadata(root: Path)(change: ObjectNode => Unit): Unit = {
    val path = root.resolve("snapshot.json")
    val json =
      mapper.readTree(Files.readAllBytes(path)).asInstanceOf[ObjectNode]
    change(json)
    Files.write(path, mapper.writeValueAsBytes(json))
  }

  private def renameCategory(root: Path, name: String): Unit =
    metadata(root) { json =>
      json
        .at("/collection/schema/fields")
        .elements()
        .asScala
        .find(_.get("name").asText() == "category")
        .get
        .asInstanceOf[ObjectNode]
        .put("name", name)
    }

  private def segmentMetadata(root: Path, segment: Long)(
      change: GenericRecord => Unit
  ): Unit = {
    val path = root.resolve(s"files/snapshots/10/manifests/1/$segment.avro")
    val schema = SnapshotSegmentSchema.schemaFor(4)
    val record = new GenericDatumReader[GenericRecord](schema).read(
      null,
      DecoderFactory.get().binaryDecoder(Files.readAllBytes(path), null)
    )
    change(record)
    val bytes = new ByteArrayOutputStream()
    val encoder = EncoderFactory.get().directBinaryEncoder(bytes, null)
    new GenericDatumWriter[GenericRecord](schema).write(record, encoder)
    encoder.flush()
    Files.write(path, bytes.toByteArray)
  }

  private def addL0(root: Path, partition: Long, channel: String): Path = {
    val l0Base = s"files/insert_log/10/$partition/41"
    val l0Avro = "files/snapshots/10/manifests/1/41.avro"
    val l0Manifest = root.resolve(s"$l0Base/_metadata/manifest-1.avro")
    Files.createDirectories(l0Manifest.getParent)
    val reader = new DataFileStream[GenericRecord](
      new ByteArrayInputStream(Files.readAllBytes(root.resolve(manifest))),
      new GenericDatumReader[GenericRecord]()
    )
    val record =
      try reader.next()
      finally reader.close()
    // Reuse the frozen delta, with no column groups in the L0 manifest.
    record.put("column_groups", Collections.emptyList[GenericRecord]())
    val writer = new DataFileWriter[GenericRecord](
      new GenericDatumWriter[GenericRecord](record.getSchema)
    )
    writer.create(record.getSchema, l0Manifest.toFile)
    try writer.append(record)
    finally writer.close()
    val l0Delete = root.resolve(s"$l0Base/_delta/delete.parquet")
    Files.createDirectories(l0Delete.getParent)
    Files.copy(root.resolve(delete), l0Delete)
    Files.write(
      root.resolve(l0Avro),
      SnapshotSegmentFixture.encode(
        segmentId = 41L,
        partitionId = partition,
        rows = 0L,
        segmentLevel = 1L,
        channelName = channel
      )
    )
    Seq(30L, 31L).foreach { segment =>
      segmentMetadata(root, segment)(
        _.put("channel_name", "source-channel")
      )
    }
    metadata(root) { json =>
      val items = json.withArray("storagev2_manifest_list")
      items.elements().asScala.foreach { item =>
        if (item.get("segmentID").asLong() == 30L) {
          // Version 1 has no deletes: only a matching L0 removes PK 3002.
          val value = item.asInstanceOf[ObjectNode]
          value.put(
            "manifest",
            value.get("manifest").asText().replace("\"ver\":2", "\"ver\":1")
          )
        }
      }
      items
        .addObject()
        .put("segmentID", 41L)
        .put("manifest", s"""{"ver":1,"base_path":"$l0Base"}""")
      json.withArray("manifest_list").add(l0Avro)
    }
    l0Manifest
  }

  private def damage(root: Path, key: String, missing: Boolean): Unit =
    if (missing) Files.delete(root.resolve(key))
    else
      Files.write(root.resolve(key), "not a valid storage file".getBytes(UTF_8))

  private def messages(error: Throwable): String =
    Iterator
      .iterate(error)(_.getCause)
      .takeWhile(_ != null)
      .map(e => String.valueOf(e.getMessage))
      .mkString("\n")

  private def failsWithFile(key: String)(body: => Any): Unit = {
    val failure = intercept[Exception](body)
    assert(messages(failure).contains(key), messages(failure))
  }

  private def failsReadPlanning(
      options: Map[String, String],
      clues: String*
  )(query: DataFrame => DataFrame): Unit = {
    val failure = intercept[Exception] {
      query(read(options)).queryExecution.toRdd
    }
    val detail = messages(failure)
    clues.foreach(clue => assert(detail.contains(clue), detail))
  }

  test(
    "Milvus 3.0.6 StructArray reads preserve order, nested nulls and raw vectors"
  ) {
    withCollection("struct-array-306") { (_, options) =>
      val frame = read(options)
      assert(
        frame.schema("chunks").dataType.simpleString ==
          "array<struct<text:string,score:int,embedding:array<float>,tags:array<bigint>>>"
      )
      val rows = frame
        .selectExpr("id", "chunks IS NULL", "size(chunks)", "chunks.text")
        .orderBy("id")
        .collect()
      assert(
        rows.map(_.getBoolean(1)).toList == List(false, false, true, false)
      )
      assert(rows(1).getInt(2) == 0)
      assert(rows(0).getSeq[String](3).toList == List("alpha", "beta"))

      val elements = frame
        .selectExpr("id", "posexplode(chunks) AS (pos, chunk)")
        .selectExpr(
          "id",
          "pos",
          "chunk.text",
          "chunk.score",
          "chunk.embedding",
          "chunk.tags"
        )
        .orderBy("id", "pos")
        .collect()
      assert(
        elements.map(_.getString(2)).toList == List("alpha", "beta", "delta")
      )
      assert(
        elements(0).getInt(3) == 7 && elements(1).isNullAt(3) && elements(2)
          .getInt(3) == 0
      )
      assert(elements(0).getSeq[Float](4).toList == List(1f, 2f))
      assert(elements(1).isNullAt(4))
      assert(elements(2).getSeq[Float](4).toList == List(-1.5f, 0.5f))
      assert(elements(0).getSeq[java.lang.Long](5).toList == List(3L, null))
      assert(elements(1).getSeq[Long](5).isEmpty && elements(2).isNullAt(5))

      val physical = read(
        options ++ Map(
          MilvusOption.ReadMode -> "physical",
          MilvusOption.ReadVectorRaw -> "true",
          MilvusOption.ReadBatchMaxRows -> "1"
        )
      ).selectExpr("id", "_milvus_row_id.row_offset", "chunks")
      val raw = physical
        .where("id = 1")
        .selectExpr(
          "row_offset",
          "hex(chunks[0].embedding)",
          "chunks[1].embedding"
        )
        .head()
      assert(
        raw.getLong(0) == 0L && raw.getString(1) == "0000803F00000040" && raw
          .isNullAt(2)
      )
    }
  }

  test(
    "StructArray members that differ in case follow the Spark session resolver"
  ) {
    withCollection("struct-array-306") { (root, options) =>
      metadata(root) { json =>
        val fields = json.at("/collection/schema/struct_array_fields/0/fields")
        val text = fields
          .elements()
          .asScala
          .find(_.path("name").asText() == "chunks[text]")
          .get
        text.asInstanceOf[ObjectNode].put("name", "chunks[Score]")
      }

      val key = "spark.sql.caseSensitive"
      val previous = spark.conf.get(key)
      try {
        spark.conf.set(key, "false")
        val collision = intercept[Exception](read(options).schema)
        assert(
          messages(collision).contains("score") && messages(collision).contains(
            "Score"
          )
        )
        val ordinary = spark.read
          .format("milvus")
          .options(options)
          .schema("id BIGINT")
          .load()
        assert(ids(ordinary) == Seq(1L, 2L, 3L, 4L))

        spark.conf.set(key, "true")
        val first = read(options)
          .where("id = 1")
          .selectExpr(
            "chunks[0].Score",
            "chunks[0].score",
            "chunks[1].Score",
            "chunks[1].score"
          )
          .head()
        assert(first.getString(0) == "alpha" && first.getInt(1) == 7)
        assert(first.getString(2) == "beta" && first.isNullAt(3))
      } finally spark.conf.set(key, previous)
    }
  }

  test("ordinary projections skip the StructArray column groups") {
    withCollection("struct-array-306") { (root, options) =>
      val files = Files.walk(root)
      try {
        val memberFiles = files
          .iterator()
          .asScala
          .filter { path =>
            val name = path.getFileName.toString
            name.endsWith(".parquet") && (name.startsWith("1_") || name
              .startsWith("2_"))
          }
          .toList
        assert(memberFiles.size == 2)
        memberFiles.foreach(Files.delete(_))
      } finally files.close()
      assert(ids(read(options)) == Seq(1L, 2L, 3L, 4L))
    }
  }

  test(
    "older segments inherit StructArray member counts and null parents from stored siblings"
  ) {
    withCollection("struct-array-306") { (root, options) =>
      metadata(root) { json =>
        val parents = json
          .at("/collection/schema")
          .asInstanceOf[ObjectNode]
          .withArray("struct_array_fields")
        parents
          .get(0)
          .asInstanceOf[ObjectNode]
          .withArray("fields")
          .addObject()
          .put("name", "chunks[new_score]")
          .put("fieldID", "106")
          .put("data_type", "Array")
          .put("element_type", "Int32")
          .put("nullable", true)
          .put("element_nullable", true)
        val added = parents
          .addObject()
          .put("name", "other")
          .put("fieldID", "107")
          .put("nullable", true)
        added
          .putArray("fields")
          .addObject()
          .put("name", "other[score]")
          .put("fieldID", "108")
          .put("data_type", "Array")
          .put("element_type", "Int32")
          .put("nullable", true)
      }
      val rows = read(options)
        .selectExpr("id", "chunks.new_score AS scores", "other")
        .orderBy("id")
        .collect()
      assert(rows.forall(_.isNullAt(2)))
      assert(rows(0).getSeq[java.lang.Integer](1).toList == List(null, null))
      assert(
        rows(1).getSeq[java.lang.Integer](1).isEmpty && rows(2).isNullAt(1)
      )
      assert(rows(3).getSeq[java.lang.Integer](1).toList == List(null))
      assert(read(options).select("other").collect().forall(_.isNullAt(0)))
    }
  }

  test(
    "fixed V3 reads need no management parser or service options"
  ) {
    fixture { (_, options) =>
      assert(
        !spark.sessionState.sqlParser.getClass.getName
          .contains("MilvusSqlParser")
      )
      assert(
        !options.keys.exists(key =>
          key.contains("uri") || key.contains("token")
        )
      )
      val frame = read(options)
      assert(ids(frame) == liveIds)
      assert(ids(frame.filter("category IS NULL")) == Seq(3001L))
    }
  }

  test("an explicit schema reads the stored Milvus RowID by name") {
    fixture { (_, options) =>
      // Both segments store RowIDs 0 through 7. Deletes remove 2 from
      // segment 30 and 5 from segment 31.
      val expected = ((0L until 8L).filterNot(_ == 2L) ++
        (0L until 8L).filterNot(_ == 5L)).sorted
      val rowIds = spark.read
        .format("milvus")
        .options(options)
        .schema("RowID BIGINT")
        .load()
        .select("RowID")
      assert(rowIds.collect().map(_.getLong(0)).toSeq.sorted == expected)
    }
  }

  test(
    "partition and segment predicates prune Spark tasks and preserve applicable deletes"
  ) {
    fixture { (root, options) =>
      // PK 3002 is deleted only by this collection-wide L0 segment.
      addL0(root, -1L, "source-channel")
      // The segment record is authoritative for the partition ID. Keep the
      // frozen data and manifest locations while assigning segment 31 to 21.
      segmentMetadata(root, 31L)(_.put("partition_id", 21L))
      metadata(root) { json =>
        json
          .get("snapshot_info")
          .asInstanceOf[ObjectNode]
          .withArray("partition_ids")
          .add(21L)
      }
      val first = liveIds.filter(_ < 3100L)
      val frame = read(options)
      assert(frame.columns.toSeq == Seq("id", "vector", "category"))
      val direct = frame.where("_partition_id IN (20, 22)")
      assert(direct.queryExecution.toRdd.getNumPartitions == 1)
      assert(ids(direct) == first)
      assert(
        frame.select("_timestamp").collect().map(_.getLong(0)).toSeq ==
          Seq.fill(liveIds.size)(100L)
      )
      val bySegment =
        frame.where("_segment_id = 30 AND _row_offset IN (1, 2, 3)")
      assert(bySegment.queryExecution.toRdd.getNumPartitions == 1)
      assert(ids(bySegment) == Seq(3001L, 3003L))

      // A temp view retains the metadata columns selected on the source.
      frame
        .select(
          "id",
          "_partition_id",
          "_segment_id",
          "_row_offset",
          "_timestamp"
        )
        .createOrReplaceTempView("partitioned_snapshot")
      try {
        val query = spark.sql("""
          SELECT id, _partition_id, _segment_id, _row_offset, _timestamp
          FROM partitioned_snapshot
          WHERE _partition_id IN (20, 22) AND _timestamp = 100
        """)
        assert(query.queryExecution.toRdd.getNumPartitions == 1)
        val rows = query.collect().map(_.toSeq).toSet
        assert(
          rows == first.map(id => Seq(id, 20L, 30L, id - 3000L, 100L)).toSet
        )
        val mixed = spark.sql("""
          SELECT id FROM partitioned_snapshot
          WHERE _partition_id = 20 OR id = 3100
        """)
        assert(ids(mixed) == (first :+ 3100L))
      } finally spark.catalog.dropTempView("partitioned_snapshot")

      val absent = frame.where("_partition_id IN (22, 23)")
      assert(absent.queryExecution.toRdd.getNumPartitions == 0)
      assert(ids(absent).isEmpty)
    }
  }

  test(
    "an external schema can omit a struct array named _timestamp and read the stored timestamp"
  ) {
    withCollection("struct-array-306") { (root, options) =>
      def times(frame: DataFrame): List[Long] =
        frame
          .select("id", "_timestamp")
          .orderBy("id")
          .collect()
          .map(_.getLong(1))
          .toList
      val expected = times(read(options))
      metadata(root) { json =>
        val parent = json
          .at("/collection/schema/struct_array_fields/0")
          .asInstanceOf[ObjectNode]
        parent.put("name", "_timestamp")
        parent.withArray("fields").elements().asScala.foreach { node =>
          val field = node.asInstanceOf[ObjectNode]
          field.put(
            "name",
            field.get("name").asText().replace("chunks[", "_timestamp[")
          )
        }
      }
      val visibleParent = read(options)
      assert(
        visibleParent
          .where("id = 1")
          .selectExpr("_timestamp[0].text")
          .head()
          .getString(0) == "alpha"
      )
      val onlyId =
        spark.read.format("milvus").options(options).schema("id BIGINT").load()
      assert(times(onlyId) == expected)
    }
  }

  test("a stored _timestamp field retains its values and predicates") {
    fixture { (root, options) =>
      renameCategory(root, "_timestamp")
      val frame = read(options)
      assert(frame.columns.toSeq == Seq("id", "vector", "_timestamp"))
      assert(ids(frame.where("_timestamp IS NULL")) == Seq(3001L))
      assert(
        frame.where("id = 3000").select("_timestamp").head().getLong(0) == 0L
      )
      val onlyId = spark.read
        .format("milvus")
        .options(options)
        .schema("id BIGINT")
        .load()
      assert(ids(onlyId.where("_timestamp = 100")) == liveIds)
      assert(
        onlyId
          .select("_timestamp")
          .distinct()
          .collect()
          .map(_.getLong(0))
          .toSeq == Seq(100L)
      )
    }
  }

  test("physical reads expose the hidden row identity for every stored row") {
    fixture { (root, options) =>
      val physicalOptions =
        options + (MilvusOption.ReadMode -> MilvusOption.ReadModePhysical)
      val liveFailure = intercept[AnalysisException] {
        read(options).select(RowIdentity.ColumnName).queryExecution.analyzed
      }
      assert(liveFailure.getMessage.contains(RowIdentity.ColumnName))
      val physical = read(physicalOptions)
      assert(
        physical.select("*").columns.toSeq == Seq("id", "vector", "category")
      )
      val rows = physical
        .select(
          "id",
          "_partition_id",
          "_segment_id",
          "_row_offset",
          "_timestamp",
          RowIdentity.ColumnName
        )
        .collect()
      assert(
        rows.map(_.getLong(0)).sorted.toSeq ==
          ((3000L until 3008L) ++ (3100L until 3108L))
      )
      rows.foreach { row =>
        val id = row.getLong(0)
        val segment = id / 100L
        val offset = id % 100L
        assert(row.toSeq.slice(1, 5) == Seq(20L, segment, offset, 100L))
        assert(row.getStruct(5).toSeq == Seq(1L, segment, offset))
      }
      val position = physical
        .where("id = 3105")
        .selectExpr(
          "_milvus_row_id.segment_id",
          "_milvus_row_id.row_offset"
        )
        .head()
      assert(position.toSeq == Seq(31L, 5L))
    }
  }

  test(
    "physical reads require a snapshot id only when the hidden row identity is requested"
  ) {
    fixture { (root, options) =>
      metadata(root)(
        _.get("snapshot_info").asInstanceOf[ObjectNode].remove("id")
      )
      val physicalOptions =
        options + (MilvusOption.ReadMode -> MilvusOption.ReadModePhysical)
      val withoutId = read(physicalOptions)
      assert(withoutId.where("_segment_id = 30").count() == 8L)
      val empty = read(
        (physicalOptions - MilvusOption.SnapshotPath) ++ Map(
          MilvusOption.SnapshotManifests -> "[]",
          MilvusOption.SnapshotSchemaJson -> Files.readString(
            root.resolve("snapshot.json")
          )
        )
      )
      assert(empty.collect().isEmpty)
      for (frame <- Seq(withoutId, empty)) {
        val failure = intercept[Exception] {
          frame.select(RowIdentity.ColumnName).collect()
        }
        val detail = messages(failure)
        assert(detail.contains("has no snapshot_info.id"), detail)
        assert(detail.contains(RowIdentity.ColumnName), detail)
        assert(detail.contains("snapshot file that Milvus created"), detail)
      }
    }
  }

  test(
    "a stored field named _milvus_row_id shadows the physical row identity"
  ) {
    fixture { (root, options) =>
      renameCategory(root, RowIdentity.ColumnName)
      val physical =
        options + (MilvusOption.ReadMode -> MilvusOption.ReadModePhysical)
      val frame = read(physical)
      assert(
        frame.columns.toSeq == Seq("id", "vector", RowIdentity.ColumnName)
      )
      assert(ids(frame.where("_milvus_row_id IS NULL")) == Seq(3001L))
      assert(
        frame
          .where("id = 3002")
          .select(RowIdentity.ColumnName)
          .head()
          .getLong(0) == 1L
      )
      val identity = spark.read
        .format("milvus")
        .options(physical)
        .schema("id BIGINT")
        .load()
        .where("id = 3002")
        .select(RowIdentity.ColumnName)
        .head()
        .getStruct(0)
      assert(identity.toSeq == Seq(1L, 30L, 2L))
      metadata(root)(
        _.get("snapshot_info").asInstanceOf[ObjectNode].remove("id")
      )
      val stored = read(physical)
        .where("id = 3002")
        .select(RowIdentity.ColumnName)
        .head()
      assert(stored.getLong(0) == 1L)
    }
  }

  test(
    "live reads apply scoped L0 and segment deletes while physical reads return every row"
  ) {
    for (
      (partition, channel, applies) <- Seq(
        (-1L, "source-channel", true),
        (20L, "source-channel", true),
        (21L, "source-channel", false),
        (-1L, "other-channel", false)
      )
    ) withClue(s"L0 partition=$partition channel=$channel: ") {
      fixture { (root, options) =>
        addL0(root, partition, channel)
        val expectedAll = if (applies) liveIds else (liveIds :+ 3002L).sorted
        val physical = read(
          options + (MilvusOption.ReadMode -> MilvusOption.ReadModePhysical)
        )
        assert(ids(physical) == ((3000L until 3008L) ++ (3100L until 3108L)))
        val live = read(options)
        assert(ids(live) == expectedAll)
        // Partition selection must retain every data segment in partition 20
        // and any applicable collection-wide L0 deletes.
        val narrowed = live.where("_partition_id = 20")
        assert(ids(narrowed) == expectedAll)
      }
    }
  }

  test(
    "nonempty explicit metadata requires a snapshot path to retain L0 deletes"
  ) {
    fixture { (root, options) =>
      addL0(root, -1L, "source-channel")
      val json =
        mapper.readTree(Files.readAllBytes(root.resolve("snapshot.json")))
      val explicit = (options - MilvusOption.SnapshotPath) ++ Map(
        MilvusOption.SnapshotSchemaJson -> json.toString,
        MilvusOption.SnapshotCollectionId -> "10",
        MilvusOption.SnapshotPartitionIds -> "20",
        MilvusOption.SnapshotManifests -> json
          .get("storagev2_manifest_list")
          .toString
      )
      failsReadPlanning(explicit, MilvusOption.SnapshotPath)(identity)
    }
  }

  test("a zero-record L0 manifest fails read planning") {
    fixture { (root, options) =>
      val l0Manifest = addL0(root, -1L, "source-channel")
      val reader = new DataFileStream[GenericRecord](
        new ByteArrayInputStream(Files.readAllBytes(l0Manifest)),
        new GenericDatumReader[GenericRecord]()
      )
      val schema =
        try reader.getSchema
        finally reader.close()
      val writer = new DataFileWriter[GenericRecord](
        new GenericDatumWriter[GenericRecord](schema)
      )
      // Keep the Avro container header and schema, but omit its manifest record.
      writer.create(schema, l0Manifest.toFile)
      writer.close()
      failsReadPlanning(
        options,
        "files/insert_log/10/-1/41",
        "no record"
      )(_.where("_partition_id = 20"))
    }
  }

  test(
    "an L0 manifest without its snapshot segment record fails before narrowing"
  ) {
    fixture { (root, options) =>
      addL0(root, -1L, "source-channel")
      metadata(root) { json =>
        val retained = json
          .get("manifest_list")
          .elements()
          .asScala
          .map(_.asText())
          .filterNot(_.endsWith("/41.avro"))
          .toVector
        val items = json.putArray("manifest_list")
        retained.foreach(items.add)
      }
      // Segment 30 has no local delete for PK 3002. Losing the L0 role must
      // fail before selection could discard that delete source as data.
      failsReadPlanning(
        options,
        "41",
        "snapshot segment Avro"
      )(_.where("_partition_id = 20"))
    }
  }

  test("invalid L0 manifest references never fall back to the latest version") {
    val base = "files/insert_log/10/-1/41"
    val malformed = s"""{"ver":1,"base_path":"$base"}""".dropRight(1)
    for (reference <- Seq(base, malformed)) {
      fixture { (root, options) =>
        addL0(root, -1L, "source-channel")
        metadata(root) { json =>
          json
            .get("storagev2_manifest_list")
            .elements()
            .asScala
            .find(_.get("segmentID").asLong() == 41L)
            .get
            .asInstanceOf[ObjectNode]
            .put("manifest", reference)
        }
        failsReadPlanning(options, "segment 41", "manifest reference")(identity)
      }
    }
  }

  test(
    "fixed V3 tables remain readable through views and reject append writes"
  ) {
    fixture { (root, options) =>
      val frame = read(options)
      frame.createOrReplaceTempView("public_fixed_snapshot")
      try {
        val all = spark.sql("SELECT * FROM public_fixed_snapshot")
        assert(all.columns.toSeq == Seq("id", "vector", "category"))
        assert(ids(all) == liveIds)
      } finally spark.catalog.dropTempView("public_fixed_snapshot")
      def files(): Set[String] = {
        val paths = Files.walk(root)
        try paths.iterator().asScala.map(root.relativize(_).toString).toSet
        finally paths.close()
      }
      val before = files()
      val failure = intercept[Exception] {
        frame.write.format("milvus").options(options).mode("append").save()
      }
      val error = messages(failure).toLowerCase(java.util.Locale.ROOT)
      assert(
        error.contains("write") || error.contains("append") ||
          error.contains("does not allow create table as select"),
        error
      )
      assert(files() == before)
      assert(ids(frame) == liveIds)
    }
  }

  test("an empty V3 snapshot retains its schema and yields no rows") {
    fixture { (root, options) =>
      metadata(root) { json =>
        json.putArray("manifest_list")
        json.putArray("storagev2_manifest_list")
        json.putArray("indexes")
        json.putArray("build_ids")
      }
      val frame = read(options)
      assert(frame.schema.fieldNames.toSeq == Seq("id", "vector", "category"))
      assert(ids(frame).isEmpty)
    }
  }

  test("missing or corrupt snapshot and segment metadata fails visibly") {
    for (
      key <- Seq("snapshot.json", avro, manifest); missing <- Seq(true, false)
    ) {
      fixture { (root, options) =>
        damage(root, key, missing)
        withClue(s"$key missing=$missing: ") {
          val context =
            if (key == manifest) "files/insert_log/10/20/30" else key
          failsWithFile(context) { ids(read(options)) }
        }
      }
    }
  }

  test("missing or corrupt V3 data cannot become a partial successful read") {
    for (missing <- Seq(true, false)) fixture { (root, options) =>
      val paths = Files.list(root.resolve("files/insert_log/10/20/30/_data"))
      val data =
        try paths.iterator().asScala.toSeq.sortBy(_.toString).head
        finally paths.close()
      val key = root.relativize(data).toString
      damage(root, key, missing)
      // A projection may correctly avoid this column group's file. Read all
      // columns so the damaged group is required by this action.
      val context =
        if (missing) data.getFileName.toString
        else "Parquet magic bytes not found in footer"
      failsWithFile(context) { read(options).collect() }
    }
  }

  test(
    "missing or corrupt required deletes fail instead of resurrecting rows"
  ) {
    for (missing <- Seq(true, false)) fixture { (root, options) =>
      damage(root, delete, missing)
      failsWithFile("delete.parquet") { ids(read(options)) }
    }
  }

  test("one relation retains its fixed snapshot across repeated actions") {
    fixture { (root, options) =>
      val frame = read(options)
      assert(ids(frame) == liveIds)
      metadata(root) { json =>
        val items = json.get("storagev2_manifest_list")
        items.elements().asScala.foreach { item =>
          val value = item.asInstanceOf[ObjectNode]
          value.put(
            "manifest",
            value.get("manifest").asText().replace("\"ver\":2", "\"ver\":1")
          )
        }
      }
      assert(ids(frame) == liveIds)
      assert(
        ids(frame.filter("category IS NOT NULL")) == liveIds.filterNot(
          _ == 3001L
        )
      )
      assert(ids(read(options)) == ((3000L until 3008L) ++ (3100L until 3108L)))
    }
  }

  test("a real task retry rereads the fixed V3 relation") {
    fixture { (_, options) =>
      val frame = read(options).select("id")
      val retried = frame.rdd
        .mapPartitions { rows =>
          val values = rows.map(_.getLong(0)).toVector
          val task = TaskContext.get()
          if (task.partitionId() == 0 && task.attemptNumber() == 0)
            throw new IllegalStateException(
              "fail once after reading native V3 rows"
            )
          values.iterator.map(id => id -> task.attemptNumber())
        }
        .collect()
        .toSeq
      assert(retried.map(_._1).sorted == liveIds)
      assert(retried.exists(_._2 > 0), "the task must actually retry")
      assert(ids(frame) == liveIds)
    }
  }

  test("a declared V3 segment rejects old and unknown Avro storage versions") {
    for (version <- Seq(2L, 0L, 1L, 4L, -1L)) fixture { (root, options) =>
      Files.write(
        root.resolve(avro),
        SnapshotSegmentFixture.encode(rows = 8L, storageVersion = version)
      )
      val failure = intercept[IllegalArgumentException] { read(options) }
      assert(messages(failure).contains("30"))
      assert(messages(failure).contains("require Storage V3"))
    }
  }

  test("an unknown-only snapshot cannot silently resolve to an empty table") {
    fixture { (root, options) =>
      Files.write(
        root.resolve(avro),
        SnapshotSegmentFixture.encode(rows = 8L, storageVersion = 9L)
      )
      metadata(root) { json =>
        json.putArray("storagev2_manifest_list")
        json.putArray("manifest_list").add(avro)
      }
      val failure = intercept[IllegalArgumentException] { read(options) }
      assert(messages(failure).contains("30=9"))
    }
  }
}
