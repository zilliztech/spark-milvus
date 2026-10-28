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
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.functions.call_function
import org.apache.spark.TaskContext
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

import com.zilliz.milvus.jni.storage.NativeStorageLibrary
import com.zilliz.milvus.jni.vector.NativeVectorLibrary
import com.zilliz.milvus.storage.manifest.{
  SnapshotSegmentFixture,
  SnapshotSegmentSchema
}
import com.zilliz.spark.connector.extensions.{
  MilvusNearestByJoinExec,
  MilvusReadSparkSessionExtensions,
  MilvusSparkPlugin
}
import com.zilliz.spark.connector.implicits._
import com.zilliz.spark.connector.metrics.{NearestByMetrics, SearchMetrics}
import com.zilliz.spark.connector.testkit.LocalMilvusCollection

/** Public file reads and searches against frozen V3 data. Every destructive
  * case changes only a temporary copy; no writer or online service prepares it.
  */
class V3ReadContractTest
    extends AnyFunSuite
    with BeforeAndAfterAll
    with AdaptiveSparkPlanHelper {
  private val mapper = new ObjectMapper()
  private var spark: SparkSession = _
  private var available = true
  private val avro = "files/snapshots/10/manifests/1/30.avro"
  private val manifest = "files/insert_log/10/20/30/_metadata/manifest-2.avro"
  private val delete = "files/insert_log/10/20/30/_delta/delete.parquet"
  private val index = "files/index_files/1030/1/20/30/HNSW_0"
  private val liveIds = ((3000L until 3008L) ++ (3100L until 3108L))
    .filterNot(Set(3002L, 3105L))
    .sorted

  override def beforeAll(): Unit = {
    val line = org.apache.spark.SPARK_VERSION.split('.').take(2).mkString(".")
    available = line != "3.5" || Runtime.version().feature() < 21
    if (available) {
      try {
        NativeStorageLibrary.load()
        NativeVectorLibrary.load()
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
        .config(
          "spark.sql.extensions",
          classOf[MilvusReadSparkSessionExtensions].getName
        )
        .config("spark.plugins", classOf[MilvusSparkPlugin].getName)
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
    assume(
      available,
      "the native libraries and a compatible Arrow JVM are required"
    )
    val root = Files.createTempDirectory("v3-read-contract-")
    try body(root, LocalMilvusCollection.load("nearest", root))
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

  private def search(base: DataFrame, mode: String): DataFrame = {
    val session = spark
    import session.implicits._
    val queries = Seq(("q", Seq(0.1f, 0f))).toDF("qid", "qv")
    queries.nearestByJoin(
      base,
      call_function("vector_l2_distance", queries("qv"), base("vector")),
      3,
      mode,
      "distance"
    )
  }

  private def searchIds(
      base: DataFrame,
      mode: String,
      indexedSegments: Long,
      segments: Long = 2L
  ): Seq[Long] = {
    val result = search(base, mode).select("id")
    val found = result.collect().map(_.getLong(0)).toSeq.sorted
    val execution = collectFirst(result.queryExecution.executedPlan) {
      case node: MilvusNearestByJoinExec => node
    }.getOrElse(
      fail("the fixed V3 search must use the connector's physical operator")
    )
    assert(
      execution.metrics(NearestByMetrics.IndexSegments).value == indexedSegments
    )
    assert(
      execution
        .metrics(NearestByMetrics.ExactSegments)
        .value == segments - indexedSegments
    )
    assert(execution.metrics(SearchMetrics.KnowhereCalls).value > 0L)
    if (indexedSegments > 0L)
      assert(execution.metrics(SearchMetrics.IndexBytes).value > 0L)
    found
  }

  test(
    "fixed V3 reads and searches need no management parser or service options"
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
      assert(searchIds(frame, "exact", 0L) == Seq(3000L, 3001L, 3100L))
      assert(searchIds(frame, "approx", 2L) == Seq(3000L, 3001L, 3100L))
    }
  }

  test(
    "L0 deletes respect partition and channel scope across native reads and narrowed searches"
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
        assert(
          ids(read(options + ("milvus.read.apply.deletes" -> "false"))) ==
            ((3000L until 3008L) ++ (3100L until 3108L))
        )
        val expectedAll = if (applies) liveIds else (liveIds :+ 3002L).sorted
        val expectedSegment = (3000L until 3008L)
          .filterNot(id => applies && id == 3002L)
        val expectedHits = if (applies) Seq(3000L) else Seq(3000L, 3002L)
        Seq(true, false).foreach { columnar =>
          val readOptions =
            options + ("milvus.read.columnar" -> columnar.toString)
          assert(ids(read(readOptions)) == expectedAll)
          // Both restrictions exclude the L0 source itself. Its applicable
          // deletes must already have been attached to the retained data task.
          val narrowed = read(
            readOptions ++ Map(
              "milvus.partitions" -> "20",
              "milvus.segments" -> "30"
            )
          )
          assert(ids(narrowed) == expectedSegment)
          Seq("exact", "approx").foreach { mode =>
            assert(
              searchIds(
                narrowed.filter("id IN (3000, 3002)"),
                mode,
                indexedSegments = if (mode == "approx") 1L else 0L,
                segments = 1L
              ) == expectedHits
            )
          }
        }
      }
    }
  }

  test(
    "fixed V3 tables remain readable through views and reject append writes"
  ) {
    fixture { (root, options) =>
      val frame = read(options)
      frame.createOrReplaceTempView("public_fixed_snapshot")
      try assert(ids(spark.table("public_fixed_snapshot")) == liveIds)
      finally spark.catalog.dropTempView("public_fixed_snapshot")
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

  test("an empty V3 snapshot retains its schema and yields no rows or hits") {
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
      assert(ids(search(frame, "exact")).isEmpty)
      assert(ids(search(frame, "approx")).isEmpty)
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

  test(
    "missing or corrupt declared indexes fail instead of silently scanning"
  ) {
    for (missing <- Seq(true, false)) fixture { (root, options) =>
      if (missing) damage(root, index, missing = true)
      else {
        // Preserve the object length so this reaches envelope decoding rather
        // than failing the earlier SLICE_META size check.
        val path = root.resolve(index)
        Files.write(path, new Array[Byte](Math.toIntExact(Files.size(path))))
      }
      assert(searchIds(read(options), "exact", 0L) == Seq(3000L, 3001L, 3100L))
      failsWithFile(if (missing) "HNSW_0" else "index object") {
        ids(search(read(options), "approx"))
      }
    }
  }

  test("explicitly unindexed V3 segments use the supported exact fallback") {
    fixture { (root, options) =>
      for (segment <- Seq(30L, 31L)) {
        Files.write(
          root.resolve(s"files/snapshots/10/manifests/1/$segment.avro"),
          SnapshotSegmentFixture.encode(segmentId = segment, rows = 8L)
        )
      }
      Files.delete(root.resolve(index))
      assert(searchIds(read(options), "approx", 0L) == Seq(3000L, 3001L, 3100L))
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
