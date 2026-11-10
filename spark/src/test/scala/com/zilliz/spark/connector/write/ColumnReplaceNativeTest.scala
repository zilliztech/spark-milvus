package com.zilliz.spark.connector.write

import java.nio.file.{Files, Path}
import java.util.Comparator
import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll

import com.zilliz.milvus.jni.storage.NativeStorageLibrary
import com.zilliz.spark.connector.options.MilvusOption
import com.zilliz.spark.connector.testkit.LocalMilvusCollection
import com.zilliz.spark.connector.types.RowIdentity

/** Column replacement over a frozen V3 collection with rows marked deleted:
  * read the stored rows with their identities, compute new values with plain
  * Spark, write them back and read the new revisions.
  */
class ColumnReplaceNativeTest extends AnyFunSuite with BeforeAndAfterAll {
  private val mapper = new ObjectMapper()
  private var spark: SparkSession = _
  private var available = true

  // The "basic" collection: segments 30 and 31 store 8 rows each; ids 3002
  // and 3105 are marked deleted. The snapshot declares no indexes.
  private val storedIds =
    ((3000L until 3008L) ++ (3100L until 3108L)).sorted
  private val deletedIds = Set(3002L, 3105L)
  private val updates = Map[Long, java.lang.Long](
    3001L -> 11L,
    3002L -> 22L,
    3104L -> null,
    3107L -> 77L
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
        .appName("column-replace-native")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "3")
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
    val root = Files.createTempDirectory("column-replace-")
    try body(root, LocalMilvusCollection.load("basic", root))
    finally {
      val paths = Files.walk(root)
      try paths.sorted(Comparator.reverseOrder[Path]()).forEach(Files.delete(_))
      finally paths.close()
    }
  }

  private def physical(options: Map[String, String]): DataFrame =
    spark.read
      .format("milvus")
      .options(options)
      .option(MilvusOption.ReadMode, MilvusOption.ReadModePhysical)
      .option(MilvusOption.ReadIncludeRowId, "true")
      .load()

  /** The update of the design example: matched rows take the new value, NULL
    * included; unmatched rows keep theirs.
    */
  private def prepared(source: DataFrame): DataFrame = {
    val changes = spark
      .createDataFrame(
        spark.sparkContext.parallelize(
          updates.toSeq.map { case (id, value) => Row(id, value) }
        ),
        StructType(
          Seq(
            StructField("id", LongType, nullable = false),
            StructField("new_category", LongType, nullable = true)
          )
        )
      )
      .withColumn("_matched", lit(true))
    source
      .select(RowIdentity.ColumnName, "id", "category")
      .join(changes, Seq("id"), "left")
      .select(
        col(RowIdentity.ColumnName),
        when(col("_matched").isNotNull, col("new_category"))
          .otherwise(col("category"))
          .alias("category")
      )
  }

  private def write(
      frame: DataFrame,
      options: Map[String, String],
      result: Path,
      mode: String = "append"
  ): Unit =
    frame.write
      .format("milvus")
      .options(options)
      .option(MilvusOption.WriteOperation, "replace_columns")
      .mode(mode)
      .save(result.toString)

  private def receipt(result: Path): DataFrame =
    spark.read
      .format("milvus")
      .option(MilvusOption.ReadMode, MilvusOption.ReadModeWriteResult)
      .load(result.toString)

  /** A copy of the snapshot that pins the given revisions, which is what a
    * snapshot taken after registration would describe.
    */
  private def pinned(root: Path, versions: Map[Long, Long]): String = {
    val json = mapper
      .readTree(Files.readAllBytes(root.resolve("snapshot.json")))
      .asInstanceOf[ObjectNode]
    val items = json.get("storagev2_manifest_list").asInstanceOf[ArrayNode]
    items.elements().asScala.foreach { item =>
      val node = item.asInstanceOf[ObjectNode]
      val segment = node.get("segmentID").asLong()
      val manifest =
        mapper.readTree(node.get("manifest").asText()).asInstanceOf[ObjectNode]
      versions.get(segment).foreach(manifest.put("ver", _))
      node.put("manifest", mapper.writeValueAsString(manifest))
    }
    Files.write(
      root.resolve("snapshot-after.json"),
      mapper.writeValueAsBytes(json)
    )
    "snapshot-after.json"
  }

  private def categories(frame: DataFrame): Map[Long, Option[Long]] =
    frame
      .select("id", "category")
      .collect()
      .map(row =>
        row.getLong(0) -> (if (row.isNullAt(1)) None else Some(row.getLong(1)))
      )
      .toMap

  test("a physical read returns every stored row with its identity") {
    fixture { (_, options) =>
      val rows = physical(options)
        .select(RowIdentity.ColumnName, "id")
        .collect()
      assert(rows.map(_.getLong(1)).sorted.toSeq == storedIds)
      val identities = rows.map(_.getStruct(0))
      assert(identities.map(_.getLong(0)).toSet == Set(1L))
      identities.groupBy(_.getLong(1)).foreach { case (segment, ids) =>
        assert(
          ids.map(_.getLong(2)).sorted.toSeq == (0L until 8L),
          s"segment $segment"
        )
      }
    }
  }

  test(
    "replaced columns read back from the new revisions; the snapshot keeps its values"
  ) {
    fixture { (root, options) =>
      val before =
        categories(spark.read.format("milvus").options(options).load())
      val result = root.resolve("write-result")
      // Scrambled input: the write restores stored order itself.
      write(prepared(physical(options)).repartition(5), options, result)

      val committed = receipt(result).collect()
      assert(
        committed.map(_.getAs[Long]("segment_id")).sorted.toSeq == Seq(30L, 31L)
      )
      committed.foreach { row =>
        assert(row.getAs[Long]("collection_id") == 10L)
        assert(row.getAs[Long]("snapshot_id") == 1L)
        assert(row.getAs[Long]("source_version") == 2L)
        assert(row.getAs[Long]("version") == 3L)
        assert(row.getAs[Long]("row_count") == 8L)
        assert(
          row
            .getAs[scala.collection.Seq[String]]("changed_fields")
            .toList == List("category")
        )
        assert(
          row
            .getAs[scala.collection.Seq[Long]]("changed_field_ids")
            .toList == List(102L)
        )
      }

      val after = categories(
        spark.read
          .format("milvus")
          .options(
            options + (MilvusOption.SnapshotPath -> pinned(
              root,
              Map(30L -> 3L, 31L -> 3L)
            ))
          )
          .load()
      )
      val expected = before.map { case (id, value) =>
        id -> updates
          .get(id)
          .map(v => Option(v).map(_.longValue))
          .getOrElse(value)
      }
      assert(after == expected)
      assert(after.keySet == storedIds.toSet -- deletedIds)
      assert(
        categories(
          spark.read.format("milvus").options(options).load()
        ) == before
      )
    }
  }

  test("an address that one write used cannot be used by another") {
    fixture { (root, options) =>
      val result = root.resolve("write-result")
      write(prepared(physical(options)), options, result)
      val error = intercept[Exception](
        write(prepared(physical(options)), options, result)
      )
      assert(
        rootMessage(error).contains("already belongs to another column write")
      )
    }
  }

  test("only append is a write mode") {
    fixture { (root, options) =>
      val frame = prepared(physical(options))
      Seq("errorifexists", "ignore", "overwrite").foreach { mode =>
        intercept[Exception](
          write(frame, options, root.resolve(s"r-$mode"), mode)
        )
        assert(!Files.exists(root.resolve(s"r-$mode/_result.json")), mode)
      }
    }
  }

  test("every stored row must arrive exactly once") {
    fixture { (root, options) =>
      val source = prepared(physical(options))
      val missing = root.resolve("missing")
      val error = intercept[Exception](
        write(source.limit(15), options, missing)
      )
      assert(rootMessage(error).contains("must be written exactly once"))
      assert(!Files.exists(missing.resolve("_result.json")))
      assert(Files.exists(missing.resolve("_failure.json")))

      val repeated = root.resolve("repeated")
      val twice = intercept[Exception](
        write(source.union(source.limit(1)), options, repeated)
      )
      assert(rootMessage(twice).contains("more than once"))
      // Nothing committed: the source revisions are still the latest.
      assert(latestVersion(root, 30L) == 2L && latestVersion(root, 31L) == 2L)
    }
  }

  test("rows of another snapshot are refused") {
    fixture { (root, options) =>
      val json = mapper
        .readTree(Files.readAllBytes(root.resolve("snapshot.json")))
        .asInstanceOf[ObjectNode]
      json.get("snapshot_info").asInstanceOf[ObjectNode].put("id", 2)
      Files.write(root.resolve("other.json"), mapper.writeValueAsBytes(json))
      val error = intercept[Exception](
        write(
          prepared(physical(options)),
          options + (MilvusOption.SnapshotPath -> "other.json"),
          root.resolve("write-result")
        )
      )
      assert(rootMessage(error).contains("was read from snapshot 1"))
    }
  }

  test("an indexed field is refused before anything is written") {
    fixture { (root, options) =>
      val snapshot = root.resolve("snapshot.json")
      val json =
        mapper.readTree(Files.readAllBytes(snapshot)).asInstanceOf[ObjectNode]
      val index = json.putArray("indexes").addObject()
      index.put("collectionID", 10).put("fieldID", 102).put("indexID", 900)
      index.put("index_name", "category_sort")
      index
        .putArray("index_params")
        .addObject()
        .put("key", "index_type")
        .put("value", "STL_SORT")
      Files.write(snapshot, mapper.writeValueAsBytes(json))
      val frame = physical(options).select(RowIdentity.ColumnName, "category")
      val error = intercept[IllegalArgumentException](
        write(frame, options, root.resolve("write-result"))
      )
      assert(error.getMessage.contains("'category' (index 'category_sort')"))
      assert(!Files.exists(root.resolve("write-result")))
    }
  }

  test(
    "a revision committed after the snapshot makes that segment fail and keeps the record"
  ) {
    fixture { (root, options) =>
      val frame = prepared(physical(options))
      // Another writer moves segment 31 past the pinned revision first.
      write(frame, options, root.resolve("first"))
      val result = root.resolve("second")
      val error = intercept[Exception](write(frame, options, result))
      assert(rootMessage(error).contains("did not commit"))
      val failure =
        mapper.readTree(Files.readAllBytes(result.resolve("_failure.json")))
      assert(failure.get("stage").asText() == "commit")
      val states = failure
        .get("segments")
        .elements()
        .asScala
        .map(s => s.get("segment_id").asLong() -> s.get("state").asText())
        .toMap
      assert(states.values.toSet == Set("not_committed"))
      assert(latestVersion(root, 30L) == 3L && latestVersion(root, 31L) == 3L)
    }
  }

  test("row and columnar reads give the same identities, filtered or not") {
    fixture { (_, options) =>
      def identities(frame: DataFrame): Map[Long, (Long, Long, Long)] =
        frame
          .select("id", RowIdentity.ColumnName)
          .collect()
          .map { row =>
            val id = row.getStruct(1)
            row.getLong(0) -> ((id.getLong(0), id.getLong(1), id.getLong(2)))
          }
          .toMap
      val columnar = identities(physical(options))
      assert(columnar.size == storedIds.size)
      assert(
        identities(
          physical(options + (MilvusOption.ReadColumnar -> "false"))
        ) == columnar
      )
      // A pushed predicate excludes rows inside a batch; the survivors keep
      // their stored positions.
      val filtered = identities(physical(options).filter(col("id") % 2 === 1))
      assert(filtered.nonEmpty && filtered.keySet.forall(_ % 2 == 1))
      assert(filtered == columnar.filter { case (id, _) =>
        filtered.contains(id)
      })
    }
  }

  test(
    "one writer can take several segments, with or without adaptive execution"
  ) {
    fixture { (root, options) =>
      Seq("true", "false").foreach { adaptive =>
        val keys =
          Seq("spark.sql.shuffle.partitions", "spark.sql.adaptive.enabled")
        val previous = keys.map(key => key -> spark.conf.getOption(key))
        spark.conf.set("spark.sql.shuffle.partitions", "1")
        spark.conf.set("spark.sql.adaptive.enabled", adaptive)
        try {
          val source =
            if (adaptive == "true") "snapshot.json" else "snapshot-after.json"
          val result = root.resolve(s"single-$adaptive")
          val at = options + (MilvusOption.SnapshotPath -> source)
          write(prepared(physical(at)), at, result)
          val versions = receipt(result)
            .collect()
            .map(row =>
              row.getAs[Long]("segment_id") -> row.getAs[Long]("version")
            )
            .toMap
          assert(versions.keySet == Set(30L, 31L))
          pinned(root, versions)
        } finally
          previous.foreach {
            case (key, Some(value)) => spark.conf.set(key, value)
            case (key, None)        => spark.conf.unset(key)
          }
      }
      assert(latestVersion(root, 30L) == 4L && latestVersion(root, 31L) == 4L)
    }
  }

  test("a field with manifest statistics is refused") {
    fixture { (root, options) =>
      val storage = com.zilliz.milvus.storage.credential.StorageProperties.from(
        Map("fs.storage_type" -> "local", "fs.root_path" -> root.toString)
      )
      val properties = new io.milvus.storage.MilvusStorageProperties()
      properties.create(storage)
      val transaction = new io.milvus.storage.MilvusStorageTransaction()
      try {
        transaction.begin(
          "files/insert_log/10/20/31",
          properties.getPtr,
          2L,
          0,
          1
        )
        transaction.updateStat(
          "text_index.102",
          Array("files/insert_log/10/20/31/_stats/text_index.102/0"),
          Map("version" -> "1")
        )
        assert(transaction.commit() == 3L)
      } finally {
        transaction.destroy()
        properties.free()
      }
      val at =
        options + (MilvusOption.SnapshotPath -> pinned(root, Map(31L -> 3L)))
      val result = root.resolve("write-result")
      val error =
        intercept[Exception](write(prepared(physical(at)), at, result))
      assert(rootMessage(error).contains("keeps statistics text_index.102"))
      assert(latestVersion(root, 30L) == 2L && latestVersion(root, 31L) == 3L)
      assert(Files.exists(result.resolve("_failure.json")))
    }
  }

  test(
    "two attempts of one partition write their own files; an aborted one removes them"
  ) {
    fixture { (root, options) =>
      val rows = prepared(physical(options))
        .orderBy(
          col(s"${RowIdentity.ColumnName}.${RowIdentity.SegmentIdField}"),
          col(s"${RowIdentity.ColumnName}.${RowIdentity.RowOffsetField}")
        )
      val schema = rows.schema
      val internal = rows.queryExecution.toRdd.map(_.copy()).collect()
      val table = ColumnReplaceTables.load(
        new org.apache.spark.sql.util.CaseInsensitiveStringMap(
          (options ++ Map(
            MilvusOption.WriteOperation -> "replace_columns",
            "path" -> root.resolve("attempts").toString
          )).asJava
        ),
        Some(schema)
      )
      val task = ColumnReplaceTask.of(table.plan, schema)
      val segments = table.plan.segments.map(s => s.id -> s).toMap
      def dataFiles(): Set[String] =
        Seq(30L, 31L).flatMap { segment =>
          Files
            .list(root.resolve(s"files/insert_log/10/20/$segment/_data"))
            .iterator()
            .asScala
            .map(_.getFileName.toString)
        }.toSet
      val before = dataFiles()
      val adopted = new ColumnReplaceDataWriter(task, segments)
      val speculative = new ColumnReplaceDataWriter(task, segments)
      internal.foreach { row =>
        adopted.write(row)
        speculative.write(row)
      }
      val message = adopted.commit().asInstanceOf[ColumnReplaceCommitMessage]
      speculative.abort()
      speculative.close()
      adopted.close()
      val written = message.segments
        .flatMap(_.columnGroups.flatMap(_.files))
        .map(path => path.substring(path.lastIndexOf('/') + 1))
        .toSet
      assert(message.segments.map(_.segmentId).sorted == Seq(30L, 31L))
      assert(written.nonEmpty && (written & before).isEmpty)
      assert(dataFiles() == before ++ written)
    }
  }

  private def latestVersion(root: Path, segment: Long): Long =
    Files
      .list(root.resolve(s"files/insert_log/10/20/$segment/_metadata"))
      .iterator()
      .asScala
      .map(_.getFileName.toString)
      .collect {
        case name if name.startsWith("manifest-") =>
          name.stripPrefix("manifest-").stripSuffix(".avro").toLong
      }
      .max

  private def rootMessage(error: Throwable): String =
    Iterator
      .iterate(error)(_.getCause)
      .takeWhile(_ != null)
      .map(e => Option(e.getMessage).getOrElse(""))
      .mkString(" | ")
}
