package com.zilliz.spark.connector.read

import java.nio.file.{Files, Path}
import java.util.Comparator
import scala.jdk.CollectionConverters._

import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, lit, when}

import com.zilliz.milvus.storage.io.ReadFixtures
import com.zilliz.spark.connector.options.MilvusOption
import com.zilliz.spark.connector.types.RowIdentity

/** Consumes frozen public files through the packaged connector and real native
  * libraries, then replaces stored columns through its public DataSource. The
  * caller pins the returned revisions for readback; the source snapshot stays
  * unchanged. Preparation only verifies and copies checksummed resources.
  */
object ArtifactConsumerSmoke {
  private final case class Entity(
      id: Long,
      vector: Seq[Float],
      category: Option[Long]
  )

  private val entities = (0 until 8).map { row =>
    Entity(
      3000L + row,
      Seq(row.toFloat, 0f),
      if (row == 1) None else Some(if (row == 0) 0L else 1L)
    )
  } ++ (0 until 8).map { row =>
    Entity(3100L + row, Seq(row + 0.25f, 1f), Some((row % 2).toLong))
  }
  private val live = entities.filterNot(row => Set(3002L, 3105L)(row.id))
  private val mapper = new ObjectMapper()

  private def passed(name: String): Unit =
    println(s"MILVUS_CHECK\t$name\tpassed")

  private def storage(directory: Path): Map[String, String] =
    Map("fs.storage_type" -> "local", "fs.root_path" -> directory.toString)

  private def read(
      spark: SparkSession,
      options: Map[String, String]
  ): DataFrame =
    spark.read.format("milvus").options(options).load()

  private def pathEntries(spark: SparkSession, directory: Path): Unit = {
    val json =
      mapper.readTree(Files.readAllBytes(directory.resolve("snapshot.json")))
    val options = storage(directory) +
      (MilvusOption.SnapshotPath -> "snapshot.json")
    val frame = read(spark, options).select("id", "category").orderBy("id")
    val wanted = live.sortBy(_.id).map(row => row.id -> row.category)
    def rows = frame
      .collect()
      .toSeq
      .map(row =>
        row.getLong(0) -> Option(row.get(1)).map(_.asInstanceOf[Long])
      )
    assert(rows == wanted, "advanced V3 fields or deletes differ")
    assert(
      read(spark, options + (MilvusOption.ReadApplyDeletes -> "false"))
        .count() == 16L
    )
    passed("v3-advanced")
    assert(
      frame.count() == 14L && rows == wanted,
      "repeated actions changed the fixed relation"
    )
    passed("repeated-actions")
    val empty = read(
      spark,
      storage(directory) ++ Map(
        MilvusOption.SnapshotSchemaJson -> json.toString,
        MilvusOption.SnapshotManifests -> "[]"
      )
    )
    assert(
      empty.schema == read(spark, options).schema,
      "empty V3 input lost its schema"
    )
    assert(empty.count() == 0L && empty.collect().isEmpty)
    passed("v3-empty")
  }

  private def snapshotReads(spark: SparkSession, directory: Path): Unit = {
    ReadFixtures.copyTo("collections/basic", directory)
    val options = storage(directory) ++ Map(
      MilvusOption.SnapshotPath -> "snapshot.json"
    )
    val rows = read(spark, options).select("id", "vector", "category").collect()
    assert(rows.length == 14, s"V3 snapshot returned ${rows.length} rows")
    val byId = rows.map(row => row.getLong(0) -> row).toMap
    assert(
      byId.keySet == live.map(_.id).toSet,
      "V3 deletes or row identities differ"
    )
    live.foreach { expected =>
      val row = byId(expected.id)
      assert(
        row.getSeq[Float](1).toSeq == expected.vector,
        s"vector ${expected.id}"
      )
      assert(
        Option(row.get(2)).map(_.asInstanceOf[Long]) == expected.category,
        s"nullable category ${expected.id}"
      )
    }
    val nulls = read(spark, options)
      .filter(col("category").isNull)
      .select("id")
      .collect()
      .map(_.getLong(0))
      .toSeq
    assert(nulls == Seq(3001L), s"nullable filter: $nulls")
    passed("v3-snapshot")

  }

  private def values(frame: DataFrame): Map[Long, (Option[Long], Seq[Float])] =
    frame
      .select("id", "category", "vector")
      .collect()
      .map { row =>
        row.getLong(0) -> (
          Option(row.get(1)).map(_.asInstanceOf[Long]),
          row.getSeq[Float](2).toSeq
        )
      }
      .toMap

  private def columnReplacement(spark: SparkSession, directory: Path): Unit = {
    ReadFixtures.copyTo("collections/basic", directory)
    val sourceSnapshot = directory.resolve("snapshot.json")
    val sourceBytes = Files.readAllBytes(sourceSnapshot)
    val options =
      storage(directory) + (MilvusOption.SnapshotPath -> "snapshot.json")
    val before = values(read(spark, options))
    val physicalOptions = options ++ Map(
      MilvusOption.ReadMode -> MilvusOption.ReadModePhysical,
      MilvusOption.ReadIncludeRowId -> "true"
    )
    val source = read(spark, physicalOptions)
    val identities = source.select("id", RowIdentity.ColumnName).collect()
    assert(identities.map(_.getLong(0)).toSet == entities.map(_.id).toSet)
    assert(identities.length == 16, "physical read must retain deleted rows")
    identities.map(_.getStruct(1)).groupBy(_.getLong(1)).foreach {
      case (segment, rows) =>
        assert(Set(30L, 31L)(segment))
        assert(rows.forall(_.getLong(0) == 1L))
        assert(rows.map(_.getLong(2)).sorted.toSeq == (0L until 8L))
    }
    passed("physical-read")

    val replacement = source.select(
      col(RowIdentity.ColumnName),
      when(col("id") === 3001L, lit(11L))
        .when(col("id") === 3002L, lit(22L))
        .when(col("id") === 3104L, lit(null).cast("long"))
        .when(col("id") === 3107L, lit(77L))
        .otherwise(col("category"))
        .alias("category")
    )
    val result = directory.resolve("write-result")
    replacement
      .repartition(3)
      .write
      .format("milvus")
      .options(options)
      .option(MilvusOption.WriteOperation, "replace_columns")
      .mode("append")
      .save(result.toString)

    val receipt = spark.read
      .format("milvus")
      .options(storage(directory))
      .option(MilvusOption.ReadMode, MilvusOption.ReadModeWriteResult)
      .load(result.toString)
      .collect()
    assert(receipt.length == 2)
    val versions = receipt.map { row =>
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
      row.getAs[Long]("segment_id") -> row.getAs[Long]("version")
    }.toMap
    assert(versions.keySet == Set(30L, 31L))
    assert(Files.isRegularFile(result.resolve("_result.json")))
    assert(!Files.exists(result.resolve("_failure.json")))
    passed("column-replacement-receipt")

    // Only the caller changes which revision a new snapshot pins. The connector
    // neither registers these revisions with Milvus nor changes the old snapshot.
    val updated = mapper.readTree(sourceBytes).asInstanceOf[ObjectNode]
    updated.get("storagev2_manifest_list").elements().asScala.foreach { item =>
      val node = item.asInstanceOf[ObjectNode]
      val manifest =
        mapper.readTree(node.get("manifest").asText()).asInstanceOf[ObjectNode]
      manifest.put("ver", versions(node.get("segmentID").asLong()))
      node.put("manifest", mapper.writeValueAsString(manifest))
    }
    Files.write(
      directory.resolve("snapshot-after.json"),
      mapper.writeValueAsBytes(updated)
    )
    val afterOptions =
      options + (MilvusOption.SnapshotPath -> "snapshot-after.json")
    val changes = Map(
      3001L -> Some(11L),
      3002L -> Some(22L),
      3104L -> None,
      3107L -> Some(77L)
    )
    val expected = entities.map { entity =>
      entity.id -> (changes.getOrElse(
        entity.id,
        entity.category
      ), entity.vector)
    }.toMap
    assert(
      values(read(spark, afterOptions)) == expected.filter { case (id, _) =>
        !Set(3002L, 3105L)(id)
      },
      "new revisions must retain deletes and unchanged vectors"
    )
    assert(
      values(
        read(
          spark,
          physicalOptions +
            (MilvusOption.SnapshotPath -> "snapshot-after.json")
        )
      ) == expected,
      "new revisions must replace values of every stored row, including deleted rows"
    )
    passed("column-replacement-readback")
    assert(Files.readAllBytes(sourceSnapshot).sameElements(sourceBytes))
    assert(
      values(read(spark, options)) == before,
      "source snapshot values changed"
    )
    passed("source-snapshot-unchanged")
  }

  def main(args: Array[String]): Unit = {
    val root = Files.createTempDirectory("artifact-consumer-")
    var spark: SparkSession = null
    try {
      spark = SparkSession
        .builder()
        .master(sys.props.getOrElse("milvus.smoke.master", "local[2]"))
        .appName("artifact-consumer-smoke")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.warehouse.dir", root.resolve("warehouse").toString)
        .getOrCreate()
      spark.sparkContext.setLogLevel("WARN")
      if (spark.sparkContext.master.startsWith("local-cluster[")) {
        val driver = ProcessHandle.current().pid()
        val workers = spark.sparkContext
          .parallelize(Seq(1, 2), 2)
          .mapPartitions(_ => Iterator(ProcessHandle.current().pid()))
          .collect()
        assert(workers.nonEmpty && workers.forall(_ != driver))
        passed("independent-executor")
      }
      snapshotReads(spark, root.resolve("basic"))
      pathEntries(spark, root.resolve("basic"))
      columnReplacement(spark, root.resolve("replacement"))
      println("ALL PASS")
    } finally {
      if (spark != null) spark.stop()
      val paths = Files.walk(root)
      try paths.sorted(Comparator.reverseOrder[Path]()).forEach(Files.delete(_))
      finally paths.close()
    }
  }
}
