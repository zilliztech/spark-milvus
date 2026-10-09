package com.zilliz.milvus.storage.manifest

import java.io.ByteArrayOutputStream
import java.nio.file.Files
import scala.jdk.CollectionConverters._

import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.avro.Schema
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.io.LocalObjectStore
import com.zilliz.milvus.storage.snapshot.DeltaLogFile

class V3ManifestReaderTest extends AnyFunSuite with Matchers {
  private val schema = new Schema.Parser().parse("""
      {
        "type": "record",
        "name": "Manifest",
        "namespace": "milvus_storage",
        "fields": [
          {
            "name": "delta_logs",
            "type": {
              "type": "array",
              "items": {
                "type": "record",
                "name": "DeltaLog",
                "fields": [
                  {"name": "path", "type": "string"},
                  {"name": "type", "type": "int"},
                  {"name": "num_entries", "type": "long"}
                ]
              }
            }
          },
          {
            "name": "column_groups",
            "type": {
              "type": "array",
              "items": {
                "type": "record",
                "name": "ColumnGroup",
                "fields": [
                  {"name": "columns", "type": {"type": "array", "items": "string"}},
                  {
                    "name": "files",
                    "type": {
                      "type": "array",
                      "items": {
                        "type": "record",
                        "name": "ColumnGroupFile",
                        "fields": [
                          {"name": "path", "type": "string"},
                          {"name": "start_index", "type": "long"},
                          {"name": "end_index", "type": "long"}
                        ]
                      }
                    }
                  }
                ]
              }
            }
          },
          {
            "name": "stats",
            "type": {
              "type": "map",
              "values": {
                "type": "record",
                "name": "SegmentStatistic",
                "fields": [
                  {"name": "paths", "type": {"type": "array", "items": "string"}},
                  {"name": "metadata", "type": {"type": "map", "values": "string"}}
                ]
              }
            }
          }
        ]
      }
    """)

  test("parse reads the primary-key StorageV3 manifest deltalogs") {
    val bytes = writeManifest(
      Seq(
        ("9001", 0, 3L),
        ("ignored-positional", 1, 5L),
        ("ignored-empty", 0, 0L)
      )
    )

    val result = V3ManifestReader.parse(
      bytes,
      "files/insert_log/10/20/30"
    )

    result shouldBe a[Right[_, _]]
    result.toOption.get.deltaLogs shouldBe Seq(
      DeltaLogFile(
        0L,
        "files/insert_log/10/20/30/_delta/9001",
        3L
      )
    )
  }

  test("a segment's rows come from one column group's row ranges") {
    // Every column group covers the same rows, so the first answers for the
    // segment; the manifest states ranges, not a count.
    val bytes = writeManifest(
      deltaLogs = Seq.empty,
      groups = Seq(
        Seq((0L, 8192L), (8192L, 12000L)),
        Seq((0L, 12000L))
      )
    )

    V3ManifestReader
      .parse(bytes, "files/insert_log/10/20/30")
      .toOption
      .get
      .rows shouldBe Some(12000L)
  }

  test("a manifest with no column groups states no rows") {
    V3ManifestReader
      .parse(writeManifest(Seq.empty), "files/insert_log/10/20/30")
      .toOption
      .get
      .rows shouldBe scala.None
  }

  test("a manifest written without column groups still reads") {
    // Avro throws on a field the writer's schema never had. A manifest older
    // than column-group row ranges must still give up its delete files.
    val older = new Schema.Parser().parse("""
      {
        "type": "record",
        "name": "Manifest",
        "namespace": "milvus_storage",
        "fields": [
          {
            "name": "delta_logs",
            "type": {
              "type": "array",
              "items": {
                "type": "record",
                "name": "DeltaLog",
                "fields": [
                  {"name": "path", "type": "string"},
                  {"name": "type", "type": "int"},
                  {"name": "num_entries", "type": "long"}
                ]
              }
            }
          }
        ]
      }
    """)
    val rec = new GenericData.Record(older)
    val logs = new GenericData.Array[GenericRecord](
      1,
      older.getField("delta_logs").schema()
    )
    val log = new GenericData.Record(
      older.getField("delta_logs").schema().getElementType
    )
    log.put("path", "9001")
    log.put("type", 0)
    log.put("num_entries", 3L)
    logs.add(log)
    rec.put("delta_logs", logs)
    val out = new java.io.ByteArrayOutputStream()
    val writer = new DataFileWriter[GenericRecord](
      new GenericDatumWriter[GenericRecord](older)
    )
    writer.create(older, out)
    writer.append(rec)
    writer.close()

    val facts = V3ManifestReader
      .parse(out.toByteArray, "files/insert_log/10/20/30")
      .toOption
      .get
    facts.rows shouldBe scala.None
    facts.deltaLogs.map(_.entriesNum) shouldBe Seq(3L)
  }

  test("manifestFilePath builds the StorageV3 metadata avro path") {
    V3ManifestReader.manifestFilePath(
      "files/insert_log/10/20/30",
      7L
    ) shouldBe "files/insert_log/10/20/30/_metadata/manifest-7.avro"
  }

  test("parseStatistics resolves relative paths and keeps metadata") {
    val bytes = writeManifest(
      Seq.empty,
      Map(
        "bloom_filter.100" -> (
          Seq(
            "files/insert_log/10/20/30/_stats/bloom_filter.100/1",
            "_stats/bloom_filter.100/9",
            "s3://other/files/insert_log/10/20/30/_stats/bloom_filter.100/11"
          ),
          Map("memory_size" -> "2048")
        )
      )
    )

    V3ManifestReader
      .parseStatistics(bytes, "files/insert_log/10/20/30") shouldBe Right(
      Map(
        "bloom_filter.100" -> ManifestStatistic(
          Seq(
            "files/insert_log/10/20/30/_stats/bloom_filter.100/1",
            "files/insert_log/10/20/30/_stats/bloom_filter.100/9",
            "s3a://other/files/insert_log/10/20/30/_stats/bloom_filter.100/11"
          ),
          Map("memory_size" -> "2048")
        )
      )
    )
  }

  test("resolveManifestStatisticsPath preserves a local absolute path") {
    V3ManifestReader.resolveManifestStatisticsPath(
      "/tmp/segment",
      "/tmp/segment/_stats/bloom_filter.100/1"
    ) shouldBe "/tmp/segment/_stats/bloom_filter.100/1"
  }

  test("resolveManifestDeltaPath preserves absolute deltalog paths") {
    V3ManifestReader.resolveManifestDeltaPath(
      "files/insert_log/10/20/30",
      "s3://bucket/files/insert_log/10/20/30/_delta/9001"
    ) shouldBe "s3a://bucket/files/insert_log/10/20/30/_delta/9001"
  }

  test("latestManifestVersion returns greatest StorageV3 manifest version") {
    val basePath = Files.createTempDirectory("milvus-v3-manifest-test")
    val metadataPath = Files.createDirectory(basePath.resolve("_metadata"))
    Files.createFile(metadataPath.resolve("manifest-2.avro"))
    Files.createFile(metadataPath.resolve("manifest-11.avro"))
    Files.createFile(metadataPath.resolve("manifest-not-a-version.avro"))
    Files.createFile(metadataPath.resolve("other"))

    V3ManifestReader.latestManifestVersion(
      basePath.toString,
      "",
      new LocalObjectStore()
    ) shouldBe Right(11L)
  }

  private def writeManifest(
      deltaLogs: Seq[(String, Int, Long)],
      stats: Map[String, (Seq[String], Map[String, String])] = Map.empty,
      groups: Seq[Seq[(Long, Long)]] = Seq.empty
  ): Array[Byte] = {
    val deltaSchema = schema
      .getField("delta_logs")
      .schema()
      .getElementType
    val rec = new GenericData.Record(schema)
    val arr = new GenericData.Array[GenericRecord](
      deltaLogs.size,
      schema.getField("delta_logs").schema()
    )
    deltaLogs.foreach { case (path, logType, entries) =>
      val log = new GenericData.Record(deltaSchema)
      log.put("path", path)
      log.put("type", logType)
      log.put("num_entries", entries)
      arr.add(log)
    }
    rec.put("delta_logs", arr)
    val groupSchema = schema.getField("column_groups").schema()
    val groupArray =
      new GenericData.Array[GenericRecord](groups.size, groupSchema)
    groups.foreach { files =>
      val group = new GenericData.Record(groupSchema.getElementType)
      val columnsSchema =
        groupSchema.getElementType.getField("columns").schema()
      val columns = new GenericData.Array[String](1, columnsSchema)
      columns.add("100")
      group.put("columns", columns)
      val filesSchema = groupSchema.getElementType.getField("files").schema()
      val fileArray =
        new GenericData.Array[GenericRecord](files.size, filesSchema)
      files.foreach { case (start, end) =>
        val file = new GenericData.Record(filesSchema.getElementType)
        file.put("path", s"0_$start.parquet")
        file.put("start_index", start)
        file.put("end_index", end)
        fileArray.add(file)
      }
      group.put("files", fileArray)
      groupArray.add(group)
    }
    rec.put("column_groups", groupArray)
    val statsSchema = schema.getField("stats").schema().getValueType
    rec.put(
      "stats",
      stats.map { case (name, (paths, metadata)) =>
        val stat = new GenericData.Record(statsSchema)
        stat.put("paths", paths.asJava)
        stat.put("metadata", metadata.asJava)
        name -> stat
      }.asJava
    )

    val out = new ByteArrayOutputStream()
    val writer =
      new DataFileWriter[GenericRecord](new GenericDatumWriter[GenericRecord]())
    writer.create(schema, out)
    writer.append(rec)
    writer.close()
    out.toByteArray
  }
}
