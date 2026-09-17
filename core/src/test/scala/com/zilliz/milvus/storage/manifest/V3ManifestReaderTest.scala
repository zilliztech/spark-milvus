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

  test("parseDeltaLogs reads primary-key StorageV3 manifest deltalogs") {
    val bytes = writeManifest(
      Seq(
        ("9001", 0, 3L),
        ("ignored-positional", 1, 5L),
        ("ignored-empty", 0, 0L)
      )
    )

    val result = V3ManifestReader.parseDeltaLogs(
      bytes,
      "files/insert_log/10/20/30"
    )

    result shouldBe a[Right[_, _]]
    result.toOption.get shouldBe Seq(
      DeltaLogFile(
        0L,
        "files/insert_log/10/20/30/_delta/9001",
        3L
      )
    )
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
      stats: Map[String, (Seq[String], Map[String, String])] = Map.empty
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
