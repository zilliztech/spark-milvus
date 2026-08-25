package com.zilliz.spark.connector.read

import java.nio.file.Files
import java.nio.file.Paths

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path => HPath}
import org.apache.parquet.example.data.simple.SimpleGroupFactory
import org.apache.parquet.example.data.Group
import org.apache.parquet.hadoop.example.{
  ExampleParquetWriter,
  GroupWriteSupport
}
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.schema.{MessageType, Types}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import io.milvus.grpc.schema.{CollectionSchema, DataType}

/** Tests for [[BackupMetaReader]] — parses milvus-backup's `full_meta.json` and
  * maps it to the packed-V2 read-path objects. Local parquet files written with
  * parquet-mr's example writer carry `PARQUET:field_id` the same way
  * milvus-storage does, so the field-id / row-count recovery can be exercised
  * without minio.
  */
class BackupMetaReaderTest extends AnyFunSuite with Matchers {

  private val groupASchema: MessageType = Types
    .buildMessage()
    .required(PrimitiveTypeName.INT64)
    .id(100)
    .named("pk")
    .required(PrimitiveTypeName.INT64)
    .id(0)
    .named("row_id")
    .required(PrimitiveTypeName.INT64)
    .id(1)
    .named("ts")
    .named("milvus_group")

  private val groupCSchema: MessageType = Types
    .buildMessage()
    .required(PrimitiveTypeName.BINARY)
    .id(101)
    .named("name")
    .named("milvus_group")

  /** Write a local parquet with the given rows (one `Group` per row) and return
    * its `file://` URI.
    */
  private def writeParquet(
      schema: MessageType,
      rows: List[SimpleGroupFactory => Group]
  ): String = {
    val tmp = Files.createTempFile("milvus-backup-test-", ".parquet")
    Files.delete(tmp)
    val conf = new Configuration()
    GroupWriteSupport.setSchema(schema, conf)
    val writer = ExampleParquetWriter
      .builder(new HPath(tmp.toUri))
      .withType(schema)
      .withConf(conf)
      .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
      .build()
    try {
      val factory = new SimpleGroupFactory(schema)
      rows.foreach(build => writer.write(build(factory)))
    } finally {
      writer.close()
    }
    tmp.toUri.toString
  }

  private def parquetA: String =
    writeParquet(
      groupASchema,
      List(
        f =>
          f.newGroup()
            .append("pk", 1L)
            .append("row_id", 10L)
            .append("ts", 100L),
        f =>
          f.newGroup().append("pk", 2L).append("row_id", 11L).append("ts", 101L)
      )
    )

  private def parquetB: String =
    writeParquet(
      groupASchema,
      List(f =>
        f.newGroup().append("pk", 3L).append("row_id", 12L).append("ts", 102L)
      )
    )

  private def parquetC: String =
    writeParquet(
      groupCSchema,
      List(
        f => f.newGroup().append("name", "a"),
        f => f.newGroup().append("name", "b"),
        f => f.newGroup().append("name", "c")
      )
    )

  private def backupFixture(
      format: String = "",
      groupAPath: String,
      groupBPath: String,
      groupCPath: String,
      deltaPath: String
  ): String = {
    s"""{
       |  "id": "task-1",
       |  "name": "b1",
       |  "format": "$format",
       |  "milvus_version": "v2.6.0",
       |  "collection_backups": [
       |    {
       |      "collection_id": 444,
       |      "collection_name": "demo",
       |      "db_name": "default",
       |      "schema": {
       |        "name": "demo",
       |        "description": "",
       |        "autoID": false,
       |        "enable_dynamic_field": false,
       |        "fields": [
       |          {"fieldID": 100, "name": "id", "is_primary_key": true, "description": "", "autoID": false, "data_type": 5, "type_params": [], "index_params": [], "state": 0, "element_type": 0, "is_dynamic": false, "is_partition_key": false, "nullable": false, "is_function_output": false},
       |          {"fieldID": 101, "name": "name", "is_primary_key": false, "description": "", "autoID": false, "data_type": 21, "type_params": [{"key": "max_length", "value": "64"}], "index_params": [], "state": 0, "element_type": 0, "is_dynamic": false, "is_partition_key": false, "nullable": false, "is_function_output": false},
       |          {"fieldID": 102, "name": "vector", "is_primary_key": false, "description": "", "autoID": false, "data_type": 101, "type_params": [{"key": "dim", "value": "8"}], "index_params": [], "state": 0, "element_type": 0, "is_dynamic": false, "is_partition_key": false, "nullable": false, "is_function_output": false}
       |        ]
       |      },
       |      "shards_num": 1,
       |      "consistency_level": 2,
       |      "partition_backups": [
       |        {
       |          "partition_id": 555,
       |          "partition_name": "_default",
       |          "collection_id": 444,
       |          "segment_backups": [
       |            {
       |              "segment_id": 777,
       |              "collection_id": 444,
       |              "partition_id": 555,
       |              "num_of_rows": 3,
       |              "binlogs": [
       |                {"fieldID": 103, "binlogs": [
       |                  {"log_path": "$groupAPath", "log_size": 100, "log_id": 1},
       |                  {"log_path": "$groupBPath", "log_size": 100, "log_id": 2}
       |                ]},
       |                {"fieldID": 101, "binlogs": [
       |                  {"log_path": "$groupCPath", "log_size": 100, "log_id": 1}
       |                ]}
       |              ],
       |              "deltalogs": [],
       |              "size": 300,
       |              "group_id": 777,
       |              "is_l0": false,
       |              "v_channel": "by-dev-rootcoord-dml-0",
       |              "storage_version": 2
       |            }
       |          ],
       |          "size": 300
       |        }
       |      ],
       |      "size": 300,
       |      "has_index": false,
       |      "index_infos": [],
       |      "load_state": "Loaded",
       |      "l0_segments": [
       |        {
       |          "segment_id": 888,
       |          "collection_id": 444,
       |          "partition_id": -1,
       |          "num_of_rows": 0,
       |          "binlogs": [],
       |          "deltalogs": [
       |            {"fieldID": 0, "binlogs": [{"log_path": "$deltaPath", "log_size": 10, "log_id": 1}]}
       |          ],
       |          "size": 10,
       |          "group_id": 0,
       |          "is_l0": true,
       |          "v_channel": "by-dev-rootcoord-dml-0",
       |          "storage_version": 2
       |        }
       |      ]
       |    }
       |  ],
       |  "size": 310,
       |  "milvus_version": "v2.6.0"
       |}""".stripMargin
  }

  test("metaPath joins the backup dir with meta/full_meta.json") {
    BackupMetaReader.metaPath("s3a://bucket/backup/b1") shouldBe
      "s3a://bucket/backup/b1/meta/full_meta.json"
    BackupMetaReader.metaPath("s3a://bucket/backup/b1/") shouldBe
      "s3a://bucket/backup/b1/meta/full_meta.json"
    BackupMetaReader.metaPath("/data/backups/b1") shouldBe
      "/data/backups/b1/meta/full_meta.json"
  }

  test("readMeta parses a binlog-format backup full_meta.json") {
    val dir = Files.createTempDirectory("milvus-backup-meta-")
    try {
      val metaDir = Paths.get(dir.toString, "meta")
      Files.createDirectories(metaDir)
      val json =
        backupFixture(
          groupAPath = "backup/b1/binlogs/insert_log/444/555/777/103/1",
          groupBPath = "backup/b1/binlogs/insert_log/444/555/777/103/2",
          groupCPath = "backup/b1/binlogs/insert_log/444/555/777/101/1",
          deltaPath = "backup/b1/binlogs/delta_log/444/-1/888/1"
        )
      Files.write(
        Paths.get(metaDir.toString, "full_meta.json"),
        json.getBytes("UTF-8")
      )

      val meta = BackupMetaReader
        .readMeta(new Configuration(), dir.toString)
        .getOrElse(fail("expected Right"))
      meta.name shouldBe "b1"
      meta.isSnapshotFormat shouldBe false
      meta.collectionBackups should have size 1

      val coll = meta.collectionBackups.head
      coll.collectionId shouldBe 444L
      coll.collectionName shouldBe "demo"
      coll.schema.isDefined shouldBe true
      coll.schema.get.fields should have size 3
      coll.schema.get.fields.head.fieldId shouldBe 100L
      coll.allSegments should have size 2 // 777 (L1) + 888 (L0)
    } finally {
      deleteRecursively(dir)
    }
  }

  test("toProtobufSchemaBytes round-trips the backup collection schema") {
    val json =
      backupFixture(
        groupAPath = "x",
        groupBPath = "y",
        groupCPath = "z",
        deltaPath = "d"
      )
    val meta = BackupMetaReader.parse(json).getOrElse(fail("expected Right"))
    val schema = meta.collectionBackups.head.schema.get

    val parsed = CollectionSchema.parseFrom(
      BackupMetaReader.toProtobufSchemaBytes(schema)
    )
    parsed.name shouldBe "demo"
    parsed.fields should have size 3

    val byId = parsed.fields.map(f => f.fieldID -> f).toMap
    byId(100L).isPrimaryKey shouldBe true
    byId(100L).dataType shouldBe DataType.Int64
    byId(101L).dataType shouldBe DataType.VarChar
    byId(101L).typeParams.find(_.key == "max_length").get.value shouldBe "64"
    byId(102L).dataType shouldBe DataType.FloatVector
    byId(102L).typeParams.find(_.key == "dim").get.value shouldBe "8"
  }

  test("toV2Segments recovers column groups, field ids and row counts") {
    val a = parquetA
    val b = parquetB
    val c = parquetC
    val meta = BackupMetaReader
      .parse(
        backupFixture(
          groupAPath = a,
          groupBPath = b,
          groupCPath = c,
          deltaPath = "dummy/delta.log"
        )
      )
      .getOrElse(fail("expected Right"))

    val segments = BackupMetaReader
      .toV2Segments(meta, new Configuration(), bucket = "")
      .getOrElse(fail("expected Right"))

    segments should have size 2
    val seg = segments.find(_.segmentId == 777L).get
    seg.partitionId shouldBe 555L
    seg.numOfRows shouldBe 3L
    seg.storageVersion shouldBe 2L
    seg.columnGroups should have size 2

    val slot103 = seg.columnGroups.find(_.slotFieldId == 103L).get
    slot103.fieldIds shouldBe Seq(100L, 0L, 1L)
    slot103.filePaths shouldBe Seq(a, b)
    slot103.fileRowCounts shouldBe Seq(2L, 1L)

    val slot101 = seg.columnGroups.find(_.slotFieldId == 101L).get
    slot101.fieldIds shouldBe Seq(101L)
    slot101.filePaths shouldBe Seq(c)
    slot101.fileRowCounts shouldBe Seq(3L)

    val l0 = segments.find(_.segmentId == 888L).get
    l0.columnGroups shouldBe empty
    l0.deltaLogs should have size 1
    l0.deltaLogs.head.logPath shouldBe "dummy/delta.log"
  }

  test("toV2Segments skips L0 segments when applyDeletes is false") {
    val meta = BackupMetaReader
      .parse(
        backupFixture(
          groupAPath = parquetA,
          groupBPath = parquetB,
          groupCPath = parquetC,
          deltaPath = "dummy/delta.log"
        )
      )
      .getOrElse(fail("expected Right"))
    val segments = BackupMetaReader
      .toV2Segments(
        meta,
        new Configuration(),
        bucket = "",
        applyDeletes = false
      )
      .getOrElse(fail("expected Right"))
    segments.map(_.segmentId) shouldBe Seq(777L)
  }

  test("buildV2Segment rejects StorageV3+ and skips StorageV1") {
    val v3 = BackupMetaReader.SegmentBackup(
      segmentId = 1L,
      storageVersion = 3L
    )
    BackupMetaReader
      .buildV2Segment(v3, new Configuration(), "", applyDeletes = true)
      .left
      .toOption
      .get
      .getMessage should include("StorageV3")

    val v1 = BackupMetaReader.SegmentBackup(
      segmentId = 2L,
      storageVersion = 0L
    )
    BackupMetaReader
      .buildV2Segment(v1, new Configuration(), "", applyDeletes = true) shouldBe
      Right(None)
  }

  test("toV2Segments rejects snapshot-format backups") {
    val meta = BackupMetaReader
      .parse(
        backupFixture(
          format = "snapshot",
          groupAPath = parquetA,
          groupBPath = parquetB,
          groupCPath = parquetC,
          deltaPath = "dummy/delta.log"
        )
      )
      .getOrElse(fail("expected Right"))
    val result =
      BackupMetaReader.toV2Segments(meta, new Configuration(), bucket = "")
    result.isLeft shouldBe true
    result.left.toOption.get.getMessage should include("snapshot format")
  }

  private def deleteRecursively(dir: java.nio.file.Path): Unit = {
    import scala.jdk.CollectionConverters._
    if (Files.exists(dir)) {
      Files.list(dir).iterator().asScala.foreach { p =>
        if (Files.isDirectory(p)) deleteRecursively(p)
        else Files.deleteIfExists(p)
      }
      Files.deleteIfExists(dir)
    }
  }
}
