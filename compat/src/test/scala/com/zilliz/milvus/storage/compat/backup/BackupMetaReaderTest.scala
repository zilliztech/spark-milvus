package com.zilliz.milvus.storage.compat.backup

import java.nio.file.{Files, Paths}
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

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

import com.zilliz.milvus.storage.snapshot.{Segment, V2ColumnGroup}
import io.milvus.grpc.schema.{CollectionSchema, DataType}

/** Tests for [[BackupMetaReader]] — parses milvus-backup's `full_meta.json` and
  * maps it to the packed-V2 read-path objects.
  *
  * The meta fixtures carry **Milvus source keys** for `log_path` (e.g.
  * `files/insert_log/...`) exactly like a real export — milvus-backup records
  * the source key and copies the data under a separate `DestKey`. The tests
  * materialize that `DestKey` layout on local disk and assert that the reader
  * reconstructs those backup paths (never the source keys).
  */
class BackupMetaReaderTest extends AnyFunSuite with Matchers {

  /** Tests read local files, so the store carries no bucket. */
  private def localStore: com.zilliz.milvus.storage.io.ObjectStore =
    new com.zilliz.milvus.storage.io.LocalObjectStore()

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

  /** Write a parquet file at `target` (creating parent dirs) with one `Group`
    * per row.
    */
  private def writeParquetAt(
      target: java.nio.file.Path,
      schema: MessageType,
      rows: List[SimpleGroupFactory => Group]
  ): Unit = {
    Files.createDirectories(target.getParent)
    val conf = new org.apache.hadoop.conf.Configuration()
    GroupWriteSupport.setSchema(schema, conf)
    val writer = ExampleParquetWriter
      .builder(new HPath(target.toUri))
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
  }

  /** Materialize the backup object layout milvus-backup's `DestKey` produces
    * under `root` for the fixture segments:
    *
    * {{{
    *   binlogs/insert_log/444/555/777/777/103/{1,2}   (2 rows, 1 row)
    *   binlogs/insert_log/444/555/777/777/101/1        (3 rows)
    * }}}
    *
    * Returns the backup dir path string.
    */
  private def materializeBackup(root: java.nio.file.Path): String = {
    val seg =
      Paths.get(
        root.toString,
        "binlogs",
        "insert_log",
        "444",
        "555",
        "777",
        "777"
      )
    writeParquetAt(
      Paths.get(seg.toString, "103", "1"),
      groupASchema,
      List(
        f =>
          f.newGroup()
            .append("pk", 1L)
            .append("row_id", 10L)
            .append("ts", 100L),
        f =>
          f.newGroup()
            .append("pk", 2L)
            .append("row_id", 11L)
            .append("ts", 101L),
        f =>
          f.newGroup().append("pk", 3L).append("row_id", 12L).append("ts", 102L)
      )
    )
    writeParquetAt(
      Paths.get(seg.toString, "101", "1"),
      groupCSchema,
      List(
        f => f.newGroup().append("name", "a"),
        f => f.newGroup().append("name", "b"),
        f => f.newGroup().append("name", "c")
      )
    )
    root.toString
  }

  /** Materialize a backup whose slot 103 column group spans **two** binlog
    * files (mirrors the `multiFile = true` fixture: `103/1` + `103/2`), with
    * the same total rows as the single-file [[materializeBackup]] shape.
    */
  private def materializeMultiFileBackup(root: java.nio.file.Path): String = {
    val seg =
      Paths.get(
        root.toString,
        "binlogs",
        "insert_log",
        "444",
        "555",
        "777",
        "777"
      )
    writeParquetAt(
      Paths.get(seg.toString, "103", "1"),
      groupASchema,
      List(
        f =>
          f.newGroup()
            .append("pk", 1L)
            .append("row_id", 10L)
            .append("ts", 100L),
        f =>
          f.newGroup()
            .append("pk", 2L)
            .append("row_id", 11L)
            .append("ts", 101L)
      )
    )
    writeParquetAt(
      Paths.get(seg.toString, "103", "2"),
      groupASchema,
      List(f =>
        f.newGroup().append("pk", 3L).append("row_id", 12L).append("ts", 102L)
      )
    )
    writeParquetAt(
      Paths.get(seg.toString, "101", "1"),
      groupCSchema,
      List(
        f => f.newGroup().append("name", "a"),
        f => f.newGroup().append("name", "b"),
        f => f.newGroup().append("name", "c")
      )
    )
    root.toString
  }

  private def backupFixture(
      format: String = "",
      groupAPath: String,
      groupBPath: String,
      groupCPath: String,
      deltaPath: String,
      multiFile: Boolean = true
  ): String = {
    val groupBBinlog =
      if (multiFile)
        """,
          |                  {"log_path": "$groupBPath", "log_size": 100, "log_id": 2}""".stripMargin
      else ""
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
        |                  {"log_path": "$groupAPath", "log_size": 100, "log_id": 1}$groupBBinlog
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

  // Milvus source keys, as a real full_meta.json records them.
  private val SourceKeys =
    (
      "files/insert_log/444/555/777/103/1",
      "files/insert_log/444/555/777/103/2",
      "files/insert_log/444/555/777/101/1",
      "files/delta_log/444/-1/888/1"
    )

  test("metaPath joins the backup dir with meta/full_meta.json") {
    BackupMetaReader.metaPath("s3a://bucket/backup/b1") shouldBe
      "s3a://bucket/backup/b1/meta/full_meta.json"
    BackupMetaReader.metaPath("s3a://bucket/backup/b1/") shouldBe
      "s3a://bucket/backup/b1/meta/full_meta.json"
    BackupMetaReader.metaPath("/data/backups/b1") shouldBe
      "/data/backups/b1/meta/full_meta.json"
  }

  test(
    "path builders split qualified URIs (Hadoop) from bucket-relative keys (native)"
  ) {
    // A groupId distinct from segmentId pins the {group}/{seg} level order —
    // groupId == segmentId would not discriminate the two.
    val seg = BackupMetaReader.SegmentBackup(
      segmentId = 777L,
      collectionId = 444L,
      partitionId = 555L,
      groupId = 999L
    )
    BackupMetaReader.backupKeyBase(
      "s3a://bucket/backup/b1"
    ) shouldBe "backup/b1"
    BackupMetaReader.backupKeyBase("/data/backup/b1") shouldBe "/data/backup/b1"
    // Empty-authority URIs keep the leading slash, matching the bare form.
    BackupMetaReader.backupKeyBase("file:///data/backup/b1") shouldBe
      "/data/backup/b1"
    // Backup at the bucket root: empty bucket-relative prefix, and trailing
    // double-slashes must collapse to a single location.
    BackupMetaReader.backupKeyBase("s3a://bucket") shouldBe ""
    BackupMetaReader.backupKeyBase("s3a://b/backup/b1//") shouldBe "backup/b1"
    BackupMetaReader.metaPath("s3a://b/backup/b1//") shouldBe
      "s3a://b/backup/b1/meta/full_meta.json"

    // Every insert-log read goes through the native store, so only the
    // bucket-relative key is produced.
    BackupMetaReader.nativeInsertLogPath(
      "s3a://bucket/backup/b1",
      seg,
      103L,
      1L
    ) shouldBe "backup/b1/binlogs/insert_log/444/555/999/777/103/1"
    // Bucket-root backup: native key has NO leading slash (a different S3 key).
    BackupMetaReader.nativeInsertLogPath("s3a://bucket", seg, 103L, 1L) shouldBe
      "binlogs/insert_log/444/555/999/777/103/1"

    // Delta logs only feed the Hadoop delete-plan reader, so only qualified
    // paths exist. groupID level present for part != -1, omitted for part == -1.
    BackupMetaReader.qualifiedDeltaLogPath(
      "s3a://bucket/backup/b1",
      seg,
      9L
    ) shouldBe "s3a://bucket/backup/b1/binlogs/delta_log/444/555/999/777/9"
    val l0 = seg.copy(partitionId = -1L, groupId = 0L)
    BackupMetaReader.qualifiedDeltaLogPath(
      "s3a://bucket/backup/b1",
      l0,
      9L
    ) shouldBe "s3a://bucket/backup/b1/binlogs/delta_log/444/-1/777/9"
  }

  test(
    "Segment.dedupColumnGroupsBySlot keeps the newest slot per field"
  ) {
    val oldGroup = V2ColumnGroup(
      fieldIds = Seq(100L, 0L, 1L),
      filePaths = Seq("p1"),
      fileRowCounts = Seq(2L),
      slotFieldId = 3L
    )
    val newGroup = V2ColumnGroup(
      fieldIds = Seq(100L),
      filePaths = Seq("p2"),
      fileRowCounts = Seq(1L),
      slotFieldId = 100L
    )
    val seg = Segment.v2(
      id = 1L,
      partitionId = 1L,
      rows = 3L,
      columnGroups = Seq(oldGroup, newGroup)
    )
    val deduped = seg.dedupColumnGroupsBySlot
    // The old group keeps its unique fields (0, 1); the shared field 100 is
    // stripped from it and read from the newer slot (100).
    deduped.columnGroups.map(_.slotFieldId) shouldBe Seq(3L, 100L)
    deduped.columnGroups.head.fieldIds shouldBe Seq(0L, 1L)
    deduped.columnGroups.last.fieldIds shouldBe Seq(100L)

    // Unknown slot (-1) disables dedup entirely.
    val unknown = V2ColumnGroup(
      fieldIds = Seq(100L),
      filePaths = Seq("p"),
      fileRowCounts = Seq(1L),
      slotFieldId = -1L
    )
    Segment
      .v2(
        id = 2L,
        partitionId = 1L,
        rows = 1L,
        columnGroups = Seq(unknown)
      )
      .dedupColumnGroupsBySlot
      .columnGroups shouldBe Seq(unknown)
  }

  test(
    "deleteOnlySegments returns delete-only segments with qualified delta paths"
  ) {
    val meta = BackupMetaReader
      .parse(
        backupFixture(
          groupAPath = SourceKeys._1,
          groupBPath = SourceKeys._2,
          groupCPath = SourceKeys._3,
          deltaPath = SourceKeys._4
        )
      )
      .getOrElse(fail("expected Right"))
    val l0 = BackupMetaReader.deleteOnlySegments(
      meta,
      444L,
      "s3a://bucket/backup/b1"
    )
    l0.map(_.id) shouldBe Seq(888L)
    l0.head.columnGroups shouldBe Seq.empty
    l0.head.deltaLogs.map(_.logPath) shouldBe Seq(
      "s3a://bucket/backup/b1/binlogs/delta_log/444/-1/888/1"
    )

    // A non-L0 StorageV2 segment with no binlogs, zero rows and non-empty
    // deltalogs is also delete-only — it must match the planner's inherited
    // predicate so its partition marker has a matching plan entry.
    val nonL0Empty = BackupMetaReader.SegmentBackup(
      segmentId = 999L,
      collectionId = 444L,
      partitionId = 555L,
      groupId = 999L,
      numOfRows = 0L,
      storageVersion = 2L,
      binlogs = Seq.empty,
      deltalogs = Seq(
        BackupMetaReader.FieldBinlog(
          fieldId = 0L,
          binlogs = Seq(
            BackupMetaReader.Binlog(
              logId = 7L,
              logPath = "files/delta_log/444/555/999/7",
              logSize = 10L
            )
          )
        )
      )
    )
    val meta2 = BackupMetaReader.BackupInfo(
      name = "b2",
      collectionBackups = Seq(
        BackupMetaReader.CollectionBackup(
          collectionId = 444L,
          collectionName = "demo",
          partitionBackups = Seq(
            BackupMetaReader.PartitionBackup(
              partitionId = 555L,
              segmentBackups = Seq(nonL0Empty)
            )
          )
        )
      )
    )
    val segs =
      BackupMetaReader.deleteOnlySegments(meta2, 444L, "s3a://bucket/backup/b2")
    segs.map(_.id) shouldBe Seq(999L)
    segs.head.columnGroups shouldBe Seq.empty
    segs.head.deltaLogs.map(_.logPath) shouldBe Seq(
      "s3a://bucket/backup/b2/binlogs/delta_log/444/555/999/999/7"
    )
  }

  test("readMeta parses a binlog-format backup full_meta.json") {
    val dir = Files.createTempDirectory("milvus-backup-meta-")
    try {
      val metaDir = Paths.get(dir.toString, "meta")
      Files.createDirectories(metaDir)
      val json = backupFixture(
        groupAPath = SourceKeys._1,
        groupBPath = SourceKeys._2,
        groupCPath = SourceKeys._3,
        deltaPath = SourceKeys._4
      )
      Files.write(
        Paths.get(metaDir.toString, "full_meta.json"),
        json.getBytes("UTF-8")
      )

      val meta = BackupMetaReader
        .readMeta(localStore, dir.toString)
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

  test("BackupSnapshotSource: schema-only snapshot from a local export") {
    val dir = Files.createTempDirectory("milvus-backup-source-")
    try {
      val metaDir = Paths.get(dir.toString, "meta")
      Files.createDirectories(metaDir)
      Files.write(
        Paths.get(metaDir.toString, "full_meta.json"),
        backupFixture(
          groupAPath = SourceKeys._1,
          groupBPath = SourceKeys._2,
          groupCPath = SourceKeys._3,
          deltaPath = SourceKeys._4
        ).getBytes("UTF-8")
      )
      val source = new BackupSnapshotSource(
        localStore,
        dir.toString,
        databaseName = "",
        collectionName = "demo",
        applyDeletes = true,
        maxJsonBytes = 1L << 20,
        withSegments = false
      )
      val snapshot = source.snapshot().getOrElse(fail("expected Right"))
      snapshot.name shouldBe "b1"
      snapshot.collectionId shouldBe 444L
      snapshot.schema.fields.map(_.name) should contain("id")
      snapshot.segments shouldBe empty
      snapshot.bucket shouldBe ""

      // A read needs the segments, and the native reader needs object
      // storage: a local dir is refused before anything is opened.
      val forRead = new BackupSnapshotSource(
        localStore,
        dir.toString,
        databaseName = "",
        collectionName = "demo",
        applyDeletes = true,
        maxJsonBytes = 1L << 20,
        withSegments = true
      )
      forRead.snapshot().left.get.getMessage should include(
        "object storage URI"
      )

      // An unknown collection is an error, never another collection.
      val wrong = new BackupSnapshotSource(
        localStore,
        dir.toString,
        databaseName = "",
        collectionName = "nope",
        applyDeletes = true,
        maxJsonBytes = 1L << 20,
        withSegments = false
      )
      wrong.snapshot().left.get.getMessage should include("does not contain")
    } finally {
      deleteRecursively(dir)
    }
  }

  test("serialize/parse round-trips the parsed backup meta") {
    val json = backupFixture(
      groupAPath = SourceKeys._1,
      groupBPath = SourceKeys._2,
      groupCPath = SourceKeys._3,
      deltaPath = SourceKeys._4
    )
    val meta = BackupMetaReader.parse(json).getOrElse(fail("expected Right"))
    val roundTripped =
      BackupMetaReader
        .parse(BackupMetaReader.serialize(meta))
        .getOrElse(
          fail("expected Right")
        )
    roundTripped shouldBe meta
  }

  test("toProtobufSchemaBytes round-trips the backup collection schema") {
    val json = backupFixture(
      groupAPath = SourceKeys._1,
      groupBPath = SourceKeys._2,
      groupCPath = SourceKeys._3,
      deltaPath = SourceKeys._4
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

  test(
    "toV2Segments reconstructs backup paths and recovers column groups, field ids and row counts"
  ) {
    val dir = Files.createTempDirectory("milvus-backup-b1-")
    try {
      val backupDir = materializeBackup(dir)
      val meta = BackupMetaReader
        .parse(
          backupFixture(
            groupAPath = SourceKeys._1,
            groupBPath = SourceKeys._2,
            groupCPath = SourceKeys._3,
            deltaPath = SourceKeys._4,
            multiFile = false
          )
        )
        .getOrElse(fail("expected Right"))

      val segments = BackupMetaReader
        .toV2Segments(meta, localStore, backupDir, collectionId = 444L)
        .getOrElse(fail("expected Right"))

      segments should have size 2
      val seg = segments.find(_.id == 777L).get
      seg.partitionId shouldBe 555L
      seg.rows shouldBe Some(3L)
      seg.storageVersion shouldBe 2
      seg.columnGroups should have size 2

      // Reconstructed backup paths (groupId=777 level present for insert_log),
      // not the source keys carried in the meta.
      val slot103 = seg.columnGroups.find(_.slotFieldId == 103L).get
      slot103.fieldIds shouldBe Seq(100L, 0L, 1L)
      slot103.filePaths shouldBe Seq(
        s"$backupDir/binlogs/insert_log/444/555/777/777/103/1"
      )
      slot103.fileRowCounts shouldBe Seq(3L)

      val slot101 = seg.columnGroups.find(_.slotFieldId == 101L).get
      slot101.fieldIds shouldBe Seq(101L)
      slot101.filePaths shouldBe Seq(
        s"$backupDir/binlogs/insert_log/444/555/777/777/101/1"
      )
      slot101.fileRowCounts shouldBe Seq(3L)

      // L0 delta logs reconstruct without the groupID level (partition == -1).
      val l0 = segments.find(_.id == 888L).get
      l0.columnGroups shouldBe empty
      l0.deltaLogs should have size 1
      l0.deltaLogs.head.logPath shouldBe
        s"$backupDir/binlogs/delta_log/444/-1/888/1"
    } finally {
      deleteRecursively(dir)
    }
  }

  test("toV2Segments materializes only the requested collection") {
    val dir = Files.createTempDirectory("milvus-backup-b1-")
    try {
      val backupDir = materializeBackup(dir)
      val base = BackupMetaReader
        .parse(
          backupFixture(
            groupAPath = SourceKeys._1,
            groupBPath = SourceKeys._2,
            groupCPath = SourceKeys._3,
            deltaPath = SourceKeys._4,
            multiFile = false
          )
        )
        .getOrElse(fail("expected Right"))
      // A second collection with no segments must never leak into the read.
      val twoColl = base.copy(
        collectionBackups = base.collectionBackups :+
          BackupMetaReader.CollectionBackup(
            collectionId = 555L,
            collectionName = "other"
          )
      )
      val segments = BackupMetaReader
        .toV2Segments(
          twoColl,
          localStore,
          backupDir,
          collectionId = 444L
        )
        .getOrElse(fail("expected Right"))
      segments.map(_.id) shouldBe Seq(777L, 888L)

      BackupMetaReader
        .toV2Segments(
          twoColl,
          localStore,
          backupDir,
          collectionId = 555L
        )
        .getOrElse(fail("expected Right")) shouldBe Seq.empty
    } finally {
      deleteRecursively(dir)
    }
  }

  test("toV2Segments skips L0 segments when applyDeletes is false") {
    val dir = Files.createTempDirectory("milvus-backup-b1-")
    try {
      val backupDir = materializeBackup(dir)
      val meta = BackupMetaReader
        .parse(
          backupFixture(
            groupAPath = SourceKeys._1,
            groupBPath = SourceKeys._2,
            groupCPath = SourceKeys._3,
            deltaPath = SourceKeys._4,
            multiFile = false
          )
        )
        .getOrElse(fail("expected Right"))
      val segments = BackupMetaReader
        .toV2Segments(
          meta,
          localStore,
          backupDir,
          applyDeletes = false,
          collectionId = 444L
        )
        .getOrElse(fail("expected Right"))
      segments.map(_.id) shouldBe Seq(777L)
    } finally {
      deleteRecursively(dir)
    }
  }

  test(
    "toV2Segments recovers per-file row counts for multi-file column groups"
  ) {
    val dir = Files.createTempDirectory("milvus-backup-multi-")
    try {
      val backupDir = materializeMultiFileBackup(dir)
      val meta = BackupMetaReader
        .parse(
          backupFixture(
            groupAPath = SourceKeys._1,
            groupBPath = SourceKeys._2,
            groupCPath = SourceKeys._3,
            deltaPath = SourceKeys._4,
            multiFile = true
          )
        )
        .getOrElse(fail("expected Right"))

      val segments = BackupMetaReader
        .toV2Segments(meta, localStore, backupDir, collectionId = 444L)
        .getOrElse(fail("expected Right"))

      val seg = segments.find(_.id == 777L).get
      seg.rows shouldBe Some(3L)
      // Slot 103 spans two binlog files; per-file row counts must be recovered
      // (not group-cumulative), otherwise the packed reader silently truncates
      // the second file.
      val slot103 = seg.columnGroups.find(_.slotFieldId == 103L).get
      slot103.filePaths shouldBe Seq(
        s"$backupDir/binlogs/insert_log/444/555/777/777/103/1",
        s"$backupDir/binlogs/insert_log/444/555/777/777/103/2"
      )
      slot103.fileRowCounts shouldBe Seq(2L, 1L)

      val slot101 = seg.columnGroups.find(_.slotFieldId == 101L).get
      slot101.filePaths shouldBe Seq(
        s"$backupDir/binlogs/insert_log/444/555/777/777/101/1"
      )
      slot101.fileRowCounts shouldBe Seq(3L)
    } finally {
      deleteRecursively(dir)
    }
  }

  test(
    "buildV2Segment fails hard for non-L0 non-StorageV2 and keeps L0 without a storage version"
  ) {
    val conf = new org.apache.hadoop.conf.Configuration()

    // StorageV3 data segment -> Left.
    val v3 = BackupMetaReader.SegmentBackup(segmentId = 1L, storageVersion = 3L)
    BackupMetaReader
      .buildV2SegmentWithStore(
        v3,
        localStore,
        "/tmp/backup",
        applyDeletes = true
      )
      .left
      .toOption
      .get
      .getMessage should include("storage_version")

    // StorageV1 data segment -> fails hard too (partial dataset is worse than
    // an error).
    val v1 = BackupMetaReader.SegmentBackup(segmentId = 2L, storageVersion = 0L)
    BackupMetaReader
      .buildV2SegmentWithStore(
        v1,
        localStore,
        "/tmp/backup",
        applyDeletes = true
      )
      .isLeft shouldBe true

    // L0 delete-only segment with storage_version 0/omitted (how Milvus 2.6
    // creates them) must NOT fall into the StorageV1 skip path.
    val l0NoVersion = BackupMetaReader.SegmentBackup(
      segmentId = 3L,
      collectionId = 444L,
      partitionId = -1L,
      isL0 = true,
      storageVersion = 0L,
      deltalogs = Seq(
        BackupMetaReader.FieldBinlog(
          fieldId = 0L,
          binlogs = Seq(
            BackupMetaReader.Binlog(
              logId = 1L,
              logPath = "files/delta_log/444/-1/3/1",
              logSize = 10L
            )
          )
        )
      )
    )
    BackupMetaReader.buildV2SegmentWithStore(
      l0NoVersion,
      localStore,
      "/tmp/backup",
      applyDeletes = true
    ) match {
      case Right(Some(seg)) =>
        seg.columnGroups shouldBe empty
        seg.deltaLogs.map(_.logPath) shouldBe Seq(
          "/tmp/backup/binlogs/delta_log/444/-1/3/1"
        )
      case other => fail(s"expected Right(Some), got $other")
    }
    // With applyDeletes=false the L0 segment is skipped entirely.
    BackupMetaReader.buildV2SegmentWithStore(
      l0NoVersion,
      localStore,
      "/tmp/backup",
      applyDeletes = false
    ) shouldBe Right(None)
  }

  test("toV2Segments rejects snapshot-format backups") {
    val meta = BackupMetaReader
      .parse(
        backupFixture(
          format = "snapshot",
          groupAPath = SourceKeys._1,
          groupBPath = SourceKeys._2,
          groupCPath = SourceKeys._3,
          deltaPath = SourceKeys._4
        )
      )
      .getOrElse(fail("expected Right"))
    val result = BackupMetaReader.toV2Segments(
      meta,
      localStore,
      "s3a://bucket/backup/b1",
      collectionId = 444L
    )
    result.isLeft shouldBe true
    result.left.toOption.get.getMessage should include("snapshot format")
  }

  test("toV2Segments waits for all footer reads before the store is closed") {
    val slowReadStarted = new CountDownLatch(1)
    val releaseSlowRead = new CountDownLatch(1)
    val callReturned = new CountDownLatch(1)
    val activeReads = new AtomicInteger(0)
    val closedWhileReading = new AtomicBoolean(false)

    val store = new com.zilliz.milvus.storage.io.ObjectStore {
      override def readAll(key: String): Array[Byte] =
        throw new UnsupportedOperationException

      override def size(key: String): Long = {
        if (key.endsWith("/1/101/1")) {
          slowReadStarted.await(5L, TimeUnit.SECONDS)
          throw new java.io.IOException("first footer failed")
        }

        activeReads.incrementAndGet()
        slowReadStarted.countDown()
        try {
          var released = false
          while (!released) {
            try released = releaseSlowRead.await(5L, TimeUnit.SECONDS)
            catch {
              // A cancelled Future interrupts its worker but does not wait for
              // that worker to stop using the shared store.
              case _: InterruptedException => ()
            }
          }
          throw new java.io.IOException("second footer failed")
        } finally {
          activeReads.decrementAndGet()
        }
      }

      override def list(
          key: String,
          recursive: Boolean
      ): Seq[com.zilliz.milvus.storage.io.FileInfo] = Seq.empty

      override def exists(key: String): Boolean = false

      override def readAt(
          key: String,
          offset: Long,
          length: Long,
          fileSize: Long
      ): Array[Byte] = throw new UnsupportedOperationException

      override def write(key: String, data: Array[Byte]): Unit =
        throw new UnsupportedOperationException

      override def createDir(key: String, recursive: Boolean): Unit =
        throw new UnsupportedOperationException

      override def delete(key: String): Unit =
        throw new UnsupportedOperationException

      override def close(): Unit =
        if (activeReads.get() > 0) closedWhileReading.set(true)
    }

    def segment(id: Long): BackupMetaReader.SegmentBackup =
      BackupMetaReader.SegmentBackup(
        segmentId = id,
        collectionId = 444L,
        partitionId = 555L,
        groupId = id,
        numOfRows = 1L,
        storageVersion = 2L,
        binlogs = Seq(
          BackupMetaReader.FieldBinlog(
            fieldId = 101L,
            binlogs = Seq(BackupMetaReader.Binlog(logId = 1L))
          )
        )
      )

    val info = BackupMetaReader.BackupInfo(
      name = "concurrent-footer-failure",
      collectionBackups = Seq(
        BackupMetaReader.CollectionBackup(
          collectionId = 444L,
          partitionBackups = Seq(
            BackupMetaReader.PartitionBackup(
              partitionId = 555L,
              segmentBackups = Seq(segment(1L), segment(2L))
            )
          )
        )
      )
    )

    var result: Either[Throwable, Seq[Segment]] = null
    val caller = new Thread(() => {
      try {
        result = BackupMetaReader.toV2Segments(
          info,
          store,
          "backup",
          collectionId = 444L
        )
      } finally {
        store.close()
        callReturned.countDown()
      }
    })
    caller.start()

    slowReadStarted.await(5L, TimeUnit.SECONDS) shouldBe true
    try {
      callReturned.await(200L, TimeUnit.MILLISECONDS) shouldBe false
    } finally {
      releaseSlowRead.countDown()
    }
    callReturned.await(5L, TimeUnit.SECONDS) shouldBe true
    caller.join()

    result.isLeft shouldBe true
    closedWhileReading.get() shouldBe false
  }

  test(
    "toProtobufSchemaBytes rejects dynamic collections without a $meta record"
  ) {
    import com.fasterxml.jackson.databind.node.IntNode
    val base = BackupMetaReader.BackupCollectionSchema(
      name = "demo",
      enableDynamicField = true,
      fields = Seq(
        BackupMetaReader.BackupFieldSchema(
          fieldId = 100L,
          name = "id",
          rawDataType = Some(IntNode.valueOf(5))
        )
      )
    )
    val err = intercept[IllegalArgumentException] {
      BackupMetaReader.toProtobufSchemaBytes(base)
    }
    err.getMessage should include("--backup_index_extra")

    // With the $meta field recorded, conversion succeeds.
    val withMeta = base.copy(
      fields = base.fields :+ BackupMetaReader.BackupFieldSchema(
        fieldId = 101L,
        name = "$meta",
        rawDataType = Some(IntNode.valueOf(23)),
        isDynamic = true,
        nullable = true
      )
    )
    BackupMetaReader.toProtobufSchemaBytes(withMeta).nonEmpty shouldBe true
  }

  test(
    "toProtobufSchemaBytes rejects collections with struct-array fields"
  ) {
    import com.fasterxml.jackson.databind.node.IntNode
    val arrayNode = com.fasterxml.jackson.databind.node.JsonNodeFactory.instance
      .arrayNode()
    arrayNode.addObject()
    val schema = BackupMetaReader.BackupCollectionSchema(
      name = "demo",
      fields = Seq(
        BackupMetaReader.BackupFieldSchema(
          fieldId = 100L,
          name = "id",
          rawDataType = Some(IntNode.valueOf(5))
        )
      ),
      structArrayFields = Some(arrayNode)
    )
    val err = intercept[IllegalArgumentException] {
      BackupMetaReader.toProtobufSchemaBytes(schema)
    }
    err.getMessage should include("struct-array")

    // Empty / absent struct_array_fields are fine.
    BackupMetaReader
      .toProtobufSchemaBytes(schema.copy(structArrayFields = None))
      .nonEmpty shouldBe true
  }

  test(
    "buildV2Segment fails hard when a non-empty StorageV2 segment has no binlogs"
  ) {
    val conf = new org.apache.hadoop.conf.Configuration()
    val seg = BackupMetaReader.SegmentBackup(
      segmentId = 9L,
      storageVersion = 2L,
      numOfRows = 50000L,
      binlogs = Seq.empty
    )
    val err = BackupMetaReader
      .buildV2SegmentWithStore(
        seg,
        localStore,
        "/tmp/backup",
        applyDeletes = true
      )
      .left
      .toOption
      .get
    err.getMessage should include("refusing to silently drop rows")

    // A genuinely empty segment (no rows, no binlogs) stays a soft empty.
    val zeroRowSeg = seg.copy(numOfRows = 0L)
    BackupMetaReader.buildV2SegmentWithStore(
      zeroRowSeg,
      localStore,
      "/tmp/backup",
      applyDeletes = true
    ) match {
      case Right(Some(v2)) => v2.columnGroups shouldBe Seq.empty
      case other           => fail(s"expected Right(Some), got $other")
    }
  }

  test(
    "buildV2Segment rejects column groups with inconsistent row totals"
  ) {
    val dir = Files.createTempDirectory("milvus-backup-inconsistent-")
    try {
      val base =
        Paths.get(
          dir.toString,
          "binlogs",
          "insert_log",
          "444",
          "555",
          "999",
          "777"
        )
      writeParquetAt(
        Paths.get(base.toString, "103", "1"),
        groupASchema,
        List(
          f =>
            f.newGroup()
              .append("pk", 1L)
              .append("row_id", 10L)
              .append("ts", 100L),
          f =>
            f.newGroup()
              .append("pk", 2L)
              .append("row_id", 11L)
              .append("ts", 101L)
        )
      )
      writeParquetAt(
        Paths.get(base.toString, "101", "1"),
        groupCSchema,
        List(
          f => f.newGroup().append("name", "a"),
          f => f.newGroup().append("name", "b"),
          f => f.newGroup().append("name", "c")
        )
      )
      val seg = BackupMetaReader.SegmentBackup(
        segmentId = 777L,
        collectionId = 444L,
        partitionId = 555L,
        groupId = 999L,
        numOfRows = 5L,
        storageVersion = 2L,
        binlogs = Seq(
          BackupMetaReader.FieldBinlog(
            fieldId = 103L,
            binlogs = Seq(
              BackupMetaReader.Binlog(
                logId = 1L,
                logPath = "files/insert_log/444/555/777/103/1",
                logSize = 10L
              )
            )
          ),
          BackupMetaReader.FieldBinlog(
            fieldId = 101L,
            binlogs = Seq(
              BackupMetaReader.Binlog(
                logId = 1L,
                logPath = "files/insert_log/444/555/777/101/1",
                logSize = 10L
              )
            )
          )
        )
      )
      val err = BackupMetaReader
        .buildV2SegmentWithStore(
          seg,
          localStore,
          dir.toString,
          applyDeletes = true
        )
        .left
        .toOption
        .get
      err.getMessage should include("inconsistent row totals")
    } finally {
      deleteRecursively(dir)
    }
  }

  test(
    "buildV2Segment rejects when footer rows disagree with num_of_rows"
  ) {
    val dir = Files.createTempDirectory("milvus-backup-rowmismatch-")
    try {
      val base =
        Paths.get(
          dir.toString,
          "binlogs",
          "insert_log",
          "444",
          "555",
          "999",
          "777"
        )
      writeParquetAt(
        Paths.get(base.toString, "103", "1"),
        groupASchema,
        List(
          f =>
            f.newGroup()
              .append("pk", 1L)
              .append("row_id", 10L)
              .append("ts", 100L),
          f =>
            f.newGroup()
              .append("pk", 2L)
              .append("row_id", 11L)
              .append("ts", 101L)
        )
      )
      val seg = BackupMetaReader.SegmentBackup(
        segmentId = 777L,
        collectionId = 444L,
        partitionId = 555L,
        groupId = 999L,
        numOfRows = 3L, // meta claims 3, footer has 2
        storageVersion = 2L,
        binlogs = Seq(
          BackupMetaReader.FieldBinlog(
            fieldId = 103L,
            binlogs = Seq(
              BackupMetaReader.Binlog(
                logId = 1L,
                logPath = "files/insert_log/444/555/777/103/1",
                logSize = 10L
              )
            )
          )
        )
      )
      val err = BackupMetaReader
        .buildV2SegmentWithStore(
          seg,
          localStore,
          dir.toString,
          applyDeletes = true
        )
        .left
        .toOption
        .get
      err.getMessage should include("meta and data disagree")
    } finally {
      deleteRecursively(dir)
    }
  }

  test("L1 segment delta logs reconstruct with the groupID level") {
    val dir = Files.createTempDirectory("milvus-backup-l1-")
    try {
      // L1 (partition != -1) delta logs carry the groupID level; a regression
      // here (empty deltaLogs or a mis-ordered delta path) would silently let
      // deleted rows come back because loadV2DeletePlans filters on
      // columnGroups.nonEmpty && deltaLogs.nonEmpty.
      val segDir =
        Paths.get(
          dir.toString,
          "binlogs",
          "insert_log",
          "444",
          "555",
          "999",
          "777"
        )
      writeParquetAt(
        Paths.get(segDir.toString, "103", "1"),
        groupASchema,
        List(f =>
          f.newGroup().append("pk", 1L).append("row_id", 10L).append("ts", 100L)
        )
      )
      val seg = BackupMetaReader.SegmentBackup(
        segmentId = 777L,
        collectionId = 444L,
        partitionId = 555L,
        groupId = 999L,
        numOfRows = 1L,
        storageVersion = 2L,
        binlogs = Seq(
          BackupMetaReader.FieldBinlog(
            fieldId = 103L,
            binlogs = Seq(
              BackupMetaReader.Binlog(
                logId = 1L,
                logPath = "files/insert_log/444/555/777/103/1",
                logSize = 10L
              )
            )
          )
        ),
        deltalogs = Seq(
          BackupMetaReader.FieldBinlog(
            fieldId = 0L,
            binlogs = Seq(
              BackupMetaReader.Binlog(
                logId = 5L,
                logPath = "files/delta_log/444/555/777/5",
                logSize = 10L
              )
            )
          )
        )
      )
      BackupMetaReader.buildV2SegmentWithStore(
        seg,
        localStore,
        dir.toString,
        applyDeletes = true
      ) match {
        case Right(Some(v2)) =>
          v2.deltaLogs.map(_.logId) shouldBe Seq(5L)
          v2.deltaLogs.map(_.logPath) shouldBe Seq(
            s"${dir.toString}/binlogs/delta_log/444/555/999/777/5"
          )
        case other => fail(s"expected Right(Some), got $other")
      }
    } finally {
      deleteRecursively(dir)
    }
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
