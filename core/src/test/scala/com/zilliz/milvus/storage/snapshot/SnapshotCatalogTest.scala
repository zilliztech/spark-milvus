package com.zilliz.milvus.storage.snapshot

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import java.util.ArrayList

import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.{GenericData, GenericDatumWriter, GenericRecord}
import org.apache.avro.Schema
import org.scalatest.funsuite.AnyFunSuite

import com.zilliz.milvus.storage.io.{LocalObjectStore, ObjectStore}
import com.zilliz.milvus.storage.manifest.{
  SnapshotSegmentEntry,
  SnapshotSegmentFixture
}
import com.zilliz.milvus.storage.read.plan.{DeleteFileListing, ReadPlan}
import com.zilliz.milvus.storage.snapshot.json.{
  CollectionJson,
  CollectionSchemaJson,
  ManifestItemJson,
  SnapshotInfoJson,
  SnapshotJson
}
import io.milvus.grpc.schema.{
  CollectionSchema => ProtoSchema,
  DataType,
  FieldSchema
}

/** R2, R3: the snapshot directory is the read entry point, and the snapshot
  * JSON's shape becomes the model.
  */
class SnapshotCatalogTest extends AnyFunSuite {

  private def snapshotJson(
      name: String,
      createTs: Long,
      v3: Seq[(Long, String)] = Seq((30L, "files/insert_log/10/20/30"))
  ): String = {
    val items = v3
      .map { case (id, base) =>
        s"""{"segmentID": $id, "manifest": "{\\"ver\\":7,\\"base_path\\":\\"$base\\"}"}"""
      }
      .mkString(",")
    s"""{
      "snapshot_info": {"name": "$name", "id": 1, "collection_id": 10, "partition_ids": [20], "create_ts": $createTs},
      "collection": {"schema": {"name": "c", "fields": [
        {"fieldID": 100, "name": "id", "data_type": "Int64", "is_primary_key": true},
        {"fieldID": 101, "name": "v", "data_type": "FloatVector", "type_params": [{"key": "dim", "value": "4"}]}
      ]}},
      "format_version": 4,
      "indexes": [],
      "manifest_list": [],
      "storagev2_manifest_list": [$items]
    }"""
  }

  private def withDir(f: Path => Unit): Unit = {
    val dir = Files.createTempDirectory("snapshot-catalog")
    try f(dir)
    finally {
      Files
        .walk(dir)
        .sorted(java.util.Comparator.reverseOrder[Path]())
        .forEach(p => Files.delete(p))
    }
  }

  private def write(dir: Path, key: String, content: String): Unit = {
    val p = dir.resolve(key)
    Files.createDirectories(p.getParent)
    Files.write(p, content.getBytes(StandardCharsets.UTF_8))
  }

  private def catalog(dir: Path) =
    new SnapshotCatalog(
      new LocalObjectStore(dir.toString),
      bucket = ""
    )

  private val avroKey = "files/snapshots/10/manifests/1/30.avro"

  test(
    "non-V3 storage versions are rejected before planning"
  ) {
    withDir { dir =>
      Files.createDirectories(dir.resolve(avroKey).getParent)
      for (version <- Seq(0L, 1L, 2L, 4L, -1L)) {
        Files.write(
          dir.resolve(avroKey),
          SnapshotSegmentFixture.encode(storageVersion = version)
        )
        val metadata = SnapshotJson
          .parse(
            snapshotJson("unknown", 100L, Seq.empty)
              .replace(
                "\"manifest_list\": []",
                "\"manifest_list\": [\"" + avroKey + "\"]"
              )
          )
          .fold(error => throw new IllegalArgumentException(error), identity)
        val failure = SnapshotCatalog
          .fromMetadata(
            metadata,
            SnapshotOrigin.Options,
            new LocalObjectStore(dir.toString),
            ""
          )
          .left
          .toOption
          .get
        assert(failure.getMessage.contains(s"30=$version"))
        assert(
          failure.getMessage.contains(
            "public snapshot reads require Storage V3"
          )
        )
      }
    }
  }

  test(
    "a V3 segment record without its physical manifest location fails closed"
  ) {
    withDir { dir =>
      writeAvro(dir, avroKey, SnapshotSegmentFixture.encode(version = 4))
      val metadata = SnapshotJson
        .parse(
          snapshotJson("unlocated", 100L, Seq.empty).replace(
            "\"manifest_list\": []",
            "\"manifest_list\": [\"" + avroKey + "\"]"
          )
        )
        .fold(throw _, identity)
      val error = SnapshotCatalog
        .fromMetadata(
          metadata,
          SnapshotOrigin.Options,
          new LocalObjectStore(dir.toString),
          ""
        )
        .left
        .toOption
        .get
      assert(error.getMessage.contains("no V3 manifest location"))
      assert(error.getMessage.contains("30"))
    }
  }

  private def indexedSnapshotJson: String = snapshotJson("indexed", 100L)
    .replace(
      "\"manifest_list\": []",
      "\"manifest_list\": [\"" + avroKey + "\"]"
    )
    .replace(
      "\"indexes\": []",
      """"indexes": [{"collectionID":"10","fieldID":"101","indexID":"469076449917763071",
        "index_name":"v_hnsw","type_params":[{"key":"dim","value":"4"}],
        "index_params":[{"key":"index_type","value":"HNSW"},{"key":"M","value":"30"}],
        "user_index_params":[{"key":"M","value":"16"}]}],
      "build_ids":["469076449917967340"]"""
    )

  private def writeAvro(dir: Path, key: String, bytes: Array[Byte]): Unit = {
    val path = dir.resolve(key)
    Files.createDirectories(path.getParent)
    Files.write(path, bytes)
  }

  private def writeSingleSegmentSnapshot(
      dir: Path,
      key: String,
      json: String
  ): Unit = {
    writeAvro(dir, avroKey, SnapshotSegmentFixture.encode())
    write(
      dir,
      key,
      json.replace(
        "\"manifest_list\": []",
        "\"manifest_list\": [\"" + avroKey + "\"]"
      )
    )
  }

  test("snapshot index definitions and exact segment builds reach read tasks") {
    withDir { dir =>
      val index = SnapshotSegmentFixture.index()
      writeAvro(
        dir,
        avroKey,
        SnapshotSegmentFixture.encode(indexes =
          Vector(
            index.copy(filePaths = index.filePaths.map("s3a://bucket/" + _))
          )
        )
      )
      write(dir, "snapshot.json", indexedSnapshotJson)
      val snapshot = new SnapshotCatalog(
        new LocalObjectStore(dir.toString),
        "bucket"
      )
        .read("snapshot.json")
      val definition = snapshot.indexes.get.head
      assert(definition.indexId == 469076449917763071L)
      assert(definition.indexParameters("M") == "30")
      assert(definition.userIndexParameters("M") == "16")
      assert(snapshot.buildIds.contains(Vector(469076449917967340L)))
      val segment = snapshot.segments.head
      assert(segment.rows.contains(2L))
      val actual =
        segment.indexes.asInstanceOf[SegmentIndexes.Available].indexes.head
      assert(
        actual.collectionId == 10L && actual.partitionId == 20L && actual.segmentId == 30L
      )
      assert(actual.filePaths == index.filePaths)
      assert(actual.parameters == index.parameters)
      assert(actual.indexVersion == 1L)
      assert(actual.currentIndexVersion.contains(10))
      assert(actual.indexStorePathVersion.contains(0))
      val plan = ReadPlan.of(
        snapshot,
        _ => Map.empty,
        applyDeletes = false,
        neededFieldIds = Seq(100L)
      )
      assert(plan.specs.head.indexes == segment.indexes)
      assert(plan.specs.head.readVersionOrLatest == 7L)
      assert(plan.specs.head.snapshotRows.contains(2L))
      assert(plan.specs.head.neededFieldIds == Seq(100L))
      assert(plan.totalRows.contains(2L))
    }
  }

  test("snapshot Avro and index paths honor the configured Milvus endpoint") {
    withDir { dir =>
      val endpoint = "minio:9000"
      val prefix = s"s3://$endpoint/bucket/"
      val index = SnapshotSegmentFixture.index()
      writeAvro(
        dir,
        avroKey,
        SnapshotSegmentFixture.encode(indexes =
          Vector(index.copy(filePaths = index.filePaths.map(prefix + _)))
        )
      )
      write(
        dir,
        "snapshot.json",
        indexedSnapshotJson.replace(avroKey, prefix + avroKey)
      )
      val snapshot = new SnapshotCatalog(
        new LocalObjectStore(dir.toString),
        "bucket",
        endpoint = endpoint
      ).read("snapshot.json")
      val loaded = snapshot.segments.head.indexes
        .asInstanceOf[SegmentIndexes.Available]
        .indexes
        .head
      assert(loaded.filePaths == index.filePaths)
    }
  }

  test(
    "V3 L0 roles survive materialization and narrowing"
  ) {
    withDir { dir =>
      val l0Key = "files/snapshots/10/manifests/1/41.avro"
      writeAvro(
        dir,
        avroKey,
        SnapshotSegmentFixture.encode(
          channelName = "channel-a"
        )
      )
      writeAvro(
        dir,
        l0Key,
        SnapshotSegmentFixture.encode(
          segmentId = 41L,
          partitionId = -1L,
          rows = 0L,
          segmentLevel = 1L,
          channelName = "channel-a"
        )
      )
      write(
        dir,
        "snapshot.json",
        snapshotJson(
          "with-l0",
          100L,
          Seq(
            30L -> "files/insert_log/10/20/30",
            41L -> "files/insert_log/10/-1/41"
          )
        ).replace(
          "\"manifest_list\": []",
          s""""manifest_list": ["$avroKey","$l0Key"]"""
        )
      )
      val snapshot = catalog(dir).read("snapshot.json")
      assert(snapshot.dataSegments.map(_.id) == Seq(30L))
      assert(snapshot.deleteOnlySegments.map(_.id) == Seq(41L))
      assert(snapshot.deleteOnlySegments.head.partitionId == -1L)
      assert(snapshot.dataSegments.head.channelName == "channel-a")
      val narrowed = snapshot.narrow(Seq.empty, Seq(30L))
      assert(narrowed.segments.map(_.id) == Seq(30L, 41L))
      assert(
        snapshot.retainDataSegments(Set(30L)).segments.map(_.id) == Seq(
          30L,
          41L
        )
      )
      val plan = ReadPlan.of(narrowed, _ => Map.empty, applyDeletes = false)
      assert(plan.specs.map(_.segmentId) == Seq(30L))
    }
  }

  test(
    "legacy L0 Avro entries are rejected by the V3 reader"
  ) {
    withDir { dir =>
      val key = "files/snapshots/10/manifests/1/41.avro"
      writeAvro(
        dir,
        key,
        SnapshotSegmentFixture.encode(
          version = 4,
          segmentId = 41L,
          partitionId = -1L,
          rows = 0L,
          storageVersion = 0L,
          segmentLevel = 1L
        )
      )
      write(
        dir,
        "snapshot.json",
        snapshotJson("with-l0", 100L).replace(
          "\"manifest_list\": []",
          "\"manifest_list\": [\"" + key + "\"]"
        )
      )
      val missing =
        intercept[IllegalArgumentException](catalog(dir).read("snapshot.json"))
      assert(
        missing.getMessage.contains("public snapshot reads require Storage V3")
      )

    }
  }

  test(
    "Avro partitions govern L0 inheritance regardless of manifest directory layout"
  ) {
    withDir { dir =>
      val sources = Seq(
        (30L, 21L, "files/insert_log/10/20/30", false),
        (41L, -1L, "archive/l0/41", true),
        (42L, 21L, "archive/l0/42", true)
      )
      val manifestSchema = new Schema.Parser().parse("""
        {"type":"record","name":"Manifest","fields":[
          {"name":"delta_logs","type":{"type":"array","items":{
            "type":"record","name":"DeltaLog","fields":[
              {"name":"path","type":"string"},{"name":"type","type":"int"},
              {"name":"num_entries","type":"long"}]}}}]}
      """)
      val paths = sources.map { case (id, partition, base, l0) =>
        val key = s"files/snapshots/10/manifests/1/$id.avro"
        writeAvro(
          dir,
          key,
          SnapshotSegmentFixture.encode(
            segmentId = id,
            partitionId = partition,
            rows = if (l0) 0L else 2L,
            segmentLevel = if (l0) 1L else 2L,
            channelName = "source"
          )
        )
        val record = new GenericData.Record(manifestSchema)
        val logs = new ArrayList[GenericRecord]()
        if (l0) {
          val log = new GenericData.Record(
            manifestSchema.getField("delta_logs").schema().getElementType
          )
          log.put("path", s"delete-$id")
          log.put("type", 0)
          log.put("num_entries", 1L)
          logs.add(log)
        }
        record.put("delta_logs", logs)
        val file = dir.resolve(s"$base/_metadata/manifest-7.avro")
        Files.createDirectories(file.getParent)
        val writer = new DataFileWriter[GenericRecord](
          new GenericDatumWriter[GenericRecord]()
        )
        writer.create(manifestSchema, file.toFile)
        try writer.append(record)
        finally writer.close()
        key
      }
      val metadata = SnapshotJson
        .parse(
          snapshotJson(
            "relocated",
            100L,
            sources.map { case (id, _, base, _) => id -> base }
          ).replace("\"partition_ids\": [20]", "\"partition_ids\": [21]")
        )
        .fold(throw _, identity)
        .copy(manifestList = paths)
      val store = new LocalObjectStore(dir.toString)
      val snapshot = SnapshotCatalog
        .fromMetadata(
          metadata,
          SnapshotOrigin.Catalog("snapshot.json"),
          store,
          ""
        )
        .fold(throw _, identity)
      assert(
        snapshot.segments.map(s => s.id -> s.partitionId).toMap == Map(
          30L -> 21L,
          41L -> -1L,
          42L -> 21L
        )
      )
      val narrowed = snapshot.narrow(Seq(21L), Seq(30L))
      assert(narrowed.segments.map(_.id) == Seq(30L, 41L, 42L))
      val deletes = DeleteFileListing
        .of(narrowed, true, "", store)
        .fold(throw _, identity)
      assert(
        deletes.filesFor(30L).map(_.logPath).toSet == Set(
          "archive/l0/41/_delta/delete-41",
          "archive/l0/42/_delta/delete-42"
        )
      )
    }
  }

  test("complete snapshots require an Avro record for every V3 location") {
    withDir { dir =>
      Seq(4, 6).foreach { version =>
        write(
          dir,
          "snapshot.json",
          snapshotJson("plain", 100L)
            .replace("\"format_version\": 4", s""""format_version": $version""")
        )
        val error = intercept[IllegalArgumentException](
          catalog(dir).read("snapshot.json")
        )
        assert(
          error.getMessage.contains("no snapshot segment Avro records: 30")
        )
      }
    }
  }

  test("an empty Avro index list is unindexed") {
    withDir { dir =>
      writeAvro(dir, avroKey, SnapshotSegmentFixture.encode())
      write(dir, "snapshot.json", indexedSnapshotJson)
      val snapshot = catalog(dir).read("snapshot.json")
      assert(snapshot.segments.head.indexes == SegmentIndexes.Unindexed)
    }
  }

  test("segment_ids and Avro records must describe the same unique segments") {
    withDir { dir =>
      val secondKey = "files/snapshots/10/manifests/1/31.avro"
      writeAvro(dir, avroKey, SnapshotSegmentFixture.encode())
      writeAvro(dir, secondKey, SnapshotSegmentFixture.encode(segmentId = 31L))
      val json = snapshotJson(
        "declared",
        100L,
        Seq(
          30L -> "files/insert_log/10/20/30",
          31L -> "files/insert_log/10/20/31"
        )
      )
      def metadata(ids: String) = SnapshotJson
        .parse(
          json.replace(
            "\"indexes\": []",
            s""""indexes": [], "segment_ids": $ids"""
          )
        )
        .fold(throw _, identity)
        .copy(manifestList = Seq(avroKey, secondKey))
      def resolve(value: SnapshotJson) = SnapshotCatalog.fromMetadata(
        value,
        SnapshotOrigin.Catalog("snapshot.json"),
        new LocalObjectStore(dir.toString),
        ""
      )
      Seq("[]", "[30,31]", "[31,30]").foreach { ids =>
        assert(resolve(metadata(ids)).isRight, ids)
      }
      Seq(
        "[30,30,31]" -> "duplicate segment ids",
        "[30]" -> "unexpected [31]",
        "[30,31,32]" -> "missing [32]"
      ).foreach { case (ids, message) =>
        val error = resolve(metadata(ids)).swap.toOption.get
        assert(error.getMessage.contains(message), error.getMessage)
      }
      val duplicate = resolve(
        metadata("[]").copy(manifestList = Seq(avroKey, avroKey, secondKey))
      ).swap.toOption.get
      assert(duplicate.getMessage.contains("duplicate segment Avro records"))
    }
  }

  test(
    "inconsistent index identity, row counts, builds and buckets fail snapshot resolution"
  ) {
    withDir { dir =>
      val index = SnapshotSegmentFixture.index()
      val invalid = Seq(
        index.copy(segmentId = 31L) -> "expected 30",
        index.copy(rowCount = 3L) -> "has 3 rows",
        index.copy(buildId = 123L) -> "build_ids",
        index.copy(indexId = 123L) -> "absent from snapshot indexes",
        index.copy(filePaths =
          Vector("s3://other/index.bin")
        ) -> "bound to 'bucket'"
      )
      write(dir, "snapshot.json", indexedSnapshotJson)
      invalid.foreach { case (badIndex, expected) =>
        writeAvro(
          dir,
          avroKey,
          SnapshotSegmentFixture.encode(indexes = Vector(badIndex))
        )
        val error = intercept[IllegalArgumentException] {
          new SnapshotCatalog(
            new LocalObjectStore(dir.toString),
            "bucket"
          )
            .read("snapshot.json")
        }
        assert(error.getMessage.contains(expected), error.getMessage)
      }
    }
  }

  test(
    "reads a V3 snapshot with segment metadata and a pinned version"
  ) {
    withDir { dir =>
      writeSingleSegmentSnapshot(
        dir,
        "files/snapshots/10/metadata/1.json",
        snapshotJson("s1", 100L)
      )
      val snapshot = catalog(dir).read("files/snapshots/10/metadata/1.json")
      assert(snapshot.name == "s1")
      assert(snapshot.collectionId == 10L)
      assert(snapshot.createdAt.contains(100L))
      assert(snapshot.partitionIds == Seq(20L))
      assert(snapshot.primaryKeyField.map(_.name).contains("id"))
      assert(
        snapshot.origin == SnapshotOrigin.Catalog(
          "files/snapshots/10/metadata/1.json"
        )
      )
      val seg = snapshot.segments.head
      assert(seg.id == 30L)
      assert(seg.partitionId == 20L)
      assert(seg.storageVersion == 3)
      assert(seg.rows.contains(2L))
      assert(
        seg.layout == SegmentLayout.Manifest("files/insert_log/10/20/30", 7L)
      )
      assert(seg.deletes == DeleteFiles.InManifest)
      assert(seg.hasData)
    }
  }

  test(
    "segment Avro supplies the partition for a nonstandard V3 path"
  ) {
    withDir { dir =>
      writeSingleSegmentSnapshot(
        dir,
        "files/snapshots/10/metadata/1.json",
        snapshotJson("s1", 100L, Seq((30L, "files/unexpected/10/20/30")))
      )
      val seg =
        catalog(dir).read("files/snapshots/10/metadata/1.json").segments.head
      assert(seg.partitionId == 20L)
    }
  }

  // An older snapshot whose V2 files are gone must not stop the newest one, or
  // a named one, from being read.

  test("snapshot materialization rejects unsafe segment ids") {
    withDir { dir =>
      write(
        dir,
        "files/snapshots/10/metadata/1.json",
        snapshotJson(
          "invalid",
          100L,
          Seq((-1L, "files/insert_log/10/20/-1"))
        )
      )
      val invalid = intercept[IllegalArgumentException](
        catalog(dir).read("files/snapshots/10/metadata/1.json")
      )
      assert(invalid.getMessage.contains("non-positive segment id(s): -1"))

      write(
        dir,
        "files/snapshots/10/metadata/0.json",
        snapshotJson(
          "zero",
          100L,
          Seq((0L, "files/insert_log/10/20/not-a-segment-id"))
        )
      )
      val zero = intercept[IllegalArgumentException](
        catalog(dir).read("files/snapshots/10/metadata/0.json")
      )
      assert(zero.getMessage.contains("non-positive segment id(s): 0"))

      write(
        dir,
        "files/snapshots/10/metadata/3.json",
        snapshotJson(
          "same-line-duplicate",
          100L,
          Seq(
            (30L, "files/insert_log/10/20/30"),
            (30L, "files/insert_log/10/20/31")
          )
        )
      )
      val sameLineDuplicate = intercept[IllegalArgumentException](
        catalog(dir).read("files/snapshots/10/metadata/3.json")
      )
      assert(
        sameLineDuplicate.getMessage.contains("duplicate segment id(s): 30")
      )

    }
  }

  test("a snapshot in another bucket than the catalog's is refused") {
    withDir { dir =>
      writeSingleSegmentSnapshot(
        dir,
        "files/snapshots/10/metadata/1.json",
        snapshotJson("s1", 100L)
      )
      val c = new SnapshotCatalog(
        new LocalObjectStore(dir.toString),
        "a"
      )
      val err = intercept[IllegalArgumentException](
        c.read("s3a://b/files/snapshots/10/metadata/1.json")
      )
      assert(err.getMessage.contains("bucket 'b'"))
      assert(c.read("s3a://a/files/snapshots/10/metadata/1.json").name == "s1")
    }
  }

  // A catalog over a store with no bucket (the local backend) used to skip
  // the bucket check and read the location's key from the local root.
  test("a store with no bucket refuses a location that names one") {
    withDir { dir =>
      writeSingleSegmentSnapshot(
        dir,
        "files/snapshots/10/metadata/1.json",
        snapshotJson("s1", 100L)
      )
      val c = new SnapshotCatalog(
        new LocalObjectStore(dir.toString),
        ""
      )
      Seq(
        "gs://b/files/snapshots/10/metadata/1.json",
        "https://s3.us-west-2.amazonaws.com/b/files/snapshots/10/metadata/1.json"
      ).foreach { location =>
        intercept[IllegalArgumentException](c.read(location))
      }
      assert(c.read("files/snapshots/10/metadata/1.json").name == "s1")
    }
  }

  test(
    "the endpoint form Milvus prints is read when the host is the catalog's"
  ) {
    withDir { dir =>
      writeSingleSegmentSnapshot(
        dir,
        "files/snapshots/10/metadata/1.json",
        snapshotJson("s1", 100L)
      )
      val c = new SnapshotCatalog(
        new LocalObjectStore(dir.toString),
        "a",
        endpoint = "s3.us-west-2.amazonaws.com"
      )
      // CreateSnapshot's s3_location: scheme://endpoint/bucket/key.
      assert(
        c.read(
          "https://s3.us-west-2.amazonaws.com/a/files/snapshots/10/metadata/1.json"
        ).name == "s1"
      )
      // Another host is a bucket in the standard form, and a foreign one.
      val err = intercept[IllegalArgumentException](
        c.read(
          "https://s3.eu-west-1.amazonaws.com/a/files/snapshots/10/metadata/1.json"
        )
      )
      assert(err.getMessage.contains("bucket 's3.eu-west-1.amazonaws.com'"))
    }
  }

  test("minio snapshot locations preserve the catalog's bucket binding") {
    withDir { dir =>
      val key = "files/snapshots/10/metadata/1.json"
      writeSingleSegmentSnapshot(dir, key, snapshotJson("s1", 100L))
      val c = new SnapshotCatalog(
        new LocalObjectStore(dir.toString),
        "spark-demo-live",
        endpoint = "localhost:29000"
      )
      assert(
        c.read(s"minio://localhost:29000/spark-demo-live/$key").name == "s1"
      )
      val wrongBucket = intercept[IllegalArgumentException](
        c.read(s"minio://localhost:29000/other-bucket/$key")
      )
      assert(wrongBucket.getMessage.contains("bucket 'other-bucket'"))
      val noBucket = intercept[IllegalArgumentException](
        catalog(dir).read(s"minio://localhost:29000/spark-demo-live/$key")
      )
      assert(noBucket.getMessage.contains("local backend"))
    }
  }

  test("fromLists returns path-normalization failures as Left") {
    val metadata = SnapshotJson
      .parse(
        snapshotJson(
          "other-bucket",
          100L,
          Seq((30L, "s3://other/files/insert_log/10/20/30"))
        )
      )
      .toOption
      .get
    val result = SnapshotCatalog.fromLists(
      name = "other-bucket",
      collectionId = 10L,
      createdAt = Some(100L),
      partitionIds = Seq(20L),
      schemaBytes = metadata.collection.schema.toProtobufBytes,
      v3Items = metadata.storageV2ManifestList.get,
      bucket = "expected",
      origin = SnapshotOrigin.Options
    )
    assert(result.isLeft)
    assert(result.left.get.getMessage.contains("bucket 'other'"))
  }

  test(
    "V3 manifest references require fixed int64 versions and a base path"
  ) {
    val schemaBytes = SnapshotJson
      .parse(snapshotJson("fixed", 100L))
      .toOption
      .get
      .collection
      .schema
      .toProtobufBytes
    def resolve(reference: String) = SnapshotCatalog.fromLists(
      "fixed",
      10L,
      None,
      Seq(20L),
      schemaBytes,
      Seq(ManifestItemJson(30L, reference)),
      "",
      SnapshotOrigin.Options
    )
    Seq(
      "files/insert_log/10/20/30",
      """{"ver":7,"base_path":"segment"""",
      """{"ver":7}""",
      """{"ver":7,"base_path":" "}""",
      """{"ver":-1,"base_path":"segment"}""",
      """{"ver":-2,"base_path":"segment"}""",
      """{"ver":9223372036854775808,"base_path":"segment"}"""
    ).foreach { reference =>
      val failure = resolve(reference).swap.toOption.get
      assert(
        failure.getMessage.contains("manifest reference for segment 30"),
        failure.getMessage
      )
    }
    Seq(0L, Int.MaxValue.toLong + 1L, Long.MaxValue).foreach { version =>
      val snapshot =
        resolve(s"""{"ver":$version,"base_path":"files/insert_log/10/20/30"}""")
          .fold(throw _, identity)
      assert(
        snapshot.segments.head.layout == SegmentLayout.Manifest(
          "files/insert_log/10/20/30",
          version
        )
      )
    }
    val initial = resolve("""{"base_path":"files/insert_log/10/20/30"}""")
      .fold(throw _, identity)
    assert(
      initial.segments.head.layout == SegmentLayout.Manifest(
        "files/insert_log/10/20/30",
        0L
      )
    )
  }

  test("dynamic snapshots require the authoritative $meta field schema") {
    val metadata = SnapshotJson
      .parse(snapshotJson("dynamic", 100L))
      .toOption
      .get
    val baseSchema = ProtoSchema(
      name = "c",
      enableDynamicField = true,
      fields = Seq(
        FieldSchema(
          fieldID = 100L,
          name = "id",
          dataType = DataType.Int64,
          isPrimaryKey = true
        )
      )
    )

    def materialize(schema: ProtoSchema) =
      SnapshotCatalog.fromLists(
        name = "dynamic",
        collectionId = 10L,
        createdAt = Some(100L),
        partitionIds = Seq(20L),
        schemaBytes = schema.toByteArray,
        v3Items = metadata.storageV2ManifestList.get,
        bucket = "",
        origin = SnapshotOrigin.Options
      )

    val dynamicField = FieldSchema(
      fieldID = 407L,
      name = "$meta",
      dataType = DataType.JSON,
      isDynamic = true,
      nullable = true
    )
    Seq(
      Seq.empty,
      Seq(dynamicField.copy(isDynamic = false)),
      Seq(dynamicField.copy(dataType = DataType.Int64)),
      Seq(dynamicField.copy(fieldID = 1L)),
      Seq(
        dynamicField,
        dynamicField.copy(fieldID = 408L, name = "other_dynamic")
      ),
      Seq(dynamicField, dynamicField.copy(fieldID = 408L, isDynamic = false))
    ).foreach { fields =>
      val invalid = materialize(baseSchema.copy(fields = fields)).left.get
      assert(invalid.getMessage.contains("$meta"))
      assert(invalid.getMessage.contains("cannot infer that id"))
    }

    val snapshot = materialize(
      baseSchema.copy(fields = baseSchema.fields :+ dynamicField)
    ).toOption.get

    val resolved = snapshot.schema.fields.find(_.isDynamic).get
    assert(resolved.name == "$meta")
    assert(resolved.fieldID == 407L)
    assert(resolved.dataType == DataType.JSON)
  }

  test("a snapshot over the size limit is refused before parsing") {
    withDir { dir =>
      write(dir, "files/snapshots/10/metadata/1.json", snapshotJson("s1", 100L))
      val c = new SnapshotCatalog(
        new LocalObjectStore(dir.toString),
        "",
        maxJsonBytes = 10L
      )
      val err = intercept[IllegalArgumentException](
        c.read("files/snapshots/10/metadata/1.json")
      )
      assert(err.getMessage.contains("byte limit"))
    }
  }

  test("fromMetadata rejects a snapshot missing its parts") {
    def bad(m: SnapshotJson) =
      SnapshotCatalog
        .fromMetadata(
          m,
          SnapshotOrigin.Options,
          new LocalObjectStore(""),
          ""
        )
        .left
        .get
        .getMessage
    assert(
      bad(
        SnapshotJson(
          snapshotInfo = null,
          collection =
            CollectionJson(CollectionSchemaJson("c", fields = Seq.empty))
        )
      ).contains("snapshot_info")
    )
    assert(
      bad(SnapshotJson(snapshotInfo = SnapshotInfoJson("s"), collection = null))
        .contains("collection")
    )
    assert(
      bad(
        SnapshotJson(
          snapshotInfo = SnapshotInfoJson("s"),
          collection = CollectionJson(null)
        )
      ).contains("collection.schema")
    )
  }

  test("snapshot format versions are checked even without segment manifests") {
    withDir { dir =>
      val valid = SnapshotJson
        .parse(snapshotJson("empty", 100L, Seq.empty))
        .fold(throw _, identity)
      val store = new LocalObjectStore(dir.toString)
      for (
        version <- Seq(
          None,
          Some(-1),
          Some(0),
          Some(1),
          Some(2),
          Some(3),
          Some(5),
          Some(7)
        );
        manifestList <- Seq(Seq.empty, Seq("missing.avro"))
      ) {
        val error = SnapshotCatalog
          .fromMetadata(
            valid.copy(formatVersion = version, manifestList = manifestList),
            SnapshotOrigin.Options,
            store,
            ""
          )
          .swap
          .toOption
          .get
        val expected = version match {
          case Some(v) => s"Unsupported Milvus snapshot format version $v"
          case None    => "snapshot is missing format_version"
        }
        assert(error.getMessage.contains(expected))
      }
      for (version <- Seq(4, 6)) {
        val snapshot = SnapshotCatalog
          .fromMetadata(
            valid.copy(formatVersion = Some(version)),
            SnapshotOrigin.Options,
            store,
            ""
          )
          .fold(throw _, identity)
        assert(snapshot.segments.isEmpty)
      }
    }
  }

  test(
    "an empty collection snapshot retains its schema and produces no read tasks"
  ) {
    withDir { dir =>
      val explicitLists = snapshotJson("empty", 100L, Seq.empty)
      // Empty collection exports may omit both manifest lists.
      val omittedLists = explicitLists
        .replace("\"manifest_list\": [],", "")
        .replace("\"storagev2_manifest_list\": []", "")
        .replace("\"format_version\": 4", "\"format_version\": 6")
        .replace("\"indexes\": [],", "\"indexes\": []")
      Seq(explicitLists, omittedLists).foreach { json =>
        write(dir, "empty.json", json)
        val snapshot = catalog(dir).read("empty.json")
        assert(snapshot.name == "empty")
        assert(snapshot.collectionId == 10L)
        assert(snapshot.createdAt.contains(100L))
        assert(snapshot.schema.fields.map(_.name) == Seq("id", "v"))
        assert(snapshot.segments.isEmpty)
        assert(
          ReadPlan
            .of(snapshot, _ => Map.empty, applyDeletes = true)
            .specs
            .isEmpty
        )
      }
      write(
        dir,
        "incomplete.json",
        snapshotJson("incomplete", 100L, Seq.empty)
          .replace(
            "\"manifest_list\": []",
            "\"manifest_list\": [], \"segment_ids\": [30]"
          )
      )
      val error = intercept[IllegalArgumentException](
        catalog(dir).read("incomplete.json")
      )
      assert(error.getMessage.contains("declares segments"))
    }
  }

}
