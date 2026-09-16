package com.zilliz.milvus.storage.snapshot

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import org.scalatest.funsuite.AnyFunSuite

import com.zilliz.milvus.storage.io.LocalObjectStore
import com.zilliz.milvus.storage.snapshot.json.{
  CollectionJson,
  CollectionSchemaJson,
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
      bucket = "",
      V2SegmentResolver.Unavailable
    )

  test(
    "reads a V3 snapshot: segment id, partition from the path, version pinned"
  ) {
    withDir { dir =>
      write(dir, "files/snapshots/10/metadata/1.json", snapshotJson("s1", 100L))
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
      assert(seg.rows.isEmpty)
      assert(
        seg.layout == SegmentLayout.Manifest("files/insert_log/10/20/30", 7L)
      )
      assert(seg.deletes == DeleteFiles.InManifest)
      assert(seg.hasData)
    }
  }

  test(
    "a V3 path outside the insert_log layout falls back to the snapshot's first partition"
  ) {
    withDir { dir =>
      write(
        dir,
        "files/snapshots/10/metadata/1.json",
        snapshotJson("s1", 100L, Seq((30L, "files/unexpected/10/20/30")))
      )
      val seg =
        catalog(dir).read("files/snapshots/10/metadata/1.json").segments.head
      assert(seg.partitionId == 20L)
    }
  }

  test("latest, by name and as-of pick among the collection's snapshots") {
    withDir { dir =>
      write(dir, "files/snapshots/10/metadata/1.json", snapshotJson("s1", 100L))
      write(dir, "files/snapshots/10/metadata/2.json", snapshotJson("s2", 300L))
      write(dir, "files/snapshots/10/metadata/3.json", snapshotJson("s3", 200L))
      write(dir, "files/snapshots/10/metadata/notes.txt", "ignored")
      val c = catalog(dir)
      assert(c.list("files", 10L).size == 3)
      assert(c.latest("files", 10L).name == "s2")
      assert(c.byName("files", 10L, "s3").name == "s3")
      assert(c.asOf("files", 10L, 250L).name == "s3")
      assert(c.asOf("files", 10L, 100L).name == "s1")
      val none = intercept[IllegalArgumentException](c.asOf("files", 10L, 50L))
      assert(none.getMessage.contains("as of 50"))
      val missing =
        intercept[IllegalArgumentException](c.byName("files", 10L, "nope"))
      assert(missing.getMessage.contains("named 'nope'"))
      val empty = intercept[IllegalArgumentException](c.latest("files", 11L))
      assert(empty.getMessage.contains("snapshots/11/metadata/"))
    }
  }

  test("snapshot selection rejects missing or ambiguous create_ts") {
    withDir { dir =>
      write(dir, "files/snapshots/10/metadata/1.json", snapshotJson("s1", 100L))
      write(dir, "files/snapshots/10/metadata/2.json", snapshotJson("s2", 100L))
      val tied = intercept[IllegalArgumentException](
        catalog(dir).latest("files", 10L)
      )
      assert(tied.getMessage.contains("share create_ts 100"))
      assert(tied.getMessage.contains("1.json"))
      assert(tied.getMessage.contains("2.json"))

      val withoutCreateTs =
        snapshotJson("s3", 300L).replace(", \"create_ts\": 300", "")
      write(dir, "files/snapshots/10/metadata/3.json", withoutCreateTs)
      val c = catalog(dir)
      assert(c.byName("files", 10L, "s3").name == "s3")
      val latest = intercept[IllegalArgumentException](c.latest("files", 10L))
      assert(latest.getMessage.contains("create_ts"))
      assert(latest.getMessage.contains("3.json"))
      val asOf = intercept[IllegalArgumentException](c.asOf("files", 10L, 200L))
      assert(asOf.getMessage.contains("create_ts"))
    }
  }

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

      val json = snapshotJson("duplicate", 200L).replace(
        "\"manifest_list\": []",
        "\"manifest_list\": [\"files/snapshots/10/manifests/2/30.avro\"]"
      )
      write(dir, "files/snapshots/10/metadata/2.json", json)
      val resolver = new V2SegmentResolver {
        def resolve(
            paths: Seq[String],
            bucket: String,
            store: com.zilliz.milvus.storage.io.ObjectStore,
            version: Int
        ) = Right(
          Seq(
            Segment.v2(
              id = 30L,
              partitionId = 20L,
              rows = 1L,
              columnGroups = Seq(
                V2ColumnGroup(
                  Seq(100L),
                  Seq("files/insert_log/10/20/30/100/1"),
                  Seq(1L),
                  slotFieldId = 100L
                )
              ),
              deltaLogs = Seq.empty
            )
          )
        )
      }
      val duplicate = intercept[IllegalArgumentException](
        new SnapshotCatalog(new LocalObjectStore(dir.toString), "", resolver)
          .read("files/snapshots/10/metadata/2.json")
      )
      assert(duplicate.getMessage.contains("duplicate segment id(s): 30"))
    }
  }

  test("metadataPrefix follows DataCoord's layout") {
    assert(
      SnapshotCatalog.metadataPrefix(
        "files",
        10L
      ) == "files/snapshots/10/metadata/"
    )
    assert(
      SnapshotCatalog.metadataPrefix(
        "files/",
        10L
      ) == "files/snapshots/10/metadata/"
    )
    assert(SnapshotCatalog.metadataPrefix("", 10L) == "snapshots/10/metadata/")
  }

  test("a snapshot in another bucket than the catalog's is refused") {
    withDir { dir =>
      write(dir, "files/snapshots/10/metadata/1.json", snapshotJson("s1", 100L))
      val c = new SnapshotCatalog(
        new LocalObjectStore(dir.toString),
        "a",
        V2SegmentResolver.Unavailable
      )
      val err = intercept[IllegalArgumentException](
        c.read("s3a://b/files/snapshots/10/metadata/1.json")
      )
      assert(err.getMessage.contains("bucket 'b'"))
      assert(c.read("s3a://a/files/snapshots/10/metadata/1.json").name == "s1")
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
      v2Segments = Seq.empty,
      bucket = "expected",
      origin = SnapshotOrigin.Options
    )
    assert(result.isLeft)
    assert(result.left.get.getMessage.contains("bucket 'other'"))
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
        v2Segments = Seq.empty,
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
        V2SegmentResolver.Unavailable,
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
          "",
          V2SegmentResolver.Unavailable
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
    val empty = bad(
      SnapshotJson(
        snapshotInfo = SnapshotInfoJson("s"),
        collection =
          CollectionJson(CollectionSchemaJson("c", fields = Seq.empty)),
        manifestList = Seq.empty,
        storageV2ManifestList = Some(Seq.empty)
      )
    )
    assert(empty.contains("no manifests and no V2 segments"))
  }

  test(
    "V2 segments are materialized through the resolver and deduplicated by slot"
  ) {
    withDir { dir =>
      write(
        dir,
        "files/snapshots/10/metadata/1.json",
        snapshotJson("s1", 100L, Seq.empty).replace(
          "\"manifest_list\": []",
          "\"manifest_list\": [\"files/snapshots/10/manifests/1/40.avro\"]"
        )
      )
      val resolver = new V2SegmentResolver {
        def resolve(
            paths: Seq[String],
            bucket: String,
            store: com.zilliz.milvus.storage.io.ObjectStore,
            v: Int
        ) = {
          assert(paths == Seq("files/snapshots/10/manifests/1/40.avro"))
          Right(
            Seq(
              Segment.v2(
                id = 40L,
                partitionId = 20L,
                rows = 2L,
                columnGroups = Seq(
                  V2ColumnGroup(
                    Seq(100L, 0L, 1L),
                    Seq("a.parquet"),
                    Seq(2L),
                    slotFieldId = 3L
                  ),
                  V2ColumnGroup(
                    Seq(100L),
                    Seq("b.parquet"),
                    Seq(2L),
                    slotFieldId = 100L
                  )
                ),
                deltaLogs = Seq(DeltaLogFile(1L, "d.log", 1L))
              ),
              Segment.v2(
                41L,
                20L,
                0L,
                Seq.empty,
                Seq(DeltaLogFile(2L, "l0.log", 3L))
              )
            )
          )
        }
      }
      val snapshot =
        new SnapshotCatalog(new LocalObjectStore(dir.toString), "", resolver)
          .read("files/snapshots/10/metadata/1.json")
      val data = snapshot.dataSegments
      assert(data.map(_.id) == Seq(40L))
      val groups =
        data.head.layout.asInstanceOf[SegmentLayout.ColumnGroups].groups
      assert(groups.find(_.slotFieldId == 3L).get.fieldIds == Seq(0L, 1L))
      assert(data.head.rows.contains(2L))
      assert(
        data.head.deletes == DeleteFiles.Listed(
          Seq(DeltaLogFile(1L, "d.log", 1L))
        )
      )
      assert(snapshot.deleteOnlySegments.map(_.id) == Seq(41L))
    }
  }

  test(
    "Milvus endpoint paths become bucket-relative V3, V2 data and delete keys"
  ) {
    withDir { dir =>
      val json = snapshotJson(
        "s1",
        100L,
        Seq(
          (
            30L,
            "s3://minio:9000/milvus-bucket/files/insert_log/10/20/30"
          )
        )
      ).replace(
        "\"manifest_list\": []",
        "\"manifest_list\": [\"s3://minio:9000/milvus-bucket/files/snapshots/10/manifests/1/40.avro\"]"
      )
      write(dir, "files/snapshots/10/metadata/1.json", json)

      val resolver = new V2SegmentResolver {
        def resolve(
            paths: Seq[String],
            bucket: String,
            store: com.zilliz.milvus.storage.io.ObjectStore,
            v: Int
        ) = {
          Right(
            Seq(
              Segment.v2(
                id = 40L,
                partitionId = 20L,
                rows = 1L,
                columnGroups = Seq(
                  V2ColumnGroup(
                    Seq(100L),
                    Seq(
                      "s3://minio:9000/milvus-bucket/files/insert_log/10/20/40/100/1"
                    ),
                    Seq(1L),
                    slotFieldId = 100L
                  )
                ),
                deltaLogs = Seq(
                  DeltaLogFile(
                    1L,
                    "s3://minio:9000/milvus-bucket/files/delta_log/10/20/40/1",
                    1L
                  )
                )
              )
            )
          )
        }
      }

      val snapshot = new SnapshotCatalog(
        new LocalObjectStore(dir.toString),
        bucket = "milvus-bucket",
        resolver,
        endpoint = "minio:9000"
      ).read("files/snapshots/10/metadata/1.json")

      val v3 = snapshot.v3Segments.head
      assert(
        v3.layout == SegmentLayout.Manifest(
          "files/insert_log/10/20/30",
          7L
        )
      )
      val v2 = snapshot.v2Segments.head
      assert(
        v2.columnGroups.head.filePaths ==
          Seq("files/insert_log/10/20/40/100/1")
      )
      assert(
        v2.deltaLogs.map(_.logPath) == Seq("files/delta_log/10/20/40/1")
      )
    }
  }
}
