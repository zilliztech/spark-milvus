package com.zilliz.milvus.storage.write.commit

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{Files, Path}

import com.fasterxml.jackson.databind.ObjectMapper
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.Inside

import com.zilliz.milvus.storage.io.LocalObjectStore
import com.zilliz.milvus.storage.manifest.SnapshotSegmentFixture
import com.zilliz.milvus.storage.snapshot.{
  SegmentIndexes,
  SnapshotCatalog,
  V2SegmentResolver
}

/** W8: what a snapshot write produces is the snapshot it copied, carrying the
  * indexes a build job made (docs/design/architecture/vector-search.html
  * section 2.7).
  */
class SnapshotWriteTest extends AnyFunSuite with Matchers with Inside {

  private val mapper = new ObjectMapper()

  private val sourceKey = "files/snapshots/10/metadata/1.json"

  /** A snapshot as Milvus writes one: the collection block carries the channels
    * and partitions a restore needs, and each segment is an Avro manifest.
    */
  private def source(directory: Path, rows: Long = 8L): Unit = {
    Seq(30L, 31L).foreach { segmentId =>
      write(
        directory,
        s"files/snapshots/10/manifests/1/$segmentId.avro",
        SnapshotSegmentFixture.encode(
          version = 4,
          segmentId = segmentId,
          partitionId = 20L,
          rows = rows,
          storageVersion = 3L
        )
      )
    }
    val document =
      s"""{
        "snapshot_info": {
          "name": "source", "id": "1", "collection_id": "10",
          "partition_ids": ["20"], "create_ts": "100",
          "state": "SnapshotStatePending"
        },
        "collection": {
          "schema": {"name": "c", "fields": [
            {"fieldID": 100, "name": "id", "data_type": "Int64", "is_primary_key": true},
            {"fieldID": 101, "name": "v", "data_type": "FloatVector",
             "type_params": [{"key": "dim", "value": "4"}]}
          ]},
          "num_shards": "1", "num_partitions": "1",
          "partitions": {"_default": "20"},
          "virtual_channel_names": ["by-dev-rootcoord-dml_0_10v0"],
          "properties": [{"key": "timezone", "value": "UTC"}]
        },
        "format_version": 4,
        "segment_ids": ["30", "31"],
        "manifest_list": [
          "files/snapshots/10/manifests/1/30.avro",
          "files/snapshots/10/manifests/1/31.avro"
        ],
        "storagev2_manifest_list": [
          {"segmentID": 30, "manifest": "{\\"ver\\":7,\\"base_path\\":\\"files/insert_log/10/20/30\\"}"},
          {"segmentID": 31, "manifest": "{\\"ver\\":7,\\"base_path\\":\\"files/insert_log/10/20/31\\"}"}
        ]
      }"""
    write(directory, sourceKey, document.getBytes(UTF_8))
  }

  private def write(directory: Path, key: String, bytes: Array[Byte]): Unit = {
    val path = directory.resolve(key)
    Files.createDirectories(path.getParent)
    Files.write(path, bytes)
  }

  private def index(segmentId: Long, rows: Long) = CommittedIndex(
    segmentId = segmentId,
    partitionId = 20L,
    fieldId = 101L,
    buildId = 7000L,
    indexVersion = 1L,
    vectorIndexVersion = 10,
    storePathVersion = 0,
    indexType = "HNSW",
    metricType = "L2",
    rowCount = rows,
    serializedSize = 4096L,
    filePaths = Seq(s"built/index_files/7000/1/20/$segmentId/HNSW"),
    params = Map("M" -> "4")
  )

  private val target = SnapshotTarget(
    rootPath = "built",
    collectionId = 10L,
    snapshotId = 5L,
    name = "built-5",
    createTs = 4200L
  )

  private def catalog(directory: Path) = new SnapshotCatalog(
    new LocalObjectStore(directory.toString),
    bucket = "",
    V2SegmentResolver.Unavailable
  )

  private def withStore(f: (Path, LocalObjectStore) => Unit): Unit = {
    val directory = Files.createTempDirectory("snapshot-write-")
    val store = new LocalObjectStore(directory.toString)
    try {
      source(directory)
      f(directory, store)
    } finally store.close()
  }

  test("a written snapshot keeps everything the source said") {
    withStore { (directory, store) =>
      val snapshot = catalog(directory).read(sourceKey)
      val written = SnapshotWriter.write(
        snapshot,
        Seq(index(30L, 8L), index(31L, 8L)),
        target,
        store,
        sourceKey,
        restorable = false
      )

      written.metadataKey shouldBe "built/snapshots/10/metadata/5.json"
      written.manifestKeys shouldBe Seq(
        "built/snapshots/10/manifests/5/30.avro",
        "built/snapshots/10/manifests/5/31.avro"
      )

      val document = mapper.readTree(
        Files.readAllBytes(directory.resolve(written.metadataKey))
      )
      // The parts a restore needs and this connector does not model: they are
      // the source's own values, not something the writer made up.
      val collection = document.get("collection")
      collection.get("virtual_channel_names").get(0).asText() shouldBe
        "by-dev-rootcoord-dml_0_10v0"
      collection.get("partitions").get("_default").asText() shouldBe "20"
      collection.get("num_shards").asText() shouldBe "1"
      collection
        .get("properties")
        .get(0)
        .get("key")
        .asText() shouldBe "timezone"
      document.get("snapshot_info").get("state").asText() shouldBe
        "SnapshotStatePending"

      // The parts the build job owns.
      document.get("snapshot_info").get("name").asText() shouldBe "built-5"
      document.get("snapshot_info").get("id").asText() shouldBe "5"
      document.get("snapshot_info").get("create_ts").asText() shouldBe "4200"
      document.get("build_ids").get(0).asText() shouldBe "7000"
      document.get("segment_ids").get(0).asText() shouldBe "30"
      document.get("manifest_list").get(0).asText() shouldBe
        "built/snapshots/10/manifests/5/30.avro"
      // The spellings Milvus's own snapshots use; its parser discards the
      // others, which is an index on field 0 in the restored collection.
      val declared = document.get("indexes").get(0)
      declared.get("fieldID").asText() shouldBe "101"
      declared.get("collectionID").asText() shouldBe "10"
      declared.get("indexID").asText() shouldBe "7000"
      declared.get("index_name").asText() shouldBe "v_hnsw"
      declared.get("index_params").toString should include(""""index_type"""")

      // Milvus reads a storage manifest mapping's segment id from `segmentID`,
      // and the mapping is the source's.
      document
        .get("storagev2_manifest_list")
        .get(0)
        .get("segmentID")
        .asLong() shouldBe 30L
    }
  }

  test("the written snapshot reads back with the indexes it was given") {
    withStore { (directory, store) =>
      val snapshot = catalog(directory).read(sourceKey)
      val written = SnapshotWriter.write(
        snapshot,
        Seq(index(30L, 8L)),
        target,
        store,
        sourceKey,
        restorable = false
      )
      val back = catalog(directory).read(written.metadataKey)

      back.name shouldBe "built-5"
      back.createdAt shouldBe Some(4200L)
      back.segments.map(_.id) should contain theSameElementsAs Seq(30L, 31L)
      back.segments.map(_.rows).toSet shouldBe Set(Some(8L))
      back.buildIds shouldBe Some(Vector(7000L))

      inside(back.segments.find(_.id == 30L).get.indexes) {
        case SegmentIndexes.Available(entries) =>
          entries should have size 1
          entries.head.buildId shouldBe 7000L
          entries.head.rowCount shouldBe 8L
          entries.head.indexType shouldBe Some("HNSW")
          entries.head.metricType shouldBe Some("L2")
          entries.head.parameters.get("M") shouldBe Some("4")
          entries.head.filePaths shouldBe Vector(
            "built/index_files/7000/1/20/30/HNSW"
          )
      }
      // A segment the job did not index keeps no index records of its own.
      back.segments.find(_.id == 31L).get.indexes shouldBe
        SegmentIndexes.Unindexed
    }
  }

  test("a write needs the document it copies and the segments it names") {
    withStore { (directory, store) =>
      val snapshot = catalog(directory).read(sourceKey)

      the[IllegalArgumentException] thrownBy SnapshotWriter.write(
        snapshot,
        Seq(index(99L, 8L)),
        target,
        store,
        sourceKey,
        restorable = false
      ) should have message
        "requirement failed: Index records name segment(s) the snapshot does not hold: 99"

      the[IllegalArgumentException] thrownBy SnapshotWriter.write(
        snapshot,
        Seq.empty,
        target,
        store,
        "",
        restorable = false
      ) should have message
        "requirement failed: A snapshot write copies a snapshot document, which this call has to name"

      SnapshotWriter.sourceKeyOf(snapshot) shouldBe Some(sourceKey)
    }
  }

  test("a restorable snapshot is refused when its data sits outside the root") {
    withStore { (directory, store) =>
      val snapshot = catalog(directory).read(sourceKey)

      val error = the[IllegalArgumentException] thrownBy SnapshotWriter.write(
        snapshot,
        Seq(index(30L, 8L), index(31L, 8L)),
        target,
        store,
        sourceKey,
        restorable = true
      )
      error.getMessage should include("outside the root 'built'")
      error.getMessage should include("files/insert_log/10/20/30")
      error.getMessage should include("files/insert_log/10/20/31")
      error.getMessage should include("restorable => false")
      store.exists(SnapshotWriter.metadataKeyOf(target)) shouldBe false
    }
  }

  test(
    "a restorable snapshot under the data's own root passes the restore check"
  ) {
    withStore { (directory, store) =>
      val snapshot = catalog(directory).read(sourceKey)
      val underFiles =
        target.copy(rootPath = "files", snapshotId = 6L, name = "files-6")
      val indexes = Seq(30L, 31L).map { id =>
        index(id, 8L).copy(
          filePaths = Seq(s"files/index_files/7000/1/20/$id/HNSW_0")
        )
      }

      val written = SnapshotWriter.write(
        snapshot,
        indexes,
        underFiles,
        store,
        sourceKey,
        restorable = true
      )
      written.metadataKey shouldBe "files/snapshots/10/metadata/6.json"
      SnapshotBundle.rootOf(written.metadataKey) shouldBe Some("files")
      SnapshotBundle.outsideRootOf(
        store,
        written.metadataKey,
        ""
      ) shouldBe empty

      // The connector-only snapshot under `built` is exactly what a restore
      // would refuse: its data files are Milvus's, under `files`.
      val connectorOnly = SnapshotWriter.write(
        snapshot,
        Seq(index(30L, 8L), index(31L, 8L)),
        target,
        store,
        sourceKey,
        restorable = false
      )
      SnapshotBundle.outsideRootOf(
        store,
        connectorOnly.metadataKey,
        ""
      ) should contain allOf ("files/insert_log/10/20/30", "files/insert_log/10/20/31")
    }
  }
}
