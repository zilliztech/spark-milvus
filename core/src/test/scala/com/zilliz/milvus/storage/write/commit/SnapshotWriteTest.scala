package com.zilliz.milvus.storage.write.commit

import java.nio.file.{Files, Path}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.Inside

import com.zilliz.milvus.storage.io.LocalObjectStore
import com.zilliz.milvus.storage.snapshot.{
  DeleteFiles,
  Segment,
  SegmentIndexes,
  SegmentLayout,
  Snapshot,
  SnapshotCatalog,
  SnapshotOrigin,
  V2SegmentResolver
}
import io.milvus.grpc.common.KeyValuePair
import io.milvus.grpc.schema.{
  CollectionSchema => ProtoSchema,
  DataType,
  FieldSchema
}

/** W8: a snapshot the connector writes is the one a snapshot read gets back
  * (docs/design/architecture/vector-search.html section 2.7).
  */
class SnapshotWriteTest extends AnyFunSuite with Matchers with Inside {

  private val schema = ProtoSchema(
    name = "built",
    fields = Seq(
      FieldSchema(
        fieldID = 100L,
        name = "id",
        dataType = DataType.Int64,
        isPrimaryKey = true
      ),
      FieldSchema(
        fieldID = 101L,
        name = "vector",
        dataType = DataType.FloatVector,
        typeParams = Seq(KeyValuePair("dim", "4"))
      )
    )
  )

  private def segment(id: Long, rows: Long) = Segment(
    id = id,
    partitionId = 20L,
    storageVersion = 3,
    rows = Some(rows),
    layout = SegmentLayout.Manifest(s"files/insert_log/10/20/$id", 7L),
    deletes = DeleteFiles.InManifest
  )

  private val snapshot = Snapshot(
    name = "source",
    collectionId = 10L,
    createdAt = Some(100L),
    schema = schema,
    partitionIds = Seq(20L),
    segments = Seq(segment(30L, 8L), segment(31L, 5L)),
    origin = SnapshotOrigin.Options,
    bucket = ""
  )

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

  private def withStore(f: (Path, LocalObjectStore) => Unit): Unit = {
    val directory = Files.createTempDirectory("snapshot-write-")
    val store = new LocalObjectStore(directory.toString)
    try f(directory, store)
    finally store.close()
  }

  test("a written snapshot reads back as the one that was written") {
    withStore { (directory, store) =>
      val written = SnapshotWriter.write(
        snapshot,
        Seq(index(30L, 8L), index(31L, 5L)),
        target,
        store
      )

      written.metadataKey shouldBe "built/snapshots/10/metadata/5.json"
      written.manifestKeys shouldBe Seq(
        "built/snapshots/10/manifests/5/30.avro",
        "built/snapshots/10/manifests/5/31.avro"
      )
      written.bytes should be > 0L

      val catalog = new SnapshotCatalog(
        new LocalObjectStore(directory.toString),
        bucket = "",
        V2SegmentResolver.Unavailable
      )
      val back = catalog.read(written.metadataKey)

      back.name shouldBe "built-5"
      back.collectionId shouldBe 10L
      back.createdAt shouldBe Some(4200L)
      back.partitionIds shouldBe Seq(20L)
      back.buildIds shouldBe Some(Vector(7000L))
      back.segments.map(_.id) should contain theSameElementsAs Seq(30L, 31L)
      back.segments.map(_.rows) should contain theSameElementsAs Seq(
        Some(8L),
        Some(5L)
      )
      back.segments.foreach { segment =>
        segment.storageVersion shouldBe 3
        segment.layout shouldBe SegmentLayout.Manifest(
          s"files/insert_log/10/20/${segment.id}",
          7L
        )
      }

      // The schema survives: field ids, types, the primary key and the
      // dimension a search binds the column by.
      back.schema.fields.map(_.name) shouldBe Seq("id", "vector")
      back.schema.fields.map(_.fieldID) shouldBe Seq(100L, 101L)
      back.primaryKeyField.map(_.name) shouldBe Some("id")
      val vector = back.schema.fields.last
      vector.dataType shouldBe DataType.FloatVector
      vector.typeParams.find(_.key == "dim").map(_.value) shouldBe Some("4")

      val indexed = back.segments.find(_.id == 30L).get.indexes
      inside(indexed) { case SegmentIndexes.Available(entries) =>
        entries should have size 1
        val entry = entries.head
        entry.fieldId shouldBe 101L
        entry.buildId shouldBe 7000L
        entry.rowCount shouldBe 8L
        entry.serializedSize shouldBe 4096L
        entry.indexVersion shouldBe 1L
        entry.currentIndexVersion shouldBe Some(10)
        entry.indexStorePathVersion shouldBe Some(0)
        entry.indexType shouldBe Some("HNSW")
        entry.metricType shouldBe Some("L2")
        entry.parameters.get("M") shouldBe Some("4")
        entry.filePaths shouldBe Vector(
          "built/index_files/7000/1/20/30/HNSW"
        )
      }

      val definition = back.indexes.get.head
      definition.fieldId shouldBe 101L
      definition.indexId shouldBe entriesIndexId(back)
      definition.name shouldBe "vector_hnsw"
      definition.typeParameters.get("dim") shouldBe Some("4")
      definition.indexParameters.get("index_type") shouldBe Some("HNSW")
    }
  }

  test("a segment with no index of its own stays unindexed") {
    withStore { (directory, store) =>
      val written =
        SnapshotWriter.write(snapshot, Seq(index(30L, 8L)), target, store)
      val back = new SnapshotCatalog(
        new LocalObjectStore(directory.toString),
        bucket = "",
        V2SegmentResolver.Unavailable
      ).read(written.metadataKey)

      back.segments.find(_.id == 31L).get.indexes shouldBe
        SegmentIndexes.Unindexed
      back.segments
        .find(_.id == 30L)
        .get
        .indexes shouldBe a[SegmentIndexes.Available]
    }
  }

  test("the collection's own index definition keeps its id and name") {
    withStore { (directory, store) =>
      val declared = com.zilliz.milvus.storage.snapshot.CollectionIndex(
        collectionId = 10L,
        fieldId = 101L,
        indexId = 900L,
        name = "vector_index",
        typeParameters = Map("dim" -> "4"),
        indexParameters = Map("index_type" -> "HNSW", "metric_type" -> "L2"),
        userIndexParameters = Map("index_type" -> "AUTOINDEX")
      )
      val written = SnapshotWriter.write(
        snapshot.copy(indexes = Some(Vector(declared))),
        Seq(index(30L, 8L), index(31L, 5L)),
        target,
        store
      )
      val back = new SnapshotCatalog(
        new LocalObjectStore(directory.toString),
        bucket = "",
        V2SegmentResolver.Unavailable
      ).read(written.metadataKey)

      back.indexes.get.map(_.indexId) shouldBe Vector(900L)
      back.indexes.get.head.name shouldBe "vector_index"
      back.indexes.get.head.userIndexParameters
        .get("index_type") shouldBe Some("AUTOINDEX")
      inside(back.segments.find(_.id == 30L).get.indexes) {
        case SegmentIndexes.Available(entries) =>
          entries.head.indexId shouldBe 900L
          entries.head.name shouldBe "vector_index"
      }
    }
  }

  test("a snapshot is not written from a segment it cannot describe") {
    withStore { (_, store) =>
      the[IllegalArgumentException] thrownBy SnapshotWriter.write(
        snapshot.copy(segments = Seq(segment(30L, 8L).copy(rows = None))),
        Seq.empty,
        target,
        store
      ) should have message
        "requirement failed: Segment 30 does not say how many rows it holds"

      the[IllegalArgumentException] thrownBy SnapshotWriter.write(
        snapshot.copy(segments =
          Seq(
            segment(30L, 8L).copy(
              storageVersion = 2,
              layout = SegmentLayout.ColumnGroups(Seq.empty)
            )
          )
        ),
        Seq.empty,
        target,
        store
      )

      the[IllegalArgumentException] thrownBy SnapshotWriter.write(
        snapshot,
        Seq(index(99L, 8L)),
        target,
        store
      ) should have message
        "requirement failed: Index records name segment(s) the snapshot does not hold: 99"
    }
  }

  private def entriesIndexId(snapshot: Snapshot): Long =
    snapshot.segments
      .flatMap(_.indexes match {
        case SegmentIndexes.Available(entries) => entries
        case _                                 => Vector.empty
      })
      .head
      .indexId
}
