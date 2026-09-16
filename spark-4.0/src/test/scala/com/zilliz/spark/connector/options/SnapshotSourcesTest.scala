package com.zilliz.spark.connector.options

import org.scalatest.funsuite.AnyFunSuite

import com.zilliz.milvus.storage.io.{FileInfo, ObjectStore}
import com.zilliz.milvus.storage.snapshot.{
  Segment,
  Snapshot,
  SnapshotOrigin,
  SnapshotSource,
  V2ColumnGroup
}
import io.milvus.grpc.schema.CollectionSchema

class SnapshotSourcesTest extends AnyFunSuite {

  private final class TrackingStore(
      closeFailure: Option[Throwable] = None
  ) extends ObjectStore {
    var closed = false

    override def close(): Unit = {
      closed = true
      closeFailure.foreach(throw _)
    }

    override def readAll(key: String): Array[Byte] = Array.emptyByteArray
    override def size(key: String): Long = 0L
    override def list(key: String, recursive: Boolean): Seq[FileInfo] =
      Seq.empty
    override def exists(key: String): Boolean = false
    override def readAt(
        key: String,
        offset: Long,
        length: Long,
        fileSize: Long
    ): Array[Byte] = Array.emptyByteArray
    override def write(key: String, data: Array[Byte]): Unit = ()
    override def createDir(key: String, recursive: Boolean): Unit = ()
    override def delete(key: String): Unit = ()
  }

  private val snapshot = Snapshot(
    name = "fixed",
    collectionId = 1L,
    createdAt = Some(2L),
    schema = CollectionSchema(name = "collection"),
    partitionIds = Seq.empty,
    segments = Seq.empty,
    origin = SnapshotOrigin.Options,
    bucket = ""
  )

  test("managed source closes its store after a successful snapshot") {
    val store = new TrackingStore
    val source = SnapshotSources.managedSource(store)(_ => snapshot)

    assert(!store.closed)
    assert(source.snapshot() == Right(snapshot))
    assert(store.closed)
  }

  test("managed source closes its store and preserves the primary failure") {
    val closeError = new IllegalStateException("close failed")
    val store = new TrackingStore(Some(closeError))
    val readError = new IllegalArgumentException("snapshot failed")
    val source = SnapshotSources.managedSource(store)(_ => throw readError)

    val result = source.snapshot()
    assert(result == Left(readError))
    assert(store.closed)
    assert(readError.getSuppressed.toSeq == Seq(closeError))
  }

  test("managed source reports a close failure after a successful read") {
    val closeError = new IllegalStateException("close failed")
    val store = new TrackingStore(Some(closeError))
    val source = SnapshotSources.managedSource(store)(_ => snapshot)

    assert(source.snapshot() == Left(closeError))
    assert(store.closed)
  }

  test("the common source wrapper applies partition and segment selectors") {
    val first = Segment.v2(
      id = 10L,
      partitionId = 1L,
      rows = 1L,
      columnGroups = Seq(V2ColumnGroup(Seq(100L), Seq("first"), Seq(1L)))
    )
    val second = Segment.v2(
      id = 20L,
      partitionId = 2L,
      rows = 1L,
      columnGroups = Seq(V2ColumnGroup(Seq(100L), Seq("second"), Seq(1L)))
    )
    val resolved = snapshot.copy(
      partitionIds = Seq(1L, 2L),
      segments = Seq(first, second)
    )

    val selected = SnapshotSources
      .narrowed(SnapshotSource(resolved), Seq(2L), Seq(20L))
      .snapshot()
      .toOption
      .get

    assert(selected.partitionIds == Seq(2L))
    assert(selected.dataSegments.map(_.id) == Seq(20L))
  }

  test("legacy source-specific selectors fail before a source is opened") {
    val option = MilvusOption(
      Map(
        MilvusOption.SnapshotMode -> "true",
        MilvusOption.SnapshotSchemaBytes -> java.util.Base64.getEncoder
          .encodeToString(snapshot.schema.toByteArray),
        MilvusOption.MilvusPartitionID -> "1"
      )
    )

    val error = intercept[IllegalArgumentException](
      SnapshotSources.forRead(option, withSegments = true)
    )
    assert(error.getMessage.contains(MilvusOption.MilvusPartitionID))
    assert(error.getMessage.contains(MilvusOption.MilvusPartitions))
  }

  test("catalog snapshot selection rejects offline read modes") {
    val option = MilvusOption(
      Map(
        MilvusOption.SnapshotMode -> "true",
        MilvusOption.SnapshotSchemaBytes -> java.util.Base64.getEncoder
          .encodeToString(snapshot.schema.toByteArray)
      )
    )

    val error = intercept[IllegalArgumentException](
      SnapshotSources.forRead(
        option,
        withSegments = true,
        SnapshotReference.Latest
      )
    )
    assert(error.getMessage.contains("requires client mode"))
    assert(error.getMessage.contains("format(\"milvus\")"))
  }
}
