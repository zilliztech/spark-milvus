package com.zilliz.spark.connector.table

import org.scalatest.funsuite.AnyFunSuite

import com.zilliz.milvus.storage.io.{FileInfo, ObjectStore}
import com.zilliz.milvus.storage.snapshot.{
  DeleteFiles,
  Segment,
  SegmentLayout,
  Snapshot,
  SnapshotOrigin,
  SnapshotSource
}
import com.zilliz.spark.connector.options.MilvusOption
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
    val first = Segment(
      id = 10L,
      partitionId = 1L,
      rows = Some(1L),
      storageVersion = 3,
      deletes = DeleteFiles.Empty,
      layout = SegmentLayout.Manifest("first", 1L)
    )
    val second = Segment(
      id = 20L,
      partitionId = 2L,
      rows = Some(1L),
      storageVersion = 3,
      deletes = DeleteFiles.Empty,
      layout = SegmentLayout.Manifest("second", 1L)
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

  test("option-string snapshot ids are strict and case insensitive") {
    val base = Map(
      MilvusOption.SnapshotMode -> "true",
      MilvusOption.SnapshotSchemaBytes -> java.util.Base64.getEncoder
        .encodeToString(snapshot.schema.toByteArray)
    )
    val valid = new OptionStringsSnapshotSource(
      MilvusOption(
        base ++ Map(
          MilvusOption.SnapshotCollectionId.toUpperCase -> " 10 ",
          MilvusOption.SnapshotPartitionIds.toUpperCase -> "0, 20"
        )
      )
    ).snapshot().toOption.get
    assert(valid.collectionId == 10L)
    assert(valid.partitionIds == Seq(0L, 20L))

    Seq(
      MilvusOption.SnapshotCollectionId -> "",
      MilvusOption.SnapshotCollectionId -> "0",
      MilvusOption.SnapshotCollectionId -> "9223372036854775808",
      MilvusOption.SnapshotPartitionIds -> "",
      MilvusOption.SnapshotPartitionIds -> "1,,2",
      MilvusOption.SnapshotPartitionIds -> "-1"
    ).foreach { case (key, value) =>
      val error = new OptionStringsSnapshotSource(
        MilvusOption(base + (key -> value))
      ).snapshot().left.toOption.get
      assert(error.getMessage.contains(key))
      assert(error.getMessage.contains(s"'$value'"))
    }
  }
}
