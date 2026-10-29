package com.zilliz.milvus.storage.manifest

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Tests for raw Avro snapshot segment records. */
class SnapshotSegmentReaderTest extends AnyFunSuite with Matchers {

  test(
    "version 4 decodes the segment identity, row count and storage version"
  ) {
    val bytes = SnapshotSegmentFixture.encode(
      segmentId = 99L,
      partitionId = 42L,
      rows = 8L,
      storageVersion = 3L,
      segmentLevel = 1L,
      channelName = "source-channel"
    )
    val entry = SnapshotSegmentReader.parse(bytes, 4).fold(throw _, identity)
    entry.segmentId shouldBe 99L
    entry.partitionId shouldBe 42L
    entry.numOfRows shouldBe 8L
    entry.storageVersion shouldBe 3L
    entry.segmentLevel shouldBe 1L
    entry.channelName shouldBe "source-channel"
  }

  test("unsupported raw manifest versions fail") {
    val bytes = SnapshotSegmentFixture.encode()
    Seq(-1, 0, 1, 2, 3, 5, 7, Int.MaxValue).foreach { version =>
      val error = SnapshotSegmentReader.parse(bytes, version).swap.toOption.get
      error.getMessage should include(
        "Unsupported Milvus manifest schema version"
      )
    }
  }

  test(
    "all manifest versions decode exact index ids, parameters and distinct versions"
  ) {
    val index = SnapshotSegmentFixture.index()
    SnapshotSegmentSchema.supportedVersions.foreach { version =>
      val bytes = SnapshotSegmentFixture.encode(
        version = version,
        indexes = Vector(index)
      )
      val entry =
        SnapshotSegmentReader.parse(bytes, version).fold(throw _, identity)
      entry.indexFiles shouldBe Some(Vector(index))
      entry.numOfRows shouldBe 2L
    }
  }

  test("all manifest versions decode the neutral statslog entries") {
    val logs = Seq(
      FieldBinlogEntry(
        100L,
        Seq(
          BinlogEntry(9L, "files/stats_log/100/9", 20L),
          BinlogEntry(7L, "files/stats_log/100/7", 10L)
        )
      )
    )
    SnapshotSegmentSchema.supportedVersions.foreach { version =>
      val entry = SnapshotSegmentReader
        .parse(
          SnapshotSegmentFixture.encode(version = version, statsLogs = logs),
          version
        )
        .toOption
        .get
      entry.statsLogFiles shouldBe logs

    }
  }

  test(
    "an explicitly empty index array is known, and truncated index bytes fail"
  ) {
    val bytes = SnapshotSegmentFixture.encode()
    SnapshotSegmentReader.parse(bytes, 4).toOption.get.indexFiles shouldBe Some(
      Vector.empty
    )
    val indexed = SnapshotSegmentFixture.encode(indexes =
      Vector(SnapshotSegmentFixture.index())
    )
    SnapshotSegmentReader
      .parse(indexed.take(indexed.length / 2), 4)
      .isLeft shouldBe true
  }

}
