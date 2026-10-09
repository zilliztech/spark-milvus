package com.zilliz.milvus.storage.delete

import java.nio.file.{Files, Paths}

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.codec.BinlogFixture
import com.zilliz.milvus.storage.io.LocalObjectStore
import com.zilliz.milvus.storage.snapshot.{DeltaLogFile, Segment, V2ColumnGroup}
import io.milvus.grpc.schema.{DataType, FieldSchema}

class DeltaLogReaderTest extends AnyFunSuite with Matchers {
  test(
    "a V2 delete event reuses the common binlog envelope and Parquet decoder"
  ) {
    val payload = Files.readAllBytes(
      Paths.get("core/src/test/data/v3-delta-469093182140298464.parquet")
    )
    val path = Files.createTempFile("delete-binlog-codec", ".binlog")
    val store = new LocalObjectStore()
    try {
      Files.write(
        path,
        BinlogFixture.encode(
          payload,
          dataType = 5,
          extras = """{"version":"MULTI_FIELD"}""",
          eventType = 2
        )
      )
      val plan = DeltaLogReader
        .loadDeletePlan(
          Seq(DeltaLogFile(1L, path.toString, 10L)),
          FieldSchema(
            fieldID = 100L,
            name = "id",
            dataType = DataType.Int64,
            isPrimaryKey = true
          ),
          "",
          store
        )
        .fold(throw _, identity)
      val deleteTs = 469093213902995459L
      (0L until 10L).foreach { id =>
        plan.containsLongPk(id, deleteTs - 1) shouldBe true
        plan.containsLongPk(id, deleteTs + 1) shouldBe false
      }
      plan.containsLongPk(10L, deleteTs - 1) shouldBe false
      Files.write(
        path,
        BinlogFixture.encode(
          payload,
          dataType = 5,
          extras = """{"version":"MULTI_FIELD","edek":"encrypted"}""",
          eventType = 2
        )
      )
      val rejected = DeltaLogReader.loadDeletePlan(
        Seq(DeltaLogFile(1L, path.toString, 10L)),
        FieldSchema(
          fieldID = 100L,
          name = "id",
          dataType = DataType.Int64,
          isPrimaryKey = true
        ),
        "",
        store
      )
      rejected.left.toOption.get shouldBe a[UnsupportedOperationException]
    } finally {
      store.close()
      Files.deleteIfExists(path)
    }
  }

  test("delete plan union keeps the latest delete timestamp per PK") {
    val merged = DeletePlan.union(
      DeletePlan.fromLongPks(Map(7L -> 100L)),
      DeletePlan.fromLongPks(Map(7L -> 200L, 8L -> 150L))
    )

    merged.containsLongPk(7L, 150L) shouldBe true
    merged.containsLongPk(7L, 250L) shouldBe false
    merged.containsLongPk(8L, 140L) shouldBe true
  }

  test("a V3 segment's bare-parquet delete file is read by its header") {
    // The file Milvus wrote under {segment}/_delta/ on the UAT instance once
    // it folded an L0 delete of ids 0..9 into a V3 segment: a plain parquet
    // file with a pk and a ts column and no event header.
    val plan = DeltaLogReader
      .loadDeletePlan(
        Seq(
          DeltaLogFile(
            469093182140298464L,
            "v3-delta-469093182140298464.parquet",
            10L
          )
        ),
        FieldSchema(
          fieldID = 100,
          name = "id",
          dataType = DataType.Int64,
          isPrimaryKey = true
        ),
        "",
        new LocalObjectStore("core/src/test/data")
      )
      .fold(e => throw e, identity)
    val deleteTs = 469093213902995459L
    (0L until 10L).foreach { id =>
      plan.containsLongPk(
        id,
        deleteTs + 1
      ) shouldBe false // deleted after the row
      plan.containsLongPk(id, deleteTs - 1) shouldBe true
    }
    plan.containsLongPk(10L, deleteTs - 1) shouldBe false
  }
}
