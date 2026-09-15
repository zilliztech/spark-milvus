package com.zilliz.milvus.storage.read.exec

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import com.zilliz.milvus.storage.delete.DeletePlan
import com.zilliz.milvus.storage.io.{FailingObjectStore, LocalObjectStore}
import com.zilliz.milvus.storage.read.plan.{DeleteSource, SegmentReadTask}
import com.zilliz.milvus.storage.snapshot.{DeltaLogFile, SegmentLayout}
import io.milvus.grpc.schema.{DataType, FieldSchema}

/** The executor's half of the delete path: files named by the task become the
  * plan the reader applies, or the task fails.
  */
class DeletePlansTest extends AnyFunSuite with Matchers {

  private val pk = FieldSchema(
    fieldID = 100,
    name = "id",
    dataType = DataType.Int64,
    isPrimaryKey = true
  )

  private def task(deletes: DeleteSource) = SegmentReadTask(
    segmentId = 30L,
    partitionId = 20L,
    layout = SegmentLayout.Manifest("files/insert_log/10/20/30", 1L),
    schemaBytes = Array.emptyByteArray,
    properties = Map("fs.storage_type" -> "local", "fs.bucket_name" -> ""),
    deletes = deletes
  )

  // The delete file Milvus wrote on UAT (see DeltaLogReaderTest): ids 0..9
  // deleted at 469093213902995459.
  private val uatDelete = DeltaLogFile(
    469093182140298464L,
    "v3-delta-469093182140298464.parquet",
    10L
  )

  test("the files a task names are read into its plan") {
    val plan = DeletePlans.of(
      task(DeleteSource.Files(Seq(uatDelete))),
      Some(pk),
      new LocalObjectStore("core/src/test/data")
    )
    plan.containsLongPk(3L, 469093213902995458L) shouldBe true
    plan.containsLongPk(10L, 469093213902995458L) shouldBe false
  }

  test("no source and a plan in hand pass straight through") {
    DeletePlans.of(
      task(DeleteSource.None),
      Some(pk),
      null
    ) shouldBe DeletePlan.empty
    val plan = DeletePlan.fromLongPks(Map(1L -> 2L))
    DeletePlans.of(
      task(DeleteSource.Materialized(plan)),
      None,
      null
    ) shouldBe plan
  }

  test("a file that cannot be read fails the task") {
    val e = intercept[IllegalStateException](
      DeletePlans.of(
        task(DeleteSource.Files(Seq(uatDelete))),
        Some(pk),
        new FailingObjectStore(new java.io.IOException("gone"))
      )
    )
    e.getMessage should include(
      "cannot read the 1 delete file(s) of segment 30"
    )
  }

  test("files without a primary key in the schema are an error") {
    an[IllegalArgumentException] should be thrownBy
      DeletePlans.of(
        task(DeleteSource.Files(Seq(uatDelete))),
        None,
        new LocalObjectStore("core/src/test/data")
      )
  }
}
