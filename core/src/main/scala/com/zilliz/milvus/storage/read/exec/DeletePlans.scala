package com.zilliz.milvus.storage.read.exec

import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.milvus.storage.delete.{DeletePlan, DeltaLogReader}
import com.zilliz.milvus.storage.io.{NativeObjectStore, ObjectStore}
import com.zilliz.milvus.storage.read.plan.{DeleteSource, SegmentReadTask}
import io.milvus.grpc.schema.FieldSchema

/** The delete plan a task applies, read on the executor from the files the task
  * names.
  *
  * A file that cannot be read fails the task: an empty plan in its place would
  * return deleted rows with no error, the failure the delete path once had
  * (docs/design/architecture/snapshot.html section 3).
  */
object DeletePlans {

  /** Opens the task's storage, reads its delete files and closes it. */
  def of(task: SegmentReadTask, pkField: Option[FieldSchema]): DeletePlan =
    task.deletes match {
      case DeleteSource.Files(_) =>
        val store = NativeObjectStore.Factory(task.properties).open()
        useAndClose(store.close())(of(task, pkField, store))
      case _ => of(task, pkField, null)
    }

  private[exec] def useAndClose[A](close: => Unit)(use: => A): A = {
    var primaryFailure: Throwable = null
    try use
    catch {
      case failure: Throwable =>
        primaryFailure = failure
        throw failure
    } finally {
      try close
      catch {
        case closeFailure: Throwable =>
          if (primaryFailure == null) throw closeFailure
          if (closeFailure ne primaryFailure)
            primaryFailure.addSuppressed(closeFailure)
      }
    }
  }

  def of(
      task: SegmentReadTask,
      pkField: Option[FieldSchema],
      store: ObjectStore
  ): DeletePlan =
    task.deletes match {
      case DeleteSource.None               => DeletePlan.empty
      case DeleteSource.Materialized(plan) => plan
      case DeleteSource.Files(files) =>
        val pk = pkField.getOrElse(
          throw new IllegalArgumentException(
            s"segment ${task.segmentId} has ${files.size} delete file(s) but the schema has no primary key"
          )
        )
        DeltaLogReader
          .loadDeletePlan(
            files,
            pk,
            task.properties.getOrElse(StorageProperties.BucketName, ""),
            store
          )
          .fold(
            e =>
              throw new IllegalStateException(
                s"cannot read the ${files.size} delete file(s) of segment ${task.segmentId}: ${e.getMessage}",
                e
              ),
            identity
          )
    }
}
