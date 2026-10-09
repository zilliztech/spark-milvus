package com.zilliz.milvus.storage.read.exec

import java.nio.charset.StandardCharsets.UTF_8

import org.apache.arrow.vector.{
  BigIntVector,
  ValueVector,
  VarBinaryVector,
  VarCharVector
}

import com.zilliz.milvus.storage.credential.StorageProperties
import com.zilliz.milvus.storage.delete.{DeletePlan, DeltaLogReader}
import com.zilliz.milvus.storage.io.{NativeObjectStore, ObjectStore}
import com.zilliz.milvus.storage.read.plan.{DeleteSource, SegmentReadTask}
import io.milvus.grpc.schema.{DataType, FieldSchema}

/** The delete plan a task applies, read on the executor from the files the task
  * names.
  *
  * A file that cannot be read fails the task: an empty plan in its place would
  * return deleted rows with no error, the failure the delete path once had
  * (docs/design/architecture/snapshot.html section 3).
  */
object DeletePlans {

  /** Evaluates the primary-key/timestamp rule shared by scans and index masks.
    */
  def rowDeleted(
      plan: DeletePlan,
      pkField: FieldSchema,
      pk: ValueVector,
      timestamp: ValueVector,
      row: Int
  ): Boolean = {
    require(
      pk != null && timestamp != null,
      "Delete filtering requires primary key and timestamp columns"
    )
    if (pk.isNull(row) || timestamp.isNull(row)) return false
    val rowTs = timestamp.asInstanceOf[BigIntVector].get(row)
    pkField.dataType match {
      case DataType.Int64 =>
        plan.containsLongPk(pk.asInstanceOf[BigIntVector].get(row), rowTs)
      case DataType.VarChar =>
        val bytes = pk match {
          case value: VarCharVector   => value.get(row)
          case value: VarBinaryVector => value.get(row)
          case other =>
            throw new IllegalStateException(
              s"Delete filtering expected a VarChar/VarBinary primary key, got ${other.getClass.getSimpleName}"
            )
        }
        plan.containsStringPk(new String(bytes, UTF_8), rowTs)
      case other =>
        throw new IllegalArgumentException(
          s"Delete filtering only supports Int64/VarChar primary keys, got $other"
        )
    }
  }

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
