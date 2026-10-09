package com.zilliz.spark.connector.uat

import java.util.UUID

import com.zilliz.milvus.storage.io.ObjectStore

/** Ownership and cleanup for mutable resources created by live-service tests.
  */
private[connector] object UatResources {
  def uniqueName(prefix: String): String = {
    val runId = sys.env
      .getOrElse("CI_RUN_ID", "local")
      .replaceAll("[^A-Za-z0-9_]", "_")
      .take(48)
    s"${prefix}${runId}_${UUID.randomUUID().toString.replace("-", "")}"
  }

  def withCollection[A](create: String => Unit, drop: String => Unit)(
      body: String => A
  ): A = {
    val name = uniqueName("spark_milvus_ct_")
    withCleanup {
      create(name)
      body(name)
    } {
      drop(name)
    }
  }

  def withCleanup[A](body: => A)(cleanup: => Unit): A = {
    var failure: Throwable = null
    try body
    catch {
      case error: Throwable =>
        failure = error
        throw error
    } finally {
      try cleanup
      catch {
        case error: Throwable =>
          if (failure == null) throw error
          if (error ne failure) failure.addSuppressed(error)
      }
    }
  }
}

/** A fresh child of the configured prefix, including on a retry of one run. */
private[connector] final class UatWriteScope(base: String, caseName: String) {
  val root: String =
    s"${base.stripSuffix("/")}/${UatResources.uniqueName(caseName + "_")}"

  def run[A](store: ObjectStore, keep: Boolean = false)(body: => A): A =
    UatResources.withCleanup(body) {
      UatResources.withCleanup {
        if (!keep && store.exists(root)) {
          val files =
            store.list(root, recursive = true).filterNot(_.isDirectory)
          require(
            files.forall(_.path.startsWith(root + "/")),
            s"storage listed files outside the owned test directory $root"
          )
          // The native API cannot delete directory markers; delete only files.
          files.foreach(file => store.delete(file.path))
        }
      } {
        store.close()
      }
    }
}
