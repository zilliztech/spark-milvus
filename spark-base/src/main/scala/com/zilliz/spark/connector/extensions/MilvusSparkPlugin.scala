package com.zilliz.spark.connector.extensions

import java.util

import org.apache.spark.api.plugin.{
  DriverPlugin,
  ExecutorPlugin,
  PluginContext,
  SparkPlugin
}
import org.apache.spark.internal.Logging

import com.zilliz.milvus.jni.storage.NativeStorageLibrary
import com.zilliz.milvus.jni.vector.NativeVectorLibrary

/** `spark.plugins=com.zilliz.spark.connector.extensions.MilvusSparkPlugin`:
  * every executor loads the connector's native libraries as it starts, off the
  * path of its first task.
  *
  * Without it the libraries load the first time a task touches Knowhere or
  * milvus-storage: the bundle is extracted from the assembly and checked (207
  * shared libraries, 850 MiB, 3.0 s on the P3 executors) while the task that
  * needs it waits. An executor starts long before its first search task -- the
  * driver is still planning and delivering the query set -- so the same work
  * done here costs the job nothing. A load that fails here is logged and left
  * to the first task, which reports the failure the way it always did
  * (docs/design/architecture/vector-search.html section 2.1, 2026-09-24
  * decision).
  */
class MilvusSparkPlugin extends SparkPlugin {
  override def driverPlugin(): DriverPlugin = null
  override def executorPlugin(): ExecutorPlugin = new MilvusExecutorPlugin
}

/** The executor side: load the native bundle once, at start. */
class MilvusExecutorPlugin extends ExecutorPlugin with Logging {
  override def init(
      context: PluginContext,
      extraConf: util.Map[String, String]
  ): Unit = MilvusExecutorPlugin.preload()
}

object MilvusExecutorPlugin extends Logging {

  /** Loads the storage and vector libraries, or logs why it could not. */
  def preload(): Boolean = {
    val started = System.nanoTime()
    try {
      NativeStorageLibrary.load()
      val runtime = NativeVectorLibrary.load()
      logInfo(
        s"Native libraries loaded at executor start in ${(System
            .nanoTime() - started) / 1000000L} ms: $runtime"
      )
      true
    } catch {
      case error: Throwable =>
        logWarning(
          "Native libraries did not load at executor start; the first task that needs them will load them and report any failure",
          error
        )
        false
    }
  }
}
