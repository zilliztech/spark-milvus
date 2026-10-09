package com.zilliz.spark.connector.options

import org.apache.spark.resource.{
  ResourceProfile,
  ResourceProfileBuilder,
  TaskResourceRequests
}
import org.apache.spark.sql.SparkSession

/** The executors a job runs on and the cores a stage's tasks take.
  *
  * A stage whose tasks call Knowhere -- an index search, an index build --
  * needs one task per executor: Knowhere spreads one call over a thread pool
  * the size of the machine, so a second task on the same executor adds no speed
  * and a second index's memory. Rather than asking the submission to set
  * `spark.task.cpus` for the whole job, which holds every other stage to one
  * task per executor too, such a stage declares its own cores per task through
  * Spark's stage-level scheduling (`TaskResourceProfile`,
  * docs/design/architecture/vector-search.html section 1.1).
  *
  * Spark runs a task-only profile on the default executors only when dynamic
  * allocation is off (`ResourceProfileManager.canBeScheduled`, 3.5 and 4.x),
  * and refuses it on a plain `local` master. There nothing is declared and the
  * submission's `spark.task.cpus` stands.
  *
  * @param executors
  *   the executors the job was given: `spark.executor.instances` when set,
  *   otherwise those registered, one in local mode
  * @param executorCores
  *   the cores of one executor: `spark.executor.cores`, otherwise the
  *   registered cores divided among the executors, the n of `local[n]` in local
  *   mode
  * @param taskCpus
  *   `spark.task.cpus`, the cores every task takes by default
  * @param declarable
  *   whether a stage may declare its own cores per task here
  */
final case class TaskResources(
    executors: Int,
    executorCores: Int,
    taskCpus: Int,
    declarable: Boolean
) {
  require(
    executors > 0 && executorCores > 0 && taskCpus > 0,
    s"Executors, cores and task cpus are positive: $this"
  )

  /** Tasks of one executor that run at once under the default task cores. */
  def tasksPerExecutor: Int = math.max(1, executorCores / taskCpus)

  /** A profile whose tasks take `cores` cores each, or None when a stage may
    * not declare one here or the default already takes that many.
    */
  def taking(cores: Int): Option[ResourceProfile] = {
    val bounded = math.max(1, math.min(cores, executorCores))
    if (!declarable || bounded == taskCpus) None
    else
      Some(
        new ResourceProfileBuilder()
          .require(new TaskResourceRequests().cpus(bounded))
          .build()
      )
  }

  /** A task that takes every core of its executor. */
  def wholeExecutor: Option[ResourceProfile] = taking(executorCores)

  /** Tasks that each take enough cores for at most `perExecutor` of them to run
    * at once on one executor.
    */
  def atMost(perExecutor: Int): Option[ResourceProfile] = {
    val running = math.max(1, perExecutor)
    taking((executorCores + running - 1) / running)
  }

  /** Where the numbers came from, for the driver log. */
  def describe: String =
    s"$executors executors x $executorCores cores, spark.task.cpus=$taskCpus, " +
      (if (declarable) "stages declare their own task cores"
       else "stages take spark.task.cpus (local master or dynamic allocation)")
}

object TaskResources {

  def of(spark: SparkSession): TaskResources = {
    val context = spark.sparkContext
    val conf = context.getConf
    val master = context.master
    val local = master == "local" || master.startsWith("local[")
    val dynamic = conf.getBoolean("spark.dynamicAllocation.enabled", false)
    val taskCpus = math.max(1, conf.getInt("spark.task.cpus", 1))
    // The status tracker lists the driver as well as the executors.
    val registered =
      math.max(0, context.statusTracker.getExecutorInfos.length - 1)
    val executors =
      if (local) 1
      else
        conf
          .getOption("spark.executor.instances")
          .flatMap(value => scala.util.Try(value.trim.toInt).toOption)
          .filter(_ > 0)
          .getOrElse(math.max(1, registered))
    val executorCores =
      if (local) math.max(1, context.defaultParallelism)
      else
        conf
          .getOption("spark.executor.cores")
          .flatMap(value => scala.util.Try(value.trim.toInt).toOption)
          .filter(_ > 0)
          .getOrElse(math.max(1, context.defaultParallelism / executors))
    TaskResources(executors, executorCores, taskCpus, !local && !dynamic)
  }
}
