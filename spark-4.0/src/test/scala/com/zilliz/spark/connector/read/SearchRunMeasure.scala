package com.zilliz.spark.connector.read

import scala.collection.mutable

import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}
import org.apache.spark.sql.SparkSession

import com.zilliz.spark.connector.metrics.SearchMetrics

/** What one NEAREST BY run costs, as the scale programs report it
  * (docs/design/architecture/dataframe-api.html sections 7 and 10): the wall
  * time, the bytes every shuffle of the run wrote, and what the take stage did
  * -- the rows it took and the bytes it read, counted in the stages whose tasks
  * took rows, since the first stage reports its reads under the same counter.
  */
private[read] object SearchRunMeasure {

  final case class Measured(
      seconds: Double,
      shuffleBytes: Long,
      takeRows: Long,
      takeReadBytes: Long,
      counters: Map[String, Long]
  ) {
    def line(label: String): String =
      f"RESULT $label seconds=$seconds%.1f shuffle_bytes=$shuffleBytes " +
        s"take_rows=$takeRows take_read_bytes=$takeReadBytes " +
        counters.toSeq.sorted
          .map { case (name, value) => s"$name=$value" }
          .mkString(" ")
  }

  def apply[A](spark: SparkSession)(run: => A): (A, Measured) = {
    val byStage = mutable.Map.empty[Int, mutable.Map[String, Long]]
    var shuffle = 0L
    var tasks = 0L
    val listener = new SparkListener {
      override def onTaskEnd(end: SparkListenerTaskEnd): Unit = synchronized {
        tasks += 1
        Option(end.taskMetrics).foreach(metrics =>
          shuffle += metrics.shuffleWriteMetrics.bytesWritten
        )
        val counters =
          byStage.getOrElseUpdate(end.stageId, mutable.Map.empty[String, Long])
        end.taskInfo.accumulables
          .filter(_.name.exists(_.startsWith("milvus.search.")))
          .foreach { value =>
            val name = value.name.get
            counters(name) = counters.getOrElse(name, 0L) +
              value.update.map(_.toString.toLong).getOrElse(0L)
          }
      }
    }
    spark.sparkContext.addSparkListener(listener)
    val started = System.nanoTime()
    var finished = started
    val result =
      try {
        val value = run
        finished = System.nanoTime()
        value
      } finally {
        // Task ends reach the listener after the job returns; wait until no
        // more arrive for a second, at most half a minute.
        val deadline = System.nanoTime() + 30L * 1000000000L
        var seen = -1L
        while (
          System.nanoTime() < deadline && listener.synchronized(tasks) != seen
        ) {
          seen = listener.synchronized(tasks)
          Thread.sleep(1000L)
        }
        spark.sparkContext.removeSparkListener(listener)
      }
    val seconds = (finished - started) / 1e9
    listener.synchronized {
      val taking = byStage.values.filter(
        _.getOrElse(SearchMetrics.TakeRows, 0L) > 0L
      )
      val totals = byStage.values
        .flatMap(_.toSeq)
        .groupBy(_._1)
        .map { case (name, all) => name -> all.map(_._2).sum }
      (
        result,
        Measured(
          seconds,
          shuffle,
          taking.map(_.getOrElse(SearchMetrics.TakeRows, 0L)).sum,
          taking.map(_.getOrElse(SearchMetrics.ReadBytes, 0L)).sum,
          totals
        )
      )
    }
  }
}
