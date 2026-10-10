package com.zilliz.spark.connector.implicits

import java.util.{Locale, UUID}
import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{Column, DataFrame, Dataset}

/** The connector's methods on one DataFrame
  * (docs/design/architecture/dataframe-api.html sections 6 and 9).
  *
  * `buildIndex` and `writeSnapshot` run `CALL milvus.system.build_index` and
  * `write_snapshot` over the Milvus table this DataFrame reads, and return the
  * procedure's rows. The DataFrame is registered as a temporary view under a
  * name of its own, the `CALL` names that view as its `table`, and the view is
  * dropped once the call has run: a `CALL` is a command, so its rows exist
  * before `sql` returns, in a classic session and in a Spark Connect one. The
  * procedure takes the table from the view's analyzed plan, so it works on the
  * snapshot this DataFrame pinned, and refuses a DataFrame that is not the
  * whole table. Only the API both sessions share is used here.
  */
final class MilvusDataFrame(frame: DataFrame) {

  /** Spark 4.2's `Dataset.nearestByJoin`, INNER: for each row of this frame,
    * the `numResults` rows of `right` the ranking puts first.
    */
  def nearestByJoin(
      right: Dataset[_],
      rankingExpression: Column,
      numResults: Int,
      mode: String,
      direction: String
  ): DataFrame =
    nearestByJoin(
      right,
      rankingExpression,
      numResults,
      mode,
      direction,
      "inner"
    )

  /** Spark 4.2's `Dataset.nearestByJoin`, INNER or LEFT OUTER. */
  def nearestByJoin(
      right: Dataset[_],
      rankingExpression: Column,
      numResults: Int,
      mode: String,
      direction: String,
      joinType: String
  ): DataFrame = NearestByEntry.nearestByJoin(
    frame,
    right,
    rankingExpression,
    numResults,
    mode,
    direction,
    joinType
  )

  /** `CALL milvus.system.build_index`: one index of `field` per segment of the
    * table, written under `output`. An argument left out is not passed, so the
    * procedure's default applies; `params` are the index's `name=value` tuning
    * parameters.
    */
  def buildIndex(
      field: String,
      output: String,
      indexType: String = null,
      metric: String = null,
      params: Map[String, String] = Map.empty,
      buildId: java.lang.Long = null,
      indexVersion: java.lang.Long = null,
      storePathVersion: java.lang.Long = null
  ): DataFrame = call(
    "build_index",
    Seq(
      "field" -> field,
      "output" -> output,
      "index_type" -> indexType,
      "metric" -> metric,
      "params" -> namedValues(params),
      "build_id" -> buildId,
      "index_version" -> indexVersion,
      "store_path_version" -> storePathVersion
    )
  )

  /** `buildIndex` for Java: the other arguments under their `CALL` names, such
    * as `index_type` or `build_id`. A string, an integer or a boolean is passed
    * as that constant, and a map as its `name=value` list; the procedure checks
    * each against the type it declares.
    */
  def buildIndex(
      field: String,
      output: String,
      arguments: java.util.Map[String, _]
  ): DataFrame = call(
    "build_index",
    Seq("field" -> field, "output" -> output) ++ fromJava(
      arguments,
      "field",
      "output"
    )
  )

  /** `CALL milvus.system.write_snapshot`: the snapshot that delivers build job
    * `job`, whose objects are under `input`, written under `output` (by default
    * `input`). An argument left out is not passed, so the procedure's default
    * applies.
    */
  def writeSnapshot(
      job: String,
      input: String,
      output: String = null,
      snapshotId: java.lang.Long = null,
      snapshotName: String = null,
      restorable: java.lang.Boolean = null
  ): DataFrame = call(
    "write_snapshot",
    Seq(
      "job" -> job,
      "input" -> input,
      "output" -> output,
      "snapshot_id" -> snapshotId,
      "snapshot_name" -> snapshotName,
      "restorable" -> restorable
    )
  )

  /** `writeSnapshot` for Java: the other arguments under their `CALL` names,
    * passed as `buildIndex` passes them.
    */
  def writeSnapshot(
      job: String,
      input: String,
      arguments: java.util.Map[String, _]
  ): DataFrame = call(
    "write_snapshot",
    Seq("job" -> job, "input" -> input) ++ fromJava(arguments, "job", "input")
  )

  /** Runs `CALL milvus.system.<procedure>(table => <view>, ...)` with the
    * arguments that are not null.
    */
  private def call(
      procedure: String,
      arguments: Seq[(String, Any)]
  ): DataFrame = {
    val spark = frame.sparkSession
    val view = "milvus_input_" + UUID.randomUUID().toString.replace("-", "")
    val given = ("table" -> view) +: arguments.filter(_._2 != null)
    val statement = given
      .map { case (name, value) =>
        s"$name => ${MilvusDataFrame.constant(value)}"
      }
      .mkString(s"CALL milvus.system.$procedure(", ", ", ")")
    frame.createOrReplaceTempView(view)
    try spark.sql(statement)
    finally spark.catalog.dropTempView(view)
  }

  /** The arguments a Java caller names, apart from the two `positional` ones
    * and the table, which the method passes itself.
    */
  private def fromJava(
      arguments: java.util.Map[String, _],
      positional: String*
  ): Seq[(String, Any)] =
    Option(arguments)
      .map(_.asScala.toSeq)
      .getOrElse(Seq.empty)
      .sortBy(_._1)
      .map { case (name, value) =>
        val parameter = String.valueOf(name).toLowerCase(Locale.ROOT)
        require(
          !(Seq("table", "collection") ++ positional).contains(parameter),
          s"'$name' is not an argument of the map: the method passes the table, and " +
            s"takes ${positional.mkString(" and ")} by position"
        )
        parameter -> (value match {
          case values: java.util.Map[_, _] =>
            namedValues(
              values.asScala.map { case (key, item) =>
                String.valueOf(key) -> String.valueOf(item)
              }.toMap
            )
          case other => other
        })
      }

  /** `name=value` pairs as the procedure's `params` takes them, or null when
    * there are none.
    */
  private def namedValues(values: Map[String, String]): String =
    if (values.isEmpty) null
    else
      values.toSeq.sorted
        .map { case (key, value) => s"$key=$value" }
        .mkString(",")
}

object MilvusDataFrame {

  /** A value as a constant of the `CALL` grammar: a string quoted, with `\` and
    * `'` escaped by a backslash, which the grammar undoes.
    */
  private[implicits] def constant(value: Any): String = value match {
    case text: String =>
      "'" + text.replace("\\", "\\\\").replace("'", "\\'") + "'"
    case number @ (_: java.lang.Long | _: java.lang.Integer |
        _: java.lang.Short | _: java.lang.Byte) =>
      number.toString
    case flag: java.lang.Boolean => flag.toString
    case other =>
      throw new IllegalArgumentException(
        s"A CALL argument is a string, an integer or a boolean, not ${other.getClass.getName}"
      )
  }
}
