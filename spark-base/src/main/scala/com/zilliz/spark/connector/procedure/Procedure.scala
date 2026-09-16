package com.zilliz.spark.connector.procedure

import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.Row

/** One parameter of a procedure: its name in `name => value`, the type the
  * constant has to be, and whether a call may leave it out.
  */
final case class Parameter(
    name: String,
    dataType: DataType,
    required: Boolean = true
)

/** What a call handed over: the procedure's own parameters by name, already
  * checked against its parameter list, and the connection and storage options
  * (the backquoted `option.key => value` arguments), which are the same keys a
  * DataFrame read takes.
  */
final case class ProcedureArgs(
    values: Map[String, Any],
    options: Map[String, String]
) {
  def string(name: String): String = values(name).asInstanceOf[String]
  def stringOpt(name: String): Option[String] =
    values.get(name).collect { case s: String => s }
}

/** A procedure a `CALL milvus.system.<name>(...)` statement runs on the driver.
  * The body knows nothing of SQL or Spark plans; it takes its arguments and
  * returns the rows of its result table. Design:
  * docs/design/architecture/procedure.html section 3.
  */
trait Procedure {
  def name: String
  def parameters: Seq[Parameter]
  def outputSchema: StructType
  def run(args: ProcedureArgs): Seq[Row]
}

/** The procedures the connector has, by name. */
object Procedures {
  val Namespace: Seq[String] = Seq("milvus", "system")

  val all: Seq[Procedure] = Seq(RegisterProcedure)

  def byName(name: String): Option[Procedure] =
    all.find(_.name.equalsIgnoreCase(name))
}
