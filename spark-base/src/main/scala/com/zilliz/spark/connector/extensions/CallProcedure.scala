package com.zilliz.spark.connector.extensions

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.{CatalystTypeConverters, InternalRow}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{LeafCommand, LogicalPlan}
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan, SparkStrategy}

import com.zilliz.spark.connector.procedure.{Procedure, ProcedureArgs}

/** A resolved `CALL`: the procedure and its checked arguments. A command, so
  * Spark runs it eagerly and `spark.sql(...)` returns its result table.
  */
final case class CallProcedure(procedure: Procedure, args: ProcedureArgs)
    extends LeafCommand {
  override lazy val output: Seq[Attribute] =
    DataTypeUtils.toAttributes(procedure.outputSchema)

  override def simpleString(maxFields: Int): String =
    s"CallProcedure milvus.system.${procedure.name}"
}

/** Runs the procedure on the driver, once, and hands its rows back. */
final case class CallProcedureExec(
    output: Seq[Attribute],
    procedure: Procedure,
    args: ProcedureArgs
) extends LeafExecNode {

  private lazy val result: Array[InternalRow] = {
    val toCatalyst =
      CatalystTypeConverters.createToCatalystConverter(procedure.outputSchema)
    procedure.run(args).map(toCatalyst(_).asInstanceOf[InternalRow]).toArray
  }

  override def executeCollect(): Array[InternalRow] = result

  override protected def doExecute(): RDD[InternalRow] =
    sparkContext.parallelize(result.toSeq, 1)

  override def simpleString(maxFields: Int): String =
    s"CallProcedureExec milvus.system.${procedure.name}"
}

/** The only strategy the extension adds: a `CallProcedure` becomes a
  * `CallProcedureExec`; every other plan is left to Spark.
  */
object CallProcedureStrategy extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case call: CallProcedure =>
      CallProcedureExec(call.output, call.procedure, call.args) :: Nil
    case _ => Nil
  }
}
