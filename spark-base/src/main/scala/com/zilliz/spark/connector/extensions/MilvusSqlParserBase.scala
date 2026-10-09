package com.zilliz.spark.connector.extensions

import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.{DataType, StructType}

/** The parser the extension installs, minus the one method whose presence
  * depends on the Spark line (`parseRoutineParam`, 4.0 and later): each line's
  * `MilvusSqlParser` adds what its `ParserInterface` has.
  *
  * Only a statement that starts with `CALL milvus.` is ours; it goes to the
  * generated `MilvusCallParser`. Everything else, including Spark 4's own
  * `CALL` for its ProcedureCatalog, is handed to `delegate` untouched.
  */
abstract class MilvusSqlParserBase(delegate: ParserInterface)
    extends ParserInterface {

  override def parsePlan(sqlText: String): LogicalPlan =
    if (MilvusSqlParserBase.isProcedureCall(sqlText)) CallBuilder.parse(sqlText)
    else delegate.parsePlan(sqlText)

  override def parseExpression(sqlText: String): Expression =
    delegate.parseExpression(sqlText)

  override def parseTableIdentifier(sqlText: String): TableIdentifier =
    delegate.parseTableIdentifier(sqlText)

  override def parseFunctionIdentifier(sqlText: String): FunctionIdentifier =
    delegate.parseFunctionIdentifier(sqlText)

  override def parseMultipartIdentifier(sqlText: String): Seq[String] =
    delegate.parseMultipartIdentifier(sqlText)

  override def parseTableSchema(sqlText: String): StructType =
    delegate.parseTableSchema(sqlText)

  override def parseDataType(sqlText: String): DataType =
    delegate.parseDataType(sqlText)

  override def parseQuery(sqlText: String): LogicalPlan =
    delegate.parseQuery(sqlText)
}

object MilvusSqlParserBase {

  /** `CALL milvus.` after comments are dropped and whitespace collapsed, in any
    * case: the cheap test that keeps every other statement out of our grammar.
    */
  def isProcedureCall(sqlText: String): Boolean = {
    val normalized = sqlText
      .replaceAll("--.*?(\\r?\\n|$)", " ")
      .replaceAll("(?s)/\\*.*?\\*/", " ")
      .replaceAll("\\s+", " ")
      .trim
      .toLowerCase(java.util.Locale.ROOT)
    normalized.startsWith("call milvus.")
  }
}
