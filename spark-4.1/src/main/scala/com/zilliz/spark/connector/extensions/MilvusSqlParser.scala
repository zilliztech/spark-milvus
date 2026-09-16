package com.zilliz.spark.connector.extensions

import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.types.StructType

/** Spark 4.0 added `parseRoutineParam` to `ParserInterface`; forwarded. */
class MilvusSqlParser(delegate: ParserInterface)
    extends MilvusSqlParserBase(delegate) {
  override def parseRoutineParam(sqlText: String): StructType =
    delegate.parseRoutineParam(sqlText)
}
