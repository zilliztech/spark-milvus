package com.zilliz.spark.connector.extensions

import org.apache.spark.sql.catalyst.parser.ParserInterface

/** The 3.5 line's `ParserInterface` has nothing beyond the base. */
class MilvusSqlParser(delegate: ParserInterface)
    extends MilvusSqlParserBase(delegate)
