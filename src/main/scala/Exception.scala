package com.zilliz.spark.connector

// DataParseException 与 DataTypeException 已搬到 core 的
// com.zilliz.milvus.storage，用到的地方直接 import。

case class MilvusConnectionException(message: String) extends Exception(message)

case class MilvusRpcException(message: String) extends Exception(message)

case class MilvusRateLimitException(message: String) extends Exception(message)
