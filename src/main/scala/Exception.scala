package com.zilliz.spark.connector

case class DataParseException(message: String) extends Exception(message)

case class DataTypeException(message: String) extends Exception(message)

case class MilvusConnectionException(message: String) extends Exception(message)

case class MilvusRpcException(message: String) extends Exception(message)
