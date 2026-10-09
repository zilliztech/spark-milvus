package com.zilliz.milvus.storage

/** A schema or data value in the storage format could not be parsed: a missing
  * field parameter, an out-of-range dimension, an unrecognized type code.
  */
case class DataParseException(message: String) extends Exception(message)

/** A type combination the mapping does not support. */
case class DataTypeException(message: String) extends Exception(message)
