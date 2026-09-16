package com.zilliz.milvus.client

/** Milvus could not be reached. */
case class MilvusConnectionException(message: String) extends Exception(message)

/** An RPC failed during transport or response validation. */
case class MilvusRpcException(message: String) extends Exception(message)

/** Milvus is rate limiting; the caller should back off and retry. */
case class MilvusRateLimitException(message: String) extends Exception(message)

/** A database or collection lookup established that the requested collection
  * does not exist. Connection and authorization failures use their own error
  * paths and must not be classified as absence.
  */
final class CollectionNotFoundException(message: String)
    extends Exception(message)

/** A database lookup established that the requested database does not exist. */
final class DatabaseNotFoundException(message: String)
    extends Exception(message)
