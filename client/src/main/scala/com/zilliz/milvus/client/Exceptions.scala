package com.zilliz.milvus.client

/** Milvus could not be reached. */
case class MilvusConnectionException(message: String) extends Exception(message)

/** An RPC came back with a non-success status. */
case class MilvusRpcException(message: String) extends Exception(message)

/** Milvus is rate limiting; the caller should back off and retry. */
case class MilvusRateLimitException(message: String) extends Exception(message)
