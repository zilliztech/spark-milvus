package com.zilliz.milvus.client

/** 连不上 Milvus。 */
case class MilvusConnectionException(message: String) extends Exception(message)

/** RPC 返回了非成功状态。 */
case class MilvusRpcException(message: String) extends Exception(message)

/** Milvus 侧限流，调用方应退避重试。 */
case class MilvusRateLimitException(message: String) extends Exception(message)
