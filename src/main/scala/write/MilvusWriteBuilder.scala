package com.zilliz.spark.connector.write

import org.apache.spark.sql.connector.write.{LogicalWriteInfo, Write, WriteBuilder}

import com.zilliz.spark.connector.MilvusOption

case class MilvusWriteBuilder(
    milvusOptions: MilvusOption,
    info: LogicalWriteInfo
) extends WriteBuilder
    with Serializable {
  override def build: Write = MilvusWrite(milvusOptions, info.schema())
}
