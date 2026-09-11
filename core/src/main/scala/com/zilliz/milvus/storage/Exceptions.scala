package com.zilliz.milvus.storage

/** 解析存储格式里的 schema 或数据时的失败：缺字段参数、维度非法、类型不认识。 */
case class DataParseException(message: String) extends Exception(message)

/** 类型映射不支持的组合。 */
case class DataTypeException(message: String) extends Exception(message)
